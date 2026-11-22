"""可追溯聚合切片测试。

覆盖：
* 新旧字段日志在调整前后的聚合分组与交付文件对照（旧字段回退仍可复用）；
* 切片版本依据稳定、已发布切片不可变；
* 交叠刷新（构建中切片）与历史下载并发交接时只消费完整可核实切片；
* 交付结果回写采集血缘，精确对应一次聚合刷新的维度与字段映射；
* 完成定义：内部计数自洽且来源摘要可核实，断裂切片拒绝交付。
"""
from __future__ import annotations

import copy
import json

from app.store import store
from app.services import aggregation_slice


def _prepare_pipeline(pipeline, source: str, logs, parser_name: str | None = None):
    """登记数据源 → 采集 → 解析 → 校验，返回批次号。"""
    pipeline.register_source(source, "http", "default")
    pipeline.validate_source(source)
    batch = pipeline.start_ingest_batch(source, "2026Q1", logs)
    batch_no = batch["batch"]["批次编号"]
    pipeline.parse_batch(batch_no, parser_name)
    pipeline.validate_entities(batch_no)
    return batch_no


def _add_legacy_parser(rule_name: str, fields: str) -> None:
    store.rows("field_parser").append({
        "id": 1000 + len(store.rows("field_parser")),
        "规则名称": rule_name,
        "适用数据源": "全部",
        "输入格式": "pipe",
        "输出字段列表": fields,
        "解析引擎": "legacy",
        "status": "已发布",
        "pending": False,
        "abnormal": False,
    })


NEW_FIELD_LOGS = [
    {"body": "2026-01-15T10:30:00|INFO|server started"},
    {"body": "2026-01-15T10:31:00|WARN|memory high"},
    {"body": "2026-01-15T10:32:00|ERROR|connection timeout"},
    {"body": "2026-01-15T10:33:00|INFO|request processed"},
]

# 同一批数据，仅字段名换成历史旧名（ts/lvl/msg）。
LEGACY_FIELD_LOGS = [
    {"body": "2026-01-15T10:30:00|INFO|server started"},
    {"body": "2026-01-15T10:31:00|WARN|memory high"},
    {"body": "2026-01-15T10:32:00|ERROR|connection timeout"},
    {"body": "2026-01-15T10:33:00|INFO|request processed"},
]


class TestTraceableSliceBasics:
    """切片基本结构与完成定义。"""

    def test_refresh_publishes_ready_slice(self, pipeline):
        _prepare_pipeline(pipeline, "slice-src", NEW_FIELD_LOGS)
        result = pipeline.refresh_aggregation("v", "level", "time", "count")
        slice_id = result["slice_id"]
        slice_entry = aggregation_slice.get_slice(slice_id)

        assert slice_entry is not None
        assert slice_entry["status"] == aggregation_slice.STATUS_READY
        assert slice_entry["slice_version"] == 1
        assert slice_entry["slice_digest"]
        assert slice_entry["completion"]["complete"] is True
        # 完成定义：消费数 = 来源引用数 = 校验通过明细数。
        assert slice_entry["completion"]["consumed"] == 4
        assert len(slice_entry["source_refs"]) == 4

    def test_slice_only_consumes_verified_boundary(self, pipeline):
        _prepare_pipeline(pipeline, "boundary-src", NEW_FIELD_LOGS[:3])
        # 额外塞一条非「校验通过」的明细，必须被取数边界排除。
        store.rows("parsed_entity").append({
            "id": 9999,
            "来源批次": "X",
            "标准化字段集": json.dumps({"level": "INFO", "time": "t"}),
            "status": "待校验",
            "content_digest": "deadbeef",
        })
        result = pipeline.refresh_aggregation("bv", "level", "time")
        slice_entry = aggregation_slice.get_slice(result["slice_id"])
        assert all(
            store.find("parsed_entity", ref["entity_id"])["status"] == "校验通过"
            for ref in slice_entry["source_refs"]
        )
        assert slice_entry["record_count"] == 3

    def test_internal_count_mismatch_marks_incomplete(self, pipeline):
        _prepare_pipeline(pipeline, "broken-src", NEW_FIELD_LOGS[:2])
        building = aggregation_slice.begin_slice("bv", "level", "time", "count")
        # 人为制造内部不自洽：分组计数与来源引用数对不上。
        building["results"] = {"INFO": {"count": 9, "sum": 0.0, "values": []}}
        building["source_refs"] = [{"entity_id": 1, "content_digest": "x"}]
        building["completion"]["consumed"] = 1
        aggregation_slice.complete_slice(building)
        assert building["status"] == aggregation_slice.STATUS_BROKEN
        assert aggregation_slice.get_latest_ready_slice("bv") is None


class TestNewAndLegacyFields:
    """新旧字段对照：分组口径与交付文件保持一致，旧字段读取可复用。"""

    def test_legacy_fields_fall_back_to_same_groups(self, pipeline):
        _add_legacy_parser("legacy-rule", "ts,lvl,msg")
        _prepare_pipeline(pipeline, "legacy-src", LEGACY_FIELD_LOGS, "legacy-rule")
        result = pipeline.refresh_aggregation("legacy-view", "level", "time", "count")
        slice_entry = aggregation_slice.get_slice(result["slice_id"])

        # 逻辑字段名 level/time 通过旧名 lvl/ts 命中，映射被如实记录。
        assert slice_entry["field_mapping"]["dim_field"]["legacy"] is True
        assert slice_entry["field_mapping"]["dim_field"]["physical"] == "lvl"
        assert slice_entry["field_mapping"]["metric_field"]["physical"] == "ts"
        # 分组结果与新字段日志完全一致（INFO 两条、WARN/ERROR 各一条）。
        groups = slice_entry["results"]
        assert set(groups) == {"INFO", "WARN", "ERROR"}
        assert groups["INFO"]["count"] == 2
        assert groups["WARN"]["count"] == 1
        assert groups["ERROR"]["count"] == 1

    def test_new_and_legacy_logs_produce_identical_files(self, pipeline):
        # 新字段链路
        _prepare_pipeline(pipeline, "new-src", NEW_FIELD_LOGS)
        new_agg = pipeline.refresh_aggregation("new-view", "level", "time", "count")
        new_export = pipeline.build_export("new-view", "csv")
        new_results = copy.deepcopy(new_export["export"]["results"])
        new_path = new_export["export"]["文件路径"]

        # 隔离第二次聚合：聚合按原口径扫描全局校验通过明细，先清空再建旧字段链路。
        store.rows("parsed_entity").clear()
        _add_legacy_parser("legacy-rule-2", "ts,lvl,msg")
        _prepare_pipeline(pipeline, "old-src", LEGACY_FIELD_LOGS, "legacy-rule-2")
        old_agg = pipeline.refresh_aggregation("old-view", "level", "time", "count")
        old_export = pipeline.build_export("old-view", "csv")

        # 调整后：旧字段日志的文件内容与新字段日志逐分组一致。
        assert old_export["export"]["results"] == new_results
        # 下载格式与路径口径保持不变。
        assert old_export["export"]["文件路径"] == "/exports/old-view.csv"
        assert new_path == "/exports/new-view.csv"
        assert old_export["export"]["record_count"] == 3
        # 调整前口径不回归：新字段日志分组仍是 INFO=2。
        assert new_agg["view"]["results"]["INFO"]["count"] == 2
        # 旧字段链路同样产出 INFO=2，且记录了旧字段回退。
        assert old_agg["view"]["results"]["INFO"]["count"] == 2

    def test_missing_both_names_falls_to_unknown_bucket(self, pipeline):
        _prepare_pipeline(pipeline, "miss-src", NEW_FIELD_LOGS[:1])
        # 造一条 level/lvl 都不存在的明细。
        store.rows("parsed_entity").append({
            "id": 9998,
            "来源批次": "M",
            "标准化字段集": json.dumps({"time": "t", "msg": "no-level"}),
            "status": "校验通过",
            "content_digest": "abcd1234",
        })
        result = pipeline.refresh_aggregation("miss-view", "level", "time")
        groups = result["view"]["results"]
        assert "unknown" in groups  # 与历史 fields.get(dim, "unknown") 口径一致


class TestSliceVersioning:
    """已发布切片的稳定版本依据。"""

    def test_repeated_refresh_versions_and_digest_stability(self, pipeline):
        _prepare_pipeline(pipeline, "ver-src", NEW_FIELD_LOGS[:2])
        first = pipeline.refresh_aggregation("ver-view", "level", "time", "count")
        second = pipeline.refresh_aggregation("ver-view", "level", "time", "count")

        s1 = aggregation_slice.get_slice(first["slice_id"])
        s2 = aggregation_slice.get_slice(second["slice_id"])
        assert s1["slice_version"] == 1
        assert s2["slice_version"] == 2
        # 相同输入 → 相同内容摘要，版本依据稳定。
        assert s1["slice_digest"] == s2["slice_digest"]

    def test_published_slice_is_immutable_snapshot(self, pipeline):
        _prepare_pipeline(pipeline, "imm-src", NEW_FIELD_LOGS[:2])
        agg = pipeline.refresh_aggregation("imm-view", "level", "time")
        slice_entry = aggregation_slice.get_slice(agg["slice_id"])
        snapshot = copy.deepcopy(slice_entry["results"])
        snapshot_refs = copy.deepcopy(slice_entry["source_refs"])

        # 底层明细随后被改写，已发布切片内容不受影响。
        for entity in store.rows("parsed_entity"):
            if entity.get("status") == "校验通过":
                entity["content_digest"] = "tampered"
        assert slice_entry["results"] == snapshot
        assert slice_entry["source_refs"] == snapshot_refs
        # 但来源核实会重新识别出断裂。
        assert aggregation_slice.verify_slice(slice_entry) is False

    def test_different_input_changes_digest(self, pipeline):
        _prepare_pipeline(pipeline, "d1-src", NEW_FIELD_LOGS[:2])
        first = pipeline.refresh_aggregation("d-view", "level", "time")
        _prepare_pipeline(pipeline, "d2-src", NEW_FIELD_LOGS[2:])
        second = pipeline.refresh_aggregation("d-view", "level", "time")
        assert (
            aggregation_slice.get_slice(first["slice_id"])["slice_digest"]
            != aggregation_slice.get_slice(second["slice_id"])["slice_digest"]
        )


class TestOverlappingRefreshAndHistory:
    """交叠刷新与历史请求的并发交接。"""

    def test_building_slice_is_not_delivered(self, pipeline):
        _prepare_pipeline(pipeline, "ov-src", NEW_FIELD_LOGS[:2])
        first = pipeline.refresh_aggregation("ov-view", "level", "time")
        # 第二次刷新进行中：先落一张「构建中」切片，尚未核实完成。
        building = aggregation_slice.begin_slice("ov-view", "level", "time", "count")
        assert aggregation_slice.has_building_slice("ov-view") is True

        # 交付端只能看到上一版完整切片，构建中的切片不参与消费。
        latest = aggregation_slice.get_latest_ready_slice("ov-view")
        assert latest["slice_id"] == first["slice_id"]
        export = pipeline.build_export("ov-view", "csv")
        assert export["ok"] is True
        assert export["export"]["slice_id"] == first["slice_id"]
        assert building["status"] == aggregation_slice.STATUS_BUILDING

    def test_completed_overlap_refresh_promotes_new_version(self, pipeline):
        _prepare_pipeline(pipeline, "ov2-src", NEW_FIELD_LOGS[:2])
        first = pipeline.refresh_aggregation("ov2-view", "level", "time")
        second = pipeline.refresh_aggregation("ov2-view", "level", "time")

        latest = aggregation_slice.get_latest_ready_slice("ov2-view")
        assert latest["slice_id"] == second["slice_id"]
        # 上一版仍可按 slice_id 历史下载。
        history = pipeline.build_export(
            "ov2-view", "csv", slice_id=first["slice_id"]
        )
        assert history["ok"] is True
        assert history["export"]["slice_id"] == first["slice_id"]

    def test_history_request_keeps_old_field_mapping(self, pipeline):
        # 第一版：旧字段名的历史统计。
        _add_legacy_parser("legacy-rule-3", "ts,lvl,msg")
        _prepare_pipeline(pipeline, "h-src", LEGACY_FIELD_LOGS, "legacy-rule-3")
        v1 = pipeline.refresh_aggregation("h-view", "level", "time", "count")
        # 第二版：同一视图按新口径再刷新（映射元数据仍记录历史回退表）。
        v2 = pipeline.refresh_aggregation("h-view", "level", "time", "count")

        history = pipeline.build_export("h-view", "csv", slice_id=v1["slice_id"])
        current = pipeline.build_export("h-view", "csv")
        assert history["export"]["slice_version"] == 1
        assert current["export"]["slice_id"] == v2["slice_id"]
        # 历史文件保留 v1 的维度、行数与字段映射。
        assert history["export"]["results"]["INFO"]["count"] == 2
        assert history["export"]["field_mapping"]["dim_field"]["physical"] == "lvl"
        assert history["lineage"]["slice_id"] == v1["slice_id"]

    def test_unknown_or_broken_slice_rejected(self, pipeline):
        _prepare_pipeline(pipeline, "rej-src", NEW_FIELD_LOGS[:1])
        pipeline.refresh_aggregation("rej-view", "level", "time")

        assert pipeline.build_export("ghost-view", "csv")["ok"] is False
        assert pipeline.build_export(
            "rej-view", "csv", slice_id="SLICE-9999"
        )["ok"] is False

        broken = aggregation_slice.begin_slice("rej-view", "level", "time", "count")
        broken["status"] = aggregation_slice.STATUS_BROKEN
        broken["completion"]["broken_sources"] = ["1"]
        # 断裂切片不会顶替已发布版本，交付仍走最新完整切片。
        latest = aggregation_slice.get_latest_ready_slice("rej-view")
        assert latest is not None
        assert latest["status"] == aggregation_slice.STATUS_READY
        assert pipeline.build_export(
            "rej-view", "csv", slice_id=broken["slice_id"]
        )["ok"] is False


class TestExportLineageWriteback:
    """交付结果回写采集血缘，可对应一次聚合刷新的维度与字段映射。"""

    def test_export_and_lineage_reference_same_slice(self, pipeline):
        _prepare_pipeline(pipeline, "lin2-src", NEW_FIELD_LOGS[:3])
        agg = pipeline.refresh_aggregation("lin2-view", "level", "time", "count")
        export_result = pipeline.build_export("lin2-view", "csv")

        export_entry = export_result["export"]
        lineage = export_result["lineage"]
        target_slice = aggregation_slice.get_slice(agg["slice_id"])

        # 导出与血缘指向同一切片版本与摘要。
        assert export_entry["slice_id"] == lineage["slice_id"] == agg["slice_id"]
        assert export_entry["slice_digest"] == target_slice["slice_digest"]
        assert lineage["slice_digest"] == target_slice["slice_digest"]
        assert lineage["slice_version"] == target_slice["slice_version"]
        # 血缘记录本次刷新采用的维度、指标与来源定义。
        assert lineage["聚合维度"] == "level"
        assert lineage["指标定义"] == "count(time)"
        assert lineage["source_scope"] == aggregation_slice.SOURCE_SCOPE
        assert lineage["field_mapping"] == target_slice["field_mapping"]

    def test_lineage_source_refs_are_verifiable(self, pipeline):
        _prepare_pipeline(pipeline, "ref-src", NEW_FIELD_LOGS)
        agg = pipeline.refresh_aggregation("ref-view", "level", "time", "count")
        export_result = pipeline.build_export("ref-view", "csv")
        refs = export_result["lineage"]["source_refs"]

        verified = store.rows("parsed_entity")
        digest_by_id = {e["id"]: e.get("content_digest") for e in verified}
        assert len(refs) == 4
        for ref in refs:
            # 每条血缘来源都能按实体 ID + 内容摘要回查核实。
            assert digest_by_id[ref["entity_id"]] == ref["content_digest"]
            assert ref["dim_via"] == "level"
            assert ref["metric_via"] == "time"

    def test_export_file_matches_slice_dimensions_and_rows(self, pipeline):
        _prepare_pipeline(pipeline, "dim-src", NEW_FIELD_LOGS)
        agg = pipeline.refresh_aggregation("dim-view", "level", "time", "count")
        export_result = pipeline.build_export("dim-view", "csv")
        slice_entry = aggregation_slice.get_slice(agg["slice_id"])

        # 维度数、各维度行数与切片完全一致，文件与来源不会错配。
        assert len(export_result["export"]["results"]) == len(slice_entry["results"])
        for dim, bucket in slice_entry["results"].items():
            assert export_result["export"]["results"][dim]["count"] == bucket["count"]
        assert export_result["export"]["record_count"] == len(slice_entry["results"])
