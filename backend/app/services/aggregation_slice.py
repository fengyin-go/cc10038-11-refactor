"""可追溯聚合切片。

收拢聚合刷新（aggregation_view）与数据交付（data_export）之间重复编写的
取数边界，让两个阶段共用同一套：

* 来源定义：只消费 ``parsed_entity`` 中「校验通过」且标准化字段集可解析的明细；
* 字段映射：新字段名优先，缺失时回退到已登记的旧字段名（历史口径仍可读）；
* 完成定义：切片内部计数自洽，且每条来源记录都能按 ID + 内容摘要回查核实；
* 版本依据：切片一经置为「完整可核实」即固化不变，带视图内递增版本号与
  内容摘要，历史下载永远对应刷新当时的维度与字段映射。

刷新过程中切片先以「构建中」落表，核实通过后才翻转为「完整可核实」，
因此交叠刷新 / 并发交接时，交付端只会看到内部完整、来源可核实的切片。
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from typing import Any

from app.store import store

# 统一来源定义：聚合与交付都只认这一份标准化明细口径。
SOURCE_SCOPE = "标准化明细"
SLICE_TABLE = "aggregation_slice"

# 旧字段名 → 当前字段名。新字段缺失时按此表回退，保证旧款字段读取仍可复用。
LEGACY_FIELD_ALIASES: dict[str, str] = {
    "lvl": "level",
    "ts": "time",
    "msg": "message",
}

# 切片生命周期：构建中（对外不可消费）→ 完整可核实 / 来源断裂。
STATUS_BUILDING = "构建中"
STATUS_READY = "完整可核实"
STATUS_BROKEN = "来源断裂"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def stable_digest(data: Any) -> str:
    """对任意可 JSON 化的数据计算稳定摘要，作为来源与切片的版本依据。"""
    serialized = json.dumps(data, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_row_id() -> int:
    return max((int(r.get("id", 0)) for r in store.rows(SLICE_TABLE)), default=0) + 1


def _next_slice_version(view_name: str) -> int:
    return max(
        (int(s.get("slice_version", 0)) for s in store.rows(SLICE_TABLE)
         if s.get("view_name") == view_name),
        default=0,
    ) + 1


def iter_verified_entities() -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """聚合与交付共用的取数边界。

    只产出 ``status == 校验通过`` 且「标准化字段集」可 JSON 解析的明细，
    返回 ``(实体行, 字段字典)``。两个阶段不再各自编写筛选逻辑，
    从根本上避免文件与来源错配。
    """
    boundary: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for entity in store.rows("parsed_entity"):
        if entity.get("status") != "校验通过":
            continue
        try:
            fields_data = json.loads(entity.get("标准化字段集", "{}"))
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(fields_data, dict):
            continue
        boundary.append((entity, fields_data))
    return boundary


def resolve_field(
    fields_data: dict[str, Any], logical_name: str
) -> tuple[Any, str, bool]:
    """按统一映射读取字段。

    优先读取当前字段名；缺失时回退到 :data:`LEGACY_FIELD_ALIASES` 中
    登记的旧字段名。返回 ``(取值, 实际命中的物理字段名, 是否走了旧字段)``；
    均未命中时取值为 ``None``，由调用方按原口径补默认值。
    """
    if logical_name in fields_data:
        return fields_data[logical_name], logical_name, False
    for legacy_name, current_name in LEGACY_FIELD_ALIASES.items():
        if current_name == logical_name and legacy_name in fields_data:
            return fields_data[legacy_name], legacy_name, True
    return None, logical_name, False


def _empty_completion(total_scanned: int) -> dict[str, Any]:
    """统一完成定义：消费数 = 核实数，且无断裂来源才算完整。"""
    return {
        "source_scope": SOURCE_SCOPE,
        "total_scanned": total_scanned,
        "consumed": 0,
        "verified": 0,
        "broken_sources": [],
        "complete": False,
    }


def begin_slice(
    view_name: str,
    dim_field: str,
    metric_field: str,
    agg_func: str,
) -> dict[str, Any]:
    """登记一张「构建中」切片并落表。

    刷新未完成期间它对外不可消费，用于承接交叠刷新时的并发交接。
    """
    row_id = _next_row_id()
    slice_entry: dict[str, Any] = {
        "id": row_id,
        "slice_id": f"SLICE-{row_id:04d}",
        "slice_version": _next_slice_version(view_name),
        "view_name": view_name,
        "dim_field": dim_field,
        "metric_field": metric_field,
        "agg_func": agg_func,
        "metric_def": f"{agg_func}({metric_field})",
        "source_scope": SOURCE_SCOPE,
        "field_mapping": {
            "dim_field": {"logical": dim_field, "physical": dim_field, "legacy": False},
            "metric_field": {"logical": metric_field, "physical": metric_field, "legacy": False},
            "legacy_aliases": dict(LEGACY_FIELD_ALIASES),
        },
        "results": {},
        "source_refs": [],
        "record_count": 0,
        "source_records": len(store.rows("parsed_entity")),
        "completion": _empty_completion(len(store.rows("parsed_entity"))),
        "status": STATUS_BUILDING,
        "refreshed_at": _now_iso(),
        "slice_digest": "",
    }
    store.rows(SLICE_TABLE).append(slice_entry)
    return slice_entry


def verify_slice(slice_entry: dict[str, Any]) -> bool:
    """核实切片：内部计数自洽 + 每条来源记录可按 ID 与内容摘要回查。"""
    refs: list[dict[str, Any]] = slice_entry.get("source_refs", [])
    results: dict[str, Any] = slice_entry.get("results", {})

    # 内部完整性：分组计数之和必须等于来源引用数。
    grouped = sum(int(bucket.get("count", 0)) for bucket in results.values())
    if grouped != len(refs):
        return False

    broken: list[str] = []
    for ref in refs:
        entity = store.find("parsed_entity", int(ref["entity_id"]))
        if entity is None:
            broken.append(str(ref["entity_id"]))
            continue
        if entity.get("status") != "校验通过":
            broken.append(str(ref["entity_id"]))
            continue
        if entity.get("content_digest") != ref.get("content_digest"):
            broken.append(str(ref["entity_id"]))
    if broken:
        slice_entry.setdefault("completion", {})["broken_sources"] = broken
        return False
    return True


def complete_slice(slice_entry: dict[str, Any]) -> dict[str, Any]:
    """按统一完成定义收尾切片，翻转状态并固化版本摘要。"""
    completion = slice_entry["completion"]
    verified_ok = (
        completion["consumed"] == len(slice_entry.get("source_refs", []))
        and not completion.get("broken_sources")
        and verify_slice(slice_entry)
    )
    completion["verified"] = completion["consumed"] if verified_ok else (
        completion["consumed"] - len(completion.get("broken_sources", []))
    )
    completion["complete"] = bool(verified_ok)
    slice_entry["status"] = STATUS_READY if verified_ok else STATUS_BROKEN
    # 摘要只覆盖口径与内容，不含状态本身，保证同输入下版本依据稳定。
    slice_entry["slice_digest"] = stable_digest({
        "view_name": slice_entry["view_name"],
        "dim_field": slice_entry["dim_field"],
        "metric_field": slice_entry["metric_field"],
        "agg_func": slice_entry["agg_func"],
        "field_mapping": slice_entry["field_mapping"],
        "source_scope": slice_entry["source_scope"],
        "source_refs": slice_entry["source_refs"],
        "results": slice_entry["results"],
    })
    return slice_entry


def build_slice(
    view_name: str,
    dim_field: str,
    metric_field: str,
    agg_func: str = "count",
) -> dict[str, Any]:
    """执行一次完整聚合刷新并产出可追溯切片。

    分组与指标累积口径与历史 ``refresh_aggregation`` 完全一致，
    只是取数边界、字段读取和来源核实改由本模块统一提供。
    """
    slice_entry = begin_slice(view_name, dim_field, metric_field, agg_func)
    results: dict[str, dict[str, Any]] = {}
    source_refs: list[dict[str, Any]] = []
    mapping = slice_entry["field_mapping"]
    dim_legacy_seen = False
    metric_legacy_seen = False

    for entity, fields_data in iter_verified_entities():
        dim_val_raw, dim_physical, dim_legacy = resolve_field(fields_data, dim_field)
        metric_val_raw, metric_physical, metric_legacy = resolve_field(
            fields_data, metric_field
        )
        dim_val = str(dim_val_raw) if dim_val_raw is not None else "unknown"
        metric_val = metric_val_raw if metric_val_raw is not None else "0"
        dim_legacy_seen = dim_legacy_seen or dim_legacy
        metric_legacy_seen = metric_legacy_seen or metric_legacy

        if dim_val not in results:
            results[dim_val] = {"count": 0, "sum": 0.0, "values": []}
        results[dim_val]["count"] += 1
        try:
            results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
        except (ValueError, TypeError):
            pass
        results[dim_val]["values"].append(metric_val)

        source_refs.append({
            "entity_id": entity["id"],
            "content_digest": entity.get("content_digest", ""),
            "source_batch": entity.get("来源批次", ""),
            "dim_via": dim_physical,
            "metric_via": metric_physical,
        })

    # 固化本次刷新实际采用的字段映射（含旧字段回退标记）。
    mapping["dim_field"]["physical"] = (
        mapping["dim_field"]["logical"] if not dim_legacy_seen
        else next(
            (old for old, new in LEGACY_FIELD_ALIASES.items()
             if new == mapping["dim_field"]["logical"]),
            mapping["dim_field"]["logical"],
        )
    )
    mapping["dim_field"]["legacy"] = dim_legacy_seen
    mapping["metric_field"]["physical"] = (
        mapping["metric_field"]["logical"] if not metric_legacy_seen
        else next(
            (old for old, new in LEGACY_FIELD_ALIASES.items()
             if new == mapping["metric_field"]["logical"]),
            mapping["metric_field"]["logical"],
        )
    )
    mapping["metric_field"]["legacy"] = metric_legacy_seen

    slice_entry["results"] = copy.deepcopy(results)
    slice_entry["source_refs"] = source_refs
    slice_entry["record_count"] = len(source_refs)
    slice_entry["completion"]["consumed"] = len(source_refs)
    return complete_slice(slice_entry)


def get_slice(slice_id: str) -> dict[str, Any] | None:
    for slice_entry in store.rows(SLICE_TABLE):
        if slice_entry.get("slice_id") == slice_id:
            return slice_entry
    return None


def list_slices(view_name: str | None = None) -> list[dict[str, Any]]:
    rows = store.rows(SLICE_TABLE)
    if view_name is None:
        return list(rows)
    return [s for s in rows if s.get("view_name") == view_name]


def get_latest_ready_slice(view_name: str) -> dict[str, Any] | None:
    """交付端取数入口：只返回最新一张「完整可核实」切片。

    「构建中」与「来源断裂」的切片不会被交付，交叠刷新时自然回退到
    上一版已发布切片。
    """
    ready = [
        s for s in store.rows(SLICE_TABLE)
        if s.get("view_name") == view_name and s.get("status") == STATUS_READY
    ]
    if not ready:
        return None
    return max(ready, key=lambda s: int(s.get("slice_version", 0)))


def has_building_slice(view_name: str) -> bool:
    return any(
        s.get("view_name") == view_name and s.get("status") == STATUS_BUILDING
        for s in store.rows(SLICE_TABLE)
    )
