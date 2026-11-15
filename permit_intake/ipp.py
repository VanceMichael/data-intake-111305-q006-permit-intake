"""IPP 属性信封解析。

打印网关把已解码的 IPP 响应整理成 JSON 信封：一条记录对应一次响应，
``groups`` 保留属性组边界，每个属性保留 value-tag。本模块只做解析，
不接触网络与存储。解析口径：

- 每个 ``job-attributes`` 组是一个作业，属性不跨组合并、不摊平；
- 属性取值区分三种在场状态：``value``（有具体值）、``out_of_band``
  （带外值，如 no-value/unknown）、``absent``（响应未返回该属性）；
- 信封字段缺失或类型不符时记录问题，不静默改写来源内容。
"""
from __future__ import annotations

import hashlib
import json

OUT_OF_BAND_TAGS = frozenset({"no-value", "unknown", "unsupported", "not-settable", "default"})

VALUE = "value"
OUT_OF_BAND = "out_of_band"
ABSENT = "absent"

JOB_STATE_NAMES = {
    3: "pending",
    4: "pending-held",
    5: "processing",
    6: "processing-stopped",
    7: "canceled",
    8: "aborted",
    9: "completed",
}
TERMINAL_JOB_STATES = frozenset({7, 8, 9})

ABSENT_PRESENCE = {"state": ABSENT, "tag": None, "values": []}


class EnvelopeError(ValueError):
    """信封结构不满足最低要求（缺身份字段或组结构不可用）。"""


def _presence_of(tag, value):
    if isinstance(tag, str) and tag in OUT_OF_BAND_TAGS:
        return {"state": OUT_OF_BAND, "tag": tag, "values": []}
    if value is None:
        # 具体 value-tag 却带空值：按带外处理并保留原 tag，不当作有值。
        return {"state": OUT_OF_BAND, "tag": tag if isinstance(tag, str) else None, "values": []}
    return {"state": VALUE, "tag": tag if isinstance(tag, str) else None, "values": [value]}


def group_attributes(group):
    """把一个属性组规范化为 name -> presence；同名属性重复出现时保留全部取值。"""
    merged = {}
    attributes = group.get("attributes", [])
    if not isinstance(attributes, list):
        return merged
    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue
        name = attribute.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        current = _presence_of(attribute.get("tag"), attribute.get("value"))
        existing = merged.get(name)
        if existing is None:
            merged[name] = current
        elif current["state"] == VALUE:
            if existing["state"] == VALUE:
                existing["values"].extend(current["values"])
            else:
                merged[name] = current
    return merged


def presence(attributes, name):
    """读取规范化后的属性在场状态；未返回时得到 ABSENT 副本。"""
    return dict(attributes.get(name, ABSENT_PRESENCE))


def envelope_number(record, name):
    """信封顶层数值字段：接受裸数字或 {"tag", "value"} 形态，其余按带外/未返回处理。"""
    if name not in record:
        return dict(ABSENT_PRESENCE)
    raw = record[name]
    if isinstance(raw, dict):
        return _presence_of(raw.get("tag"), raw.get("value"))
    if isinstance(raw, bool):
        return {"state": OUT_OF_BAND, "tag": None, "values": []}
    if isinstance(raw, (int, float)):
        return {"state": VALUE, "tag": None, "values": [raw]}
    return {"state": OUT_OF_BAND, "tag": None, "values": []}


def content_hash(raw):
    canonical = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_record(raw):
    """规范化一条信封记录，返回 (record, problems)。

    身份字段（gateway/observation）缺失或组结构不可用时抛 EnvelopeError；
    其他问题记入 problems，记录仍然可用。每个组同时保留规范化属性与原文。
    """
    if not isinstance(raw, dict):
        raise EnvelopeError("record is not an object")
    gateway = raw.get("gateway")
    if not isinstance(gateway, str) or not gateway.strip():
        raise EnvelopeError("gateway identity missing")
    observation = raw.get("observation")
    if isinstance(observation, bool) or not isinstance(observation, int):
        raise EnvelopeError("observation must be an integer")
    groups = raw.get("groups")
    if not isinstance(groups, list) or any(not isinstance(group, dict) for group in groups):
        raise EnvelopeError("groups must be a list of objects")

    problems = []
    normalized_groups = []
    for group in groups:
        tag = group.get("tag")
        if not isinstance(tag, str) or not tag.strip():
            problems.append("group_without_tag")
            continue
        normalized_groups.append(
            {"tag": tag.strip(), "attributes": group_attributes(group), "raw": group}
        )

    printer_uuid = raw.get("printer_uuid")
    if printer_uuid is not None and (not isinstance(printer_uuid, str) or not printer_uuid.strip()):
        problems.append("printer_uuid_unusable")
        printer_uuid = None
    printer_uri = raw.get("printer_uri")
    if printer_uri is not None and (not isinstance(printer_uri, str) or not printer_uri.strip()):
        problems.append("printer_uri_unusable")
        printer_uri = None

    def _optional_text(key):
        value = raw.get(key)
        return value.strip() if isinstance(value, str) and value.strip() else None

    record = {
        "gateway": gateway.strip(),
        "observation": observation,
        "printer_uuid": printer_uuid.strip() if printer_uuid else None,
        "printer_uri": printer_uri.strip() if printer_uri else None,
        "printer_up_time": envelope_number(raw, "printer_up_time"),
        "operation": _optional_text("operation"),
        "which_jobs": _optional_text("which_jobs") or _optional_text("which-jobs"),
        "groups": normalized_groups,
        "content_hash": content_hash(raw),
    }
    return record, problems
