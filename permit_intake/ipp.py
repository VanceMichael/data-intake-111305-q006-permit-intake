"""IPP 属性组的取值口径。

打印网关已经把 RFC 8010/8011 的 IPP 编码解码成属性信封，本模块只解释
value-tag 的语义，不发起任何网络请求、不投递文档、不操作打印机。

需要保留的四种“没有可用值”的情况：

- ``no-value``：属性被显式返回，但本次没有值（RFC 8010 0x13）；
- ``unknown``：属性被支持，但当前值未知（0x12）；
- ``unsupported``：该属性不被支持（0x10）；
- 属性根本没有出现在组里：本模块记为 ``missing``，与上面三种互不等同。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: RFC 8010 规定的带外（out-of-band）value-tag。
OUT_OF_BAND_TAGS = ("unsupported", "unknown", "no-value")

#: 取值存在性的全部口径，``missing`` 表示属性未在本组返回。
PRESENCE_KINDS = ("value", "no-value", "unknown", "unsupported", "missing")

#: RFC 8011 第 5.3.7 节的 job-state 枚举。
JOB_STATE_NAMES = {
    3: "pending",
    4: "pending-held",
    5: "processing",
    6: "processing-stopped",
    7: "canceled",
    8: "aborted",
    9: "completed",
}

#: IPP 定义的终态。canceled/aborted 也是终态，但不代表纸张已输出。
TERMINAL_JOB_STATES = (7, 8, 9)


def normalize_tag(tag: Any) -> str:
    """规范化网关给出的 value-tag 文本。"""
    if not isinstance(tag, str) or not tag.strip():
        raise ValueError("attribute value-tag must be a non-empty string")
    return tag.strip().lower()


@dataclass(frozen=True)
class AttributeView:
    """单个属性在一个属性组内的观察结果。

    presence 取值见 :data:`PRESENCE_KINDS`；只有 ``value`` 时 value 才可能
    非 None，其余口径一律为 None，由调用方按口径区别处理。
    """

    name: str
    presence: str
    tag: str | None
    value: Any = None


def view_attribute(attributes: list[dict], name: str) -> AttributeView:
    """按名读取单值属性，保留 no-value/unknown/unsupported/未返回的区别。"""
    for attribute in attributes:
        if attribute.get("name") != name:
            continue
        tag = normalize_tag(attribute["tag"])
        if tag in OUT_OF_BAND_TAGS:
            return AttributeView(name, tag, tag, None)
        return AttributeView(name, "value", tag, attribute.get("value"))
    return AttributeView(name, "missing", None, None)


def collect_attribute(attributes: list[dict], name: str) -> AttributeView:
    """读取可能多值的属性（如 job-state-reasons）。

    多值属性在信封里可能是同名属性的多次出现；只要其中有带值出现，就按
    ``value`` 口径返回值列表；全部带外时返回第一个带外口径；未返回为
    ``missing``。
    """
    matches = [attribute for attribute in attributes if attribute.get("name") == name]
    if not matches:
        return AttributeView(name, "missing", None, None)
    valued: list[Any] = []
    for attribute in matches:
        tag = normalize_tag(attribute["tag"])
        if tag not in OUT_OF_BAND_TAGS:
            valued.append(attribute.get("value"))
    if valued:
        return AttributeView(name, "value", "multi", valued)
    first = matches[0]
    tag = normalize_tag(first["tag"])
    return AttributeView(name, tag, tag, None)


def integer_value(view: AttributeView) -> int | None:
    """取出严格的 IPP 整数；布尔值、非整数或无值口径都返回 None。"""
    if view.presence != "value":
        return None
    value = view.value
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value
