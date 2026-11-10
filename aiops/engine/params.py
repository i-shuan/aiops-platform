"""參數的合併與分層挑選。純函式，不碰 DB；偵測算子、hooks、調參回測共用同一套規則。"""
from __future__ import annotations

import math
from copy import deepcopy
from typing import Any

# 調參的「不限定」維度值。用獨立欄位存（不是組字串），實體名稱裡有冒號（IPv6、namespace:pod）也不會歧義
ANY = "*"


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """遞迴合併：override 的鍵蓋過 base，巢狀 dict 逐層合併而不是整個換掉。兩邊都不會被修改。"""
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def number(name: str, value: Any, *, minimum: float | None = None, above: float | None = None,
           maximum: float | None = None) -> float:
    """算子參數的載入期檢查：必須是有限的數值（不收 true／false、字串、NaN、inf），再檢查範圍。
    above = 必須嚴格大於；minimum／maximum = 含端點。不合就丟 ValueError（引擎轉成 WorkflowError，啟動時就擋下）。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} 必須是有限的數值，不是 {value!r}")
    if above is not None and not value > above:
        raise ValueError(f"{name} 必須大於 {above:g}，不是 {value:g}")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} 不能小於 {minimum:g}，不是 {value:g}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} 不能大於 {maximum:g}，不是 {value:g}")
    return float(value)


def positive(name: str, value: Any) -> float:
    """窗口、冷卻、門檻這類「0 或負數沒有意義」的參數。"""
    return number(name, value, above=0)


def whole(name: str, value: Any, *, minimum: int = 1) -> int:
    """點數、筆數：必須是整數（不收 1.5、true），且不小於 minimum。"""
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} 必須是 >= {minimum} 的整數，不是 {value!r}")
    return value


def scope_rank(pipeline: str, entity: str) -> int:
    """越具體分數越高：管線×實體 3 > 管線 2 > 實體 1 > 全域 0。"""
    return (2 if pipeline != ANY else 0) + (1 if entity != ANY else 0)


def pick_most_specific(rows: list[dict[str, Any]], pipeline: str, entity: str) -> dict[str, Any] | None:
    """從候選設定（pipeline ∈ {pipeline, *}、entity ∈ {entity, *}）挑出最具體、且真的適用的一筆。

    呼叫端用一次查詢撈出所有候選（不要分四次查），再交給這裡依優先序挑。
    """
    applicable = [r for r in rows if r["pipeline"] in (pipeline, ANY) and r["entity"] in (entity, ANY)]
    if not applicable:
        return None
    return max(applicable, key=lambda r: scope_rank(r["pipeline"], r["entity"]))
