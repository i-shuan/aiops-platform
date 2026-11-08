"""參數的合併與分層挑選。純函式，不碰 DB；偵測算子、hooks、調參回測共用同一套規則。"""
from __future__ import annotations

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
