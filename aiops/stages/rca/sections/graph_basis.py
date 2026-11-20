"""關係圖依據：這份報告用到的每一條邊是哪裡來的、最後什麼時候觀測到，以及這張圖是什麼時候的。

為什麼要有這一段：收斂（把幾筆告警併成一張卡）與拓撲推測（topology_inference）都建立在「A 依賴 B」這條邊上。
報告不交代這條邊的出處，看的人就無從判斷它可不可信——是自己團隊宣告的、是平台從 trace 觀測到的、還是一份很久沒更新的清冊。
這一段只「呈現」ctx.graph 每條邊帶的出處（engine/graph.Provenance），不做任何判斷，也不因為邊舊就把它拿掉。

列哪些邊：兩端都在這個事件群裡的邊（就是收斂與傳播路徑實際用到的那些）。一條都沒有就略過。
沒有「圖資版本」：最後一行寫每種出處最新的時間，當作「這次用的圖是什麼時候的」。
"""
from __future__ import annotations

from datetime import datetime

from aiops.engine.graph import (ORIGIN_DECLARED, ORIGIN_INCIDENT_TRACE, ORIGIN_LOT, ORIGIN_TRACE, ORIGIN_WORKFLOW, Edge,
                                EdgeKind, Provenance)
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.stages.rca.base import RcaSection

RELATION = {EdgeKind.CALL: "呼叫", EdgeKind.ASYNC: "發訊息給", EdgeKind.FLOW: "流向", EdgeKind.SUPPLY: "供應"}
ORIGIN_TEXT = {
    ORIGIN_DECLARED: "人工宣告（匯入）",
    ORIGIN_WORKFLOW: "管線設定檔寫的",
    ORIGIN_TRACE: "平台從 trace 沉澱",
    ORIGIN_INCIDENT_TRACE: "這次事件的 trace 直接觀測到",
    ORIGIN_LOT: "從 Lot 履歷推導",
}
# 出處裡的時間是什麼意思（宣告的邊沒有人「觀測」它，只有最後一次更新）
SEEN_TEXT = {ORIGIN_DECLARED: "最後更新", ORIGIN_TRACE: "最後觀測", ORIGIN_INCIDENT_TRACE: "最後一筆"}
UNKNOWN_TEXT = "來源未記錄（事件群建立時的版本沒有記邊的出處）"


def _age(seen: datetime, reference: datetime) -> str:
    """seen 相對事件開始的時間差，寫成人看得懂的「事件開始前 3 小時 5 分」。"""
    minutes = round((as_utc(reference) - as_utc(seen)).total_seconds() / 60)
    side = "前" if minutes >= 0 else "後"
    minutes = abs(minutes)
    if minutes < 1:
        return "事件開始的同一分鐘"
    days, rest = divmod(minutes, 24 * 60)
    hours, mins = divmod(rest, 60)
    parts = [f"{days} 天"] * bool(days) + [f"{hours} 小時"] * bool(hours) + [f"{mins} 分"] * bool(mins and not days)
    return f"事件開始{side} {' '.join(parts)}"


def describe(p: Provenance, reference: datetime) -> str:
    if p.origin not in ORIGIN_TEXT:
        return UNKNOWN_TEXT
    bits = [p.detail] if p.detail else []
    if p.last_seen is not None:
        bits.append(f"{SEEN_TEXT.get(p.origin, '時間')} {as_utc(p.last_seen).strftime('%Y-%m-%d %H:%M')} UTC，{_age(p.last_seen, reference)}")
    return ORIGIN_TEXT[p.origin] + (f"（{'；'.join(bits)}）" if bits else "")


def edge_rows(edges: tuple[Edge, ...]) -> list[dict]:
    """同一條邊（同型別、同方向）的幾種出處併成一列。"""
    rows: dict[tuple[str, str, str], dict] = {}
    for e in edges:
        row = rows.setdefault((e.source, e.target, e.kind.value),
                              {"source": e.source, "target": e.target, "kind": e.kind.value, "provenance": []})
        p = e.provenance
        item = {"origin": p.origin, "detail": p.detail, "last_seen": as_utc(p.last_seen).isoformat() if p.last_seen else None}
        if item not in row["provenance"]:
            row["provenance"].append(item)
    return list(rows.values())


@OperatorRegistry.register("rca.section.graph_basis")
class GraphBasis(RcaSection):
    key, title = "graph_basis", "關係圖依據"

    def applies(self, incident, ctx):
        return bool(ctx.graph.between(incident.entities))

    def build(self, incident, ctx):
        graph = ctx.graph
        edges = graph.between(incident.entities)
        by_edge: dict[tuple[str, str, EdgeKind], list[Provenance]] = {}
        for e in edges:
            seen = by_edge.setdefault((e.source, e.target, e.kind), [])
            if e.provenance not in seen:
                seen.append(e.provenance)
        lines = []
        for (source, target, kind), origins in by_edge.items():
            # 同一條邊既有說得出來源的、又有「來源未記錄」的（宣告的呼叫邊同時也在事件群快照的拓撲裡，那一份沒有出處）：
            # 只列說得出來源的，不然每條宣告的邊都多一行「來源未記錄」
            origins = [p for p in origins if p.origin in ORIGIN_TEXT] or origins
            lines.append(f"{source} {RELATION[kind]} {target}（{kind.value}）")
            lines += [f"  - {describe(p, incident.start)}" for p in origins]
        as_of = graph.as_of()
        stamps = [f"{ORIGIN_TEXT[o]} {as_utc(t).strftime('%Y-%m-%d %H:%M')} UTC" for o, t in sorted(as_of.items()) if o in ORIGIN_TEXT]
        lines.append("圖資時間（整張圖每種來源最新的一筆）：" + ("；".join(stamps) if stamps else "沒有任何一條邊帶時間"))
        ctx.result["graph_basis"] = {"edges": edge_rows(edges),
                                     "as_of": {o: as_utc(t).isoformat() for o, t in sorted(as_of.items())}}
        return "\n".join(lines)
