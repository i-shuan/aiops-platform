"""Log 樣式比對：事件期間哪種例外訊息暴增了。補齊「Metrics → Trace → Log」報告的最後一段。

做法（輕量、可預期，不用 Drain 這類聚類套件）：
  1. 把訊息裡會變的部分洗掉：UUID、長十六進位 id -> <ID>；IP（含 port）-> <IP>；數字 -> <NUM>
     例：「Read timed out after 3012 ms (request 21)」->「Read timed out after <NUM> ms (request <NUM>)」
  2. 依 (服務, 例外類型, 樣板) 分組，分別數「事件期間」與「事件前的正常期間」的筆數，換算成每分鐘頻率
  3. 排名：平常沒出現過的（新出現）優先，其次是頻率倍率高的；事件期間少於 min_count 筆的不列
資料要靠前面的 ingestion.incident_logs 撈；沒撈到任何事件期間的日誌就略過這段。
"""
from __future__ import annotations

import re
from collections import Counter

from aiops.engine.registry import OperatorRegistry
from aiops.stages.rca.base import RcaSection

_UUID = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
_HEX_ID = re.compile(r"\b(?=[0-9a-fA-F]*\d)[0-9a-fA-F]{12,}\b")
_IP = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")
_NUM = re.compile(r"(?<![A-Za-z_])-?\d+(?:\.\d+)?")
_SPACE = re.compile(r"\s+")
MAX_TEMPLATE_LEN = 200


def template_of(message: str) -> str:
    """把訊息洗成樣板。順序有關係：先洗 UUID／長 id，再洗 IP，最後才洗剩下的數字。"""
    t = _UUID.sub("<ID>", message)
    t = _HEX_ID.sub("<ID>", t)
    t = _IP.sub("<IP>", t)
    t = _NUM.sub("<NUM>", t)
    return _SPACE.sub(" ", t).strip()[:MAX_TEMPLATE_LEN]


def rank_patterns(during, baseline, minutes: tuple[float, float], min_count: int = 3) -> list[dict]:
    """回傳依暴增程度排好的樣板：service、error_type、template、during、baseline、rate_during、rate_baseline、
    ratio（倍率；平常沒有就是 None）、new（平常沒出現過）、example（一筆原始訊息）、trace_id。"""
    m_during, m_base = (max(minutes[0], 1e-9), max(minutes[1], 1e-9))
    key = lambda r: (r.service, r.error_type, template_of(r.message))  # noqa: E731
    count_during, count_base = Counter(map(key, during)), Counter(map(key, baseline))
    example = {}
    for r in during:
        example.setdefault(key(r), r)
    out = []
    for k, n in count_during.items():
        if n < min_count:
            continue
        base = count_base.get(k, 0)
        rate_d, rate_b = n / m_during, base / m_base
        out.append({"service": k[0], "error_type": k[1], "template": k[2], "during": n, "baseline": base,
                    "rate_during": rate_d, "rate_baseline": rate_b, "ratio": (rate_d / rate_b) if base else None,
                    "new": base == 0, "example": example[k].message, "trace_id": example[k].trace_id})
    # 新出現的排前面（越多越前）；其餘依倍率、再依筆數
    out.sort(key=lambda p: (not p["new"], -(p["during"] if p["new"] else p["ratio"]), -p["during"]))
    return out


@OperatorRegistry.register("rca.section.log_patterns")
class LogPatterns(RcaSection):
    """params: top_n（列幾個，預設 3）、min_count（事件期間至少幾筆才列，預設 3）、
    min_ratio（不是新出現的樣板，頻率倍率至少多少才算暴增，預設 2）。"""

    key, title = "log_patterns", "錯誤日誌：事件期間暴增的例外"

    def __init__(self, top_n: int = 3, min_count: int = 3, min_ratio: float = 2.0):
        if top_n < 1 or min_count < 1 or min_ratio < 1:
            raise ValueError("top_n、min_count 至少 1；min_ratio 至少 1")
        self.top_n, self.min_count, self.min_ratio = int(top_n), int(min_count), float(min_ratio)

    def applies(self, incident, ctx):
        return bool(ctx.rca_logs)

    def build(self, incident, ctx):
        ranked = rank_patterns(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes, self.min_count)
        bursts = [p for p in ranked if p["new"] or p["ratio"] >= self.min_ratio][: self.top_n]
        m_during, m_base = ctx.rca_log_window_minutes
        head = f"事件期間 {len(ctx.rca_logs)} 筆錯誤日誌（{m_during:.0f} 分鐘），事件前 {m_base:.0f} 分鐘 {len(ctx.rca_logs_baseline)} 筆。"
        if not bursts:
            common = ", ".join(f"[{p['service']}] {p['template']}（{p['during']} 筆）" for p in ranked[:3]) or "無"
            return head + f"\n沒有明顯暴增的例外（新出現或頻率 ≥ {self.min_ratio:g} 倍）。事件期間最常見：{common}"
        lines = [head]
        for i, p in enumerate(bursts, 1):
            kind = f"{p['error_type']}: " if p["error_type"] else ""
            change = "新出現（平常沒有）" if p["new"] else f"平常 {p['baseline']} 筆，頻率 {p['ratio']:.1f} 倍"
            lines.append(f"{i}. [{p['service']}] {kind}{p['template']}\n"
                         f"   事件期間 {p['during']} 筆（每分鐘 {p['rate_during']:.2f}），{change}"
                         + (f"；例：trace {p['trace_id']}" if p["trace_id"] else ""))
        return "\n".join(lines)
