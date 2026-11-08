"""RCA 用的錯誤日誌樣板與暴增排序（純函式）。報告段落 rca.section.log_patterns 與根因算子 rca.root_cause.evidence 共用。

做法（輕量、可預期，不用 Drain 這類聚類套件）：
  1. 把訊息裡會變的部分洗掉：UUID、長十六進位 id -> <ID>；IP（含 port）-> <IP>；數字 -> <NUM>
  2. 依 (服務, 例外類型, 樣板) 分組，分別加總「事件期間」與「事件前的正常期間」的筆數（每筆乘上抽樣權重），換算成每分鐘頻率
  3. 暴增倍率 = 事件期間頻率 ÷ 正常期間頻率；正常期間沒出現過的樣板，分母當成「正常期間出現 1 筆」，
     所以新出現的樣板跟既有樣板用同一把尺排序（以前新出現的一律排最前面，902 倍的既有錯誤會被幾筆新樣板擠到後面）
"""
from __future__ import annotations

import re
from collections import defaultdict

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
    """回傳依暴增倍率排好的樣板：service、error_type、template、during、baseline（加權後筆數）、rate_during、
    rate_baseline、ratio（暴增倍率；新出現的以正常期間 1 筆計）、new（正常期間沒出現過）、example、trace_id。
    事件期間（加權後）少於 min_count 筆的不列。"""
    m_during, m_base = (max(minutes[0], 1e-9), max(minutes[1], 1e-9))
    key = lambda r: (r.service, r.error_type, template_of(r.message))  # noqa: E731
    count_during: dict[tuple, float] = defaultdict(float)
    count_base: dict[tuple, float] = defaultdict(float)
    example = {}
    for r in during:
        k = key(r)
        count_during[k] += getattr(r, "weight", 1.0)
        example.setdefault(k, r)
    for r in baseline:
        count_base[key(r)] += getattr(r, "weight", 1.0)
    out = []
    for k, n in count_during.items():
        if n < min_count:
            continue
        base = count_base.get(k, 0.0)
        rate_d, rate_b = n / m_during, base / m_base
        ratio = rate_d / (max(base, 1.0) / m_base)
        out.append({"service": k[0], "error_type": k[1], "template": k[2], "during": round(n, 1), "baseline": round(base, 1),
                    "rate_during": rate_d, "rate_baseline": rate_b, "ratio": ratio, "new": base == 0,
                    "example": example[k].message, "trace_id": example[k].trace_id})
    out.sort(key=lambda p: (-p["ratio"], -p["during"]))
    return out


def service_burst(during, baseline, minutes: tuple[float, float], min_count: int = 3) -> dict[str, dict]:
    """每個服務暴增倍率最高的那個樣板（根因算子拿來當佐證）。"""
    best: dict[str, dict] = {}
    for p in rank_patterns(during, baseline, minutes, min_count):
        if p["service"] not in best:
            best[p["service"]] = p
    return best
