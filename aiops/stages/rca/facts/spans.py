"""span -> 事實。數字全部來自舊引擎的純函式（stages/rca/spans.py），這裡只負責「把數字攤開、不加解讀」。

重用（不改它們）：
  window_stats       每個服務的入口數、入口平均耗時、入口失敗率、自身耗時（區間聯集）、資源等待、自己失敗（own_error_rate）
  inbound_callers    每個服務的入口是誰呼叫的（流入流量、呼叫邊流量、扇出都從這裡數）
  resource_kinds     資源名稱 -> 種類字樣
  _children_index    同步子 span 索引（訊息消費者不算子 span），子 span 超出父 span 用它找父子

剝掉的解讀：
  - spans.py 把「子 span 超出父 span 區間」記成時鐘偏移（clock_skew_spans）。這裡只輸出 child_outlives_parent／
    child_precedes_parent：超出幾 ms、幾對、哪一對，不說是時鐘偏移、逾時放棄還是 fire-and-forget。
  - own_error_rate 在舊引擎叫「自己造成的失敗」。這裡叫 failed_without_failed_child：入口失敗、往下整條同步呼叫鏈都沒有
    失敗的 span，只陳述這個比例，不判定是誰的錯（下游沒裝 APM、錯誤被吞成 200 再由上游丟出，都長這樣）。
  - evidence.py 的流量放大歸因（往上游走找放大者、把增量轉記、ingress_surge）、主導資源（min_dependency_share）、
    顯著門檻、拉開檢查都是判斷，一律不搬。只留 _fanout 的算法（每個入口請求打下游幾次，前後比）當事實。
  - 不輸出任何「是不是異常」的旗標：增量是負的、比例是 0，照樣輸出。
"""
from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from typing import Any

from aiops.engine.context import SpanRecord
from aiops.engine.rca_types import Fact
from aiops.engine.types import as_utc
from aiops.stages.rca.facts.windows import Windows
from aiops.stages.rca.spans import (CLOCK_SKEW_TOLERANCE_MS, EXTERNAL_CALLER, MQ_CALLER_PREFIX, UNKNOWN_CALLER,
                                    _children_index, inbound_callers, resource_kinds, resource_label, window_stats)

SOURCE = "stages.rca.facts.spans"
# 呼叫者標籤：spans.inbound_callers 用空字串與 "?"，給下游消費者看的事實用看得懂的字
EXTERNAL_LABEL, UNKNOWN_LABEL = "<external>", "<unknown>"
_APP_KINDS = ("", "app")


def _caller_label(caller: str) -> str:
    return EXTERNAL_LABEL if caller == EXTERNAL_CALLER else UNKNOWN_LABEL if caller == UNKNOWN_CALLER else caller


def _is_service_caller(caller: str) -> bool:
    """同步呼叫的服務（外部入口、孤兒、訊息生產者都不是同步呼叫邊）。"""
    return caller not in (EXTERNAL_CALLER, UNKNOWN_CALLER) and not caller.startswith(MQ_CALLER_PREFIX)


def _span_key(s: SpanRecord) -> tuple:
    """全欄位排序鍵：資料源偶爾給重複的 span_id（同一份文件撈兩次、不同 trace 撞號），只用 (ts, trace, span) 排，
    重複的那幾筆誰先誰後會隨輸入順序變，下游 dict（span_id -> 服務）就會留到不同的那一筆。"""
    return (as_utc(s.ts), s.trace_id, s.span_id, s.parent_id or "", s.service, s.duration_ms, s.error, s.kind,
            s.subtype, s.resource, s.status_code, s.name, s.consumer, s.queue,
            -1.0 if s.age_ms is None else s.age_ms, s.links)


def span_facts(spans: list[SpanRecord], w: Windows, min_requests: int, skips: list[str]) -> list[Fact]:
    if not spans:
        skips.append("spans:這一輪沒有 span")
        return []
    spans = sorted(spans, key=_span_key)  # 輸入順序不同也得到同一份結果（含浮點加總順序）
    out: list[Fact] = []
    out += _service_facts(spans, w, min_requests, skips)
    out += _traffic_facts(spans, w, min_requests, skips)
    out += _outlives_facts(spans, w)
    return out


def _service_facts(spans: list[SpanRecord], w: Windows, min_requests: int, skips: list[str]) -> list[Fact]:
    during = window_stats(spans, w.start, w.end)
    before = window_stats(spans, w.base_lo, w.start) if w.has_baseline else {}
    kinds = resource_kinds(spans)
    out: list[Fact] = []
    for svc in sorted(set(during) | set(before)):
        b, d = before.get(svc), during.get(svc)
        nb, nd = (b.requests if b else 0), (d.requests if d else 0)
        b_ok, d_ok = nb >= min_requests, nd >= min_requests
        if not d_ok:
            skips.append(f"entry:{svc}:事件期間入口 {nd} 筆 < {min_requests}")
            continue
        assert d is not None
        failures = round(d.error_rate * nd)
        bare = round(d.own_error_rate * nd)
        out += [
            Fact("entry_latency", svc, d.total_ms_mean, "ms", "incident",
                 {"before": b.total_ms_mean if b_ok else None, "during": d.total_ms_mean, "n": nd, "n_before": nb}, SOURCE),
            Fact("entry_failure_rate", svc, d.error_rate, "ratio", "incident",
                 {"before": b.error_rate if b_ok else None, "during": d.error_rate, "failures": failures,
                  "n": nd, "n_before": nb}, SOURCE),
            Fact("failed_without_failed_child", svc, d.own_error_rate, "ratio", "incident",
                 {"before": b.own_error_rate if b_ok else None, "during": d.own_error_rate, "count": bare,
                  "failures": failures, "share_of_failures": bare / failures if failures else None,
                  "n": nd, "n_before": nb}, SOURCE),
        ]
        if not b_ok:
            skips.append(f"delta:{svc}:基準期入口 {nb} 筆 < {min_requests}（不算 self_time_delta／wait_delta）")
            continue
        assert b is not None
        out.append(Fact("self_time_delta", svc, d.self_ms_per_request - b.self_ms_per_request, "ms", "delta",
                        {"before": b.self_ms_per_request, "during": d.self_ms_per_request,
                         "app_before": b.app_ms_per_request, "app_during": d.app_ms_per_request,
                         "n": nd, "n_before": nb}, SOURCE))
        for res in sorted(set(b.wait_ms_per_request) | set(d.wait_ms_per_request)):
            bw, dw = b.wait_ms_per_request.get(res, 0.0), d.wait_ms_per_request.get(res, 0.0)
            out.append(Fact("wait_delta", svc, dw - bw, "ms", "delta",
                            {"target": res, "target_kind": kinds.get(res, ""), "before": bw, "during": dw,
                             "failures_before": int(b.wait_failures.get(res, 0)),
                             "failures_during": int(d.wait_failures.get(res, 0)), "n": nd, "n_before": nb}, SOURCE))
    return out


def _traffic_facts(spans: list[SpanRecord], w: Windows, min_requests: int, skips: list[str]) -> list[Fact]:
    """inbound_rate（每個服務的入口流量前後比）、traffic_drop（每條同步呼叫邊的流量前後比）、fanout_ratio（evidence._fanout）。"""
    if not w.has_baseline:
        skips.append("traffic:span 沒有涵蓋事件開始之前，沒有基準期（不算 inbound_rate／traffic_drop／fanout_ratio）")
        return []
    before, during = inbound_callers(spans, w.base_lo, w.start), inbound_callers(spans, w.start, w.end)
    bm, dm = w.base_minutes, w.incident_minutes
    out: list[Fact] = []
    for svc in sorted(set(before) | set(during)):
        nb, nd = sum(before.get(svc, {}).values()), sum(during.get(svc, {}).values())
        if nb < min_requests:
            skips.append(f"inbound_rate:{svc}:基準期入口 {nb} 筆 < {min_requests}")
            continue
        callers = sorted(set(before.get(svc, {})) | set(during.get(svc, {})))
        by_caller = {_caller_label(c): {"before": before.get(svc, {}).get(c, 0), "during": during.get(svc, {}).get(c, 0)}
                     for c in callers}
        out.append(Fact("inbound_rate", svc, (nd / dm) / (nb / bm), "ratio", "delta",
                        {"before_per_min": nb / bm, "during_per_min": nd / dm, "n": nd, "n_before": nb,
                         "by_caller": by_caller}, SOURCE))
    edges = sorted({(caller, callee) for side in (before, during) for callee, m in side.items()
                    for caller in m if _is_service_caller(caller)})
    for caller, callee in edges:
        cb, cd = before.get(callee, {}).get(caller, 0), during.get(callee, {}).get(caller, 0)
        if cb < min_requests:
            skips.append(f"edge:{caller}->{callee}:基準期呼叫 {cb} 次 < {min_requests}（不算 traffic_drop／fanout_ratio）")
            continue
        out.append(Fact("traffic_drop", caller, (cd / dm) / (cb / bm), "ratio", "delta",
                        {"callee": callee, "before_per_min": cb / bm, "during_per_min": cd / dm,
                         "calls_before": cb, "calls_during": cd, "n": cd, "n_before": cb}, SOURCE))
        # 扇出 = 呼叫者每處理一個入口請求打這個下游幾次（evidence._fanout 的算法：三個數都要 >= min_requests）
        nb_c, nd_c = sum(before.get(caller, {}).values()), sum(during.get(caller, {}).values())
        if min(nb_c, nd_c) < min_requests:
            skips.append(f"fanout:{caller}->{callee}:呼叫者入口 {nb_c}→{nd_c} 筆，有一邊 < {min_requests}")
            continue
        fb, fd = cb / nb_c, cd / nd_c
        out.append(Fact("fanout_ratio", caller, fd / fb, "ratio", "delta",
                        {"callee": callee, "before": fb, "during": fd, "calls_before": cb, "calls_during": cd,
                         "n": nd_c, "n_before": nb_c}, SOURCE))
    return out


def _child_label(parent: SpanRecord, child: SpanRecord) -> str:
    """子 span 是誰：另一個服務就寫服務名；同服務的資源呼叫寫資源名（redis、tax-gw:443）；程式自己的 span 寫 app。"""
    if child.service != parent.service:
        return child.service
    if child.kind not in _APP_KINDS:
        return resource_label(child)
    return child.kind or child.service  # "app"，或同服務內再開的 transaction


def _outlives_facts(spans: list[SpanRecord], w: Windows) -> list[Fact]:
    """子 span 有一段落在父 span 區間外：晚於父結束（child_outlives_parent）／早於父開始（child_precedes_parent）。
    依 (父服務, 子) 分組，各窗口各一筆。超出不到 CLOCK_SKEW_TOLERANCE_MS（毫秒截斷）不算，跟舊引擎同一把尺。
    沒有任何一對超出就不輸出（n 對都在區間內不是「資料不足」，是事實為零；只是沒有必要逐對列 0）。"""
    children = _children_index(spans)
    windows = [("incident", w.start, w.end)] + ([("baseline", w.base_lo, w.start)] if w.has_baseline else [])
    out: list[Fact] = []
    for name, lo, hi in windows:
        pairs: dict[tuple[str, str], int] = defaultdict(int)
        late: dict[tuple[str, str], list[tuple[float, str, str, str]]] = defaultdict(list)
        early: dict[tuple[str, str], list[tuple[float, str, str, str]]] = defaultdict(list)
        for p in spans:
            if not (lo <= as_utc(p.ts) < hi):
                continue
            origin = as_utc(p.ts)
            for c in children.get(p.span_id, ()):
                key = (p.service, _child_label(p, c))
                pairs[key] += 1
                offset = (as_utc(c.ts) - origin) / timedelta(milliseconds=1)
                over = offset + c.duration_ms - p.duration_ms
                if over > CLOCK_SKEW_TOLERANCE_MS:
                    late[key].append((over, p.trace_id, p.span_id, c.span_id))
                if -offset > CLOCK_SKEW_TOLERANCE_MS:
                    early[key].append((-offset, p.trace_id, p.span_id, c.span_id))
        for kind, groups in (("child_outlives_parent", late), ("child_precedes_parent", early)):
            for key in sorted(groups):
                out.append(_outlives_fact(kind, key, groups[key], pairs[key], name))
    return out


def _outlives_fact(kind: str, key: tuple[str, str], hits: list[tuple[float, str, str, str]], n: int, window: str) -> Fact:
    ms = sorted(h[0] for h in hits)
    worst = max(hits, key=lambda h: (h[0], h[1], h[2], h[3]))
    detail: dict[str, Any] = {"parent": key[0], "child": key[1], "count": len(hits), "n": n, "share": len(hits) / n,
                              "p50_ms": ms[len(ms) // 2], "max_ms": ms[-1],
                              "sample": {"trace_id": worst[1], "parent_span": worst[2], "child_span": worst[3]}}
    return Fact(kind, key[0], ms[-1], "ms", window, detail, SOURCE)  # type: ignore[arg-type]
