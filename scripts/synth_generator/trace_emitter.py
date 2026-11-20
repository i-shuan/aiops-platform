"""Traces 與 Error Logs：一次請求走訪一整棵呼叫樹，所有文件共用同一個 trace.id，父子 id 嚴格串接。

照 Elastic APM 資料模型發兩種 trace 文件（平台 aiops/sources/grafana.py 的 _span 就是這樣讀的）：
- processor.event: transaction —— transaction.id、transaction.duration.us；被別的服務呼叫時 parent.id = 上游那個 client span 的 span.id
- processor.event: span —— span.id、span.duration.us、parent.id = 所屬 transaction 的 id、transaction.id、
  span.destination.service.resource（對外呼叫的目標）
所以上游的 client span 與下游的 transaction 用 span.id ↔ parent.id 接起來，engine/topology.service_edges 算得出 (caller, callee)。

耗時是加總出來的：transaction = 自己的處理時間 + 依序呼叫的每個 span；呼叫下游服務的 span = 網路 + 下游整段 transaction。
每個量都從 SystemState 那一分鐘的值取（跟 metrics 同一個狀態函數），所以 trace 的期望耗時 = metrics 的 e2e_ms。

Error log（processor.event: error）在兩種情況寫，跟出事的 span／transaction 帶同一個 trace.id：
- 出錯：這個服務自己出錯（例外訊息依服務），或呼叫下游失敗（「下游回 503」）。parent.id 指向出錯的那個 transaction 或 span
- 延遲超標：transaction 耗時超過平常期望值的 SLOW_FACTOR 倍（handled: true 的警告）
error.exception 刻意寫成「單一物件」而不是 Elastic APM 的陣列，Grafana 的 log 面板才走得進 error.exception.message；
另外寫頂層 message 字串當保險。
"""
from __future__ import annotations

import math
import random
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np

from .documents import common, grouping_key, hex_id
from .faults import iso
from .topology import Call
from .waveforms import EXTERNAL_SIGMA, SELF_SIGMA, SystemState, _rng

SLOW_FACTOR = 2.5
PRE_CALL_SHARE = 0.3   # 自己的處理時間有多少比例在呼叫下游之前

# 服務自己出錯時的例外（type, message）。message 裡的 {n} 會換成這次請求的序號，讓 log 一行一行看得出差別
OWN_ERRORS = {
    "api-gw": ("UpstreamResetError", "upstream connect error or disconnect/reset before headers (request {n})"),
    "auth-svc": ("RedisTimeoutError", "redis GET session timed out after 500 ms (request {n})"),
    "cart-api": ("SQLTransientConnectionException",
                 "HikariPool-1 - Connection is not available, request timed out after 3000 ms (request {n})"),
    "ord-svc": ("RejectedExecutionException", "order worker queue full (capacity 200), request {n} rejected"),
    "bill-svc": ("ReadTimeoutError", "Read timed out calling tax-gw after 2000 ms (request {n})"),
}


def _poisson(rng: random.Random, lam: float) -> int:
    """Knuth 演算法（lam 很小，重試次數用）。用 random.Random 才能跟其他亂數同一條確定性序列。"""
    if lam <= 0:
        return 0
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def _lognormal(rng: random.Random, mean: float, sigma: float) -> float:
    return rng.lognormvariate(math.log(max(mean, 1e-6)) - sigma * sigma / 2, sigma)


@dataclass
class TraceOut:
    traces: list[dict] = field(default_factory=list)
    logs: list[dict] = field(default_factory=list)


class _Walker:
    def __init__(self, state: SystemState, rng: random.Random, minute: int, trace_id: str, seq: int):
        self.state, self.rng, self.i, self.trace_id, self.seq = state, rng, minute, trace_id, seq
        self.out = TraceOut()

    def _us(self, ms: float) -> int:
        return max(int(round(ms * 1000)), 1)

    def transaction(self, name: str, start: datetime, parent_id: str | None) -> tuple[float, bool, str]:
        """走訪一個服務的 transaction（含它所有對外呼叫）。回傳 (總耗時 ms, 是否失敗, transaction.id)。"""
        st, rng, i = self.state, self.rng, self.i
        svc = st.topo.services[name]
        tx_id = hex_id(rng, 64)
        self_total = _lognormal(rng, float(st.self_ms[name][i]), SELF_SIGMA)
        cursor = start + timedelta(milliseconds=self_total * PRE_CALL_SHARE)
        spans: list[dict] = []
        failed_call: tuple[Call, str] | None = None
        for call in svc.calls:
            if rng.random() >= call.prob:
                continue
            is_service = st.topo.is_service(call.callee)
            attempts = 1 + _poisson(rng, float(st.mult[name][i]) - 1) if is_service else 1
            last: tuple[str, bool] = ("", False)
            for _ in range(attempts):
                span_id = hex_id(rng, 64)
                if is_service:
                    net = call.base_ms * rng.uniform(0.8, 1.2)
                    child_ms, child_failed, _ = self.transaction(
                        call.callee, cursor + timedelta(milliseconds=net / 2), span_id)
                    dur, span_failed = net + child_ms, child_failed
                else:
                    dur, span_failed = _lognormal(rng, call.base_ms, EXTERNAL_SIGMA), False
                spans.append(self._span(svc.name, tx_id, span_id, call, cursor, dur, span_failed))
                cursor += timedelta(milliseconds=dur)
                last = (span_id, span_failed)
            # 重試風暴：每次嘗試都會做，最後一次的結果才算這次呼叫的結果；記第一個失敗的呼叫
            if last[1] and failed_call is None:
                failed_call = (call, last[0])
        duration = (cursor - start).total_seconds() * 1000 + self_total * (1 - PRE_CALL_SHARE)
        own_failed = rng.random() < float(st.own_err[name][i])
        failed = own_failed or failed_call is not None
        doc = {
            "@timestamp": iso(start), **common(svc, "traces", st.scenario.namespace),
            "processor": {"event": "transaction"},
            "timestamp": {"us": int(start.timestamp() * 1_000_000)},
            "trace": {"id": self.trace_id},
            "transaction": {"id": tx_id, "name": svc.transaction, "type": "request", "sampled": True,
                            "result": "HTTP 5xx" if failed else "HTTP 2xx",
                            "duration": {"us": self._us(duration)}, "span_count": {"started": len(spans)}},
            "event": {"outcome": "failure" if failed else "success"},
        }
        if parent_id:
            doc["parent"] = {"id": parent_id}
        self.out.traces.append(doc)
        self.out.traces.extend(spans)
        at = start + timedelta(milliseconds=duration)
        if own_failed:
            etype, msg = OWN_ERRORS.get(name, ("InternalError", f"{name} internal error (request {{n}})"))
            self._error(name, tx_id, tx_id, at, etype, msg.format(n=self.seq), handled=False)
        elif failed_call is not None:
            call, span_id = failed_call
            self._error(name, tx_id, span_id, at, "HttpServerErrorException",
                        f"503 Service Unavailable from {call.callee} on {call.name} (request {self.seq})", handled=False)
        elif duration > SLOW_FACTOR * st.baseline_e2e_ms(name):
            self._error(name, tx_id, tx_id, at, "SlowTransactionWarning",
                        f"{svc.transaction} took {duration:.0f} ms, over SLO {SLOW_FACTOR * st.baseline_e2e_ms(name):.0f} ms "
                        f"(request {self.seq})", handled=True)
        return duration, failed, tx_id

    def _span(self, service: str, tx_id: str, span_id: str, call: Call, start: datetime, dur_ms: float,
              failed: bool) -> dict:
        svc = self.state.topo.services[service]
        resource = f"{call.callee}:8080" if self.state.topo.is_service(call.callee) else call.callee
        return {
            "@timestamp": iso(start), **common(svc, "traces", self.state.scenario.namespace),
            "processor": {"event": "span"},
            "timestamp": {"us": int(start.timestamp() * 1_000_000)},
            "trace": {"id": self.trace_id},
            "transaction": {"id": tx_id},
            "parent": {"id": tx_id},
            "span": {"id": span_id, "name": call.name or f"call {call.callee}", "type": call.span_type,
                     "subtype": call.subtype, "duration": {"us": self._us(dur_ms)},
                     "destination": {"service": {"resource": resource}}},
            "event": {"outcome": "failure" if failed else "success"},
        }

    def _error(self, service: str, tx_id: str, parent_id: str, at: datetime, etype: str, message: str,
               handled: bool) -> None:
        svc = self.state.topo.services[service]
        culprit = f"{service} {svc.transaction}"
        self.out.logs.append({
            "@timestamp": iso(at), **common(svc, "logs", self.state.scenario.namespace),
            "processor": {"event": "error"},
            "timestamp": {"us": int(at.timestamp() * 1_000_000)},
            "trace": {"id": self.trace_id},
            "transaction": {"id": tx_id, "name": svc.transaction, "type": "request", "sampled": True},
            "parent": {"id": parent_id},
            "error": {"id": hex_id(self.rng, 128), "grouping_key": grouping_key(service, etype), "culprit": culprit,
                      "exception": {"type": etype, "message": message, "handled": handled}},
            "message": message,
            "log": {"level": "warning" if handled else "error"},
        })


def trace_rate_per_minute(state: SystemState) -> np.ndarray:
    """雙速：事件前後 ±margin 分鐘每分鐘 dense_per_minute 條 trace；其他時間每小時 sparse_per_hour 條健康樣本。"""
    cfg, w = state.scenario.traces, state.scenario.window
    rate = np.full(len(state.ts), cfg["sparse_per_hour"] / 60.0)
    for inc in state.scenario.incidents:
        lo = (inc.start.timestamp() - w.start.timestamp()) / 60 - cfg["margin_minutes"]
        hi = (inc.end.timestamp() - w.start.timestamp()) / 60 + cfg["margin_minutes"]
        idx = np.arange(len(rate))
        rate[(idx >= lo) & (idx < hi)] = cfg["dense_per_minute"]
    return rate


def trace_and_log_docs(state: SystemState) -> Iterator[TraceOut]:
    """一分鐘一批（依時間排序）。每條 trace 的亂數種子由 (seed, 分鐘, 第幾條) 決定，改了別段不影響這段。"""
    counts = _rng(state.scenario.seed, 3).poisson(trace_rate_per_minute(state))
    seq = 0
    for i, n in enumerate(counts):
        if n == 0:
            continue
        minute = (state.scenario.window.start + timedelta(minutes=i)).astimezone(timezone.utc)
        batch = TraceOut()
        rngs = [random.Random(f"{state.scenario.seed}:trace:{i}:{j}") for j in range(int(n))]
        starts = sorted((r.uniform(0, 59.0), j) for j, r in enumerate(rngs))
        for offset, j in starts:
            seq += 1
            rng = rngs[j]
            walker = _Walker(state, rng, i, hex_id(rng, 128), seq)
            walker.transaction(state.topo.entry, minute + timedelta(seconds=offset), None)
            batch.traces.extend(walker.out.traces)
            batch.logs.extend(walker.out.logs)
        yield batch
