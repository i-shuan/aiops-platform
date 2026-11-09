"""世界模擬器：服務呼叫圖 + 每分鐘參數陣列 -> 指標陣列與 APM trace 文件。

所有時變參數（請求率、自身耗時、依賴耗時、失敗率…）都是長度 = 分鐘數的 numpy 陣列，場景用 world.set(...) 在區間內改值。
指標用「解析期望值 + 取樣噪音」向量化算出（幾萬分鐘也很快）；trace 只在需要的時窗用同一組參數逐請求走訪。
"""
from __future__ import annotations

import math
import random
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np

CRASH_MS = 2.0          # 一進 controller 就拋例外的請求耗時
SELF_SIGMA = 0.15       # 每個請求耗時的對數常態 sigma
PRE_CALL_SHARE = 0.3    # 自身處理時間有多少在呼叫下游之前


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class Call:
    callee: str                    # 服務名，或外部資源名（redis、postgres、payment-gw:443…）
    prob: float = 1.0
    kind: str = "service"          # service 或 APM span.type：db／external／cache
    subtype: str = ""              # http／redis／postgresql／oracle
    ms: float | np.ndarray = 1.0   # 資源：這次呼叫的平均耗時；服務：網路來回
    fail_p: float | np.ndarray = 0.0   # 資源呼叫失敗（逾時、回錯）的機率
    swallow: bool = False          # 呼叫端接住例外：交易照樣回 200
    name: str = ""                 # span.name

    def is_service(self) -> bool:
        return self.kind == "service"


@dataclass
class Svc:
    name: str
    transaction: str
    self_ms: float | np.ndarray
    calls: list[Call] = field(default_factory=list)
    own_fail_p: float | np.ndarray = 0.0     # 這個服務自己出錯的機率
    crash_p: float | np.ndarray = 0.0        # 一進 controller 就崩潰（沒有任何子 span）的機率
    error_type: str = "InternalError"
    error_message: str = "internal error"
    version: str = "1.0.0"
    # True：所有下游呼叫同一個起點並行發出（asyncio.gather／執行緒池），請求耗時 = 自身 + 最慢的那個呼叫；
    # False（預設）：依序呼叫，cursor 逐個往後推
    parallel: bool = False


class World:
    def __init__(self, t0: datetime, minutes: int, services: list[Svc], entries: dict[str, float | np.ndarray],
                 seed: int = 7, seasonal: float = 0.5):
        self.t0, self.minutes, self.seed = t0, minutes, seed
        self.svcs = {s.name: s for s in services}
        self.rng = np.random.default_rng(seed)
        self.pyrng = random.Random(seed)
        idx = np.arange(minutes)
        mod = ((t0.hour * 60 + t0.minute + idx) % 1440) / 1440.0
        self.season = 1.0 + seasonal * np.sin(2 * np.pi * (mod - 0.58))
        self.rate_mult = np.ones(minutes)
        self._entries = {k: v for k, v in entries.items()}
        for s in self.svcs.values():
            s.self_ms = self._a(s.self_ms)
            s.own_fail_p = self._a(s.own_fail_p)
            s.crash_p = self._a(s.crash_p)
            for c in s.calls:
                c.ms, c.fail_p = self._a(c.ms), self._a(c.fail_p)
        self._built = False

    # ── 參數 ──
    def _a(self, x) -> np.ndarray:
        return np.array(x, dtype=float).copy() if isinstance(x, np.ndarray) else np.full(self.minutes, float(x))

    def m(self, ts: datetime) -> int:
        return int((ts - self.t0).total_seconds() // 60)

    def window(self, lo: datetime, hi: datetime) -> slice:
        return slice(max(self.m(lo), 0), min(self.m(hi), self.minutes))

    def set(self, target: np.ndarray, lo: datetime, hi: datetime, value) -> None:
        target[self.window(lo, hi)] = value
        self._built = False

    def scale(self, target: np.ndarray, lo: datetime, hi: datetime, factor: float) -> None:
        target[self.window(lo, hi)] *= factor
        self._built = False

    def call(self, svc: str, callee: str) -> Call:
        return next(c for c in self.svcs[svc].calls if c.callee == callee)

    # ── 解析期望值（向量化）──
    def _order(self) -> list[str]:
        done, seen = [], set()

        def visit(n):
            if n in seen:
                return
            seen.add(n)
            for c in self.svcs[n].calls:
                if c.is_service():
                    visit(c.callee)
            done.append(n)
        for n in sorted(self.svcs):
            visit(n)
        return done  # callee 在前

    def build(self) -> None:
        if self._built:
            return
        order = self._order()
        self.e_dur, self.p_fail, self.d_ok, self.d_fail, self.p_fail_rest = {}, {}, {}, {}, {}
        for n in order:
            s = self.svcs[n]
            e = s.self_ms.copy()
            ok_rest = 1.0 - s.own_fail_p
            slowest = np.zeros(self.minutes)
            for c in s.calls:
                child_ms = c.ms + self.e_dur[c.callee] if c.is_service() else c.ms
                child_fail = self.p_fail[c.callee] if c.is_service() else c.fail_p
                if s.parallel:  # 並行：期望值近似成「最慢的那個」（每個呼叫的期望值取最大，忽略 max 的 Jensen 偏差）
                    slowest = np.maximum(slowest, c.prob * child_ms)
                else:
                    e += c.prob * child_ms
                ok_rest = ok_rest * (1.0 - c.prob * child_fail * (0.0 if c.swallow else 1.0))
            e += slowest
            fail_rest = 1.0 - ok_rest
            tot_fail = s.crash_p + (1.0 - s.crash_p) * fail_rest
            self.e_dur[n] = s.crash_p * CRASH_MS + (1.0 - s.crash_p) * e
            self.p_fail[n], self.p_fail_rest[n] = tot_fail, fail_rest
            fail_mass = s.crash_p * CRASH_MS + (1.0 - s.crash_p) * fail_rest * e
            ok_mass = (1.0 - s.crash_p) * ok_rest * e
            with np.errstate(divide="ignore", invalid="ignore"):
                self.d_fail[n] = np.where(tot_fail > 1e-9, fail_mass / tot_fail, e)
                self.d_ok[n] = np.where(tot_fail < 1 - 1e-9, ok_mass / (1.0 - tot_fail), e)
        rate = {n: np.zeros(self.minutes) for n in self.svcs}
        for n, base in self._entries.items():
            rate[n] += self._a(base) * self.season * self.rate_mult
        for n in reversed(order):  # caller 在前
            s = self.svcs[n]
            for c in s.calls:
                if c.is_service():
                    rate[c.callee] += rate[n] * (1.0 - s.crash_p) * c.prob
        self.rate = rate
        self._sample_metrics()
        self._built = True

    def _sample_metrics(self) -> None:
        self.n_ok, self.n_fail, self.sum_ok, self.sum_fail = {}, {}, {}, {}
        for k, n in enumerate(sorted(self.svcs)):
            r = np.random.default_rng([self.seed, k, int(self.rate[n].sum() * 1000) % 100003])
            tot = r.poisson(np.maximum(self.rate[n], 0.0))
            fail = r.binomial(tot, np.clip(self.p_fail[n], 0, 1))
            ok = tot - fail
            noise_ok = np.exp(r.normal(0, 1, self.minutes) * 0.1 / np.sqrt(np.maximum(ok, 1)))
            noise_fail = np.exp(r.normal(0, 1, self.minutes) * 0.1 / np.sqrt(np.maximum(fail, 1)))
            self.n_ok[n], self.n_fail[n] = ok, fail
            self.sum_ok[n] = ok * self.d_ok[n] * 1000.0 * noise_ok       # µs
            self.sum_fail[n] = fail * self.d_fail[n] * 1000.0 * noise_fail

    # ── trace（逐請求）──
    def traces(self, lo: datetime, hi: datetime) -> tuple[list[dict], list[dict]]:
        """[lo, hi) 內所有請求的 APM 文件（transaction + span）與錯誤日誌，依時間排序。"""
        self.build()
        rng = random.Random(self.seed * 31 + 17)
        docs: list[dict] = []
        logs: list[dict] = []
        seq = [0]
        for m in range(max(self.m(lo), 0), min(self.m(hi), self.minutes)):
            ts0 = self.t0 + timedelta(minutes=m)
            for entry in self._entries:
                n = np.random.default_rng([self.seed, m, zlib.crc32(entry.encode()) % 9973]).poisson(max(self.rate[entry][m], 0.0))
                for _ in range(int(n)):
                    seq[0] += 1
                    tid = f"{rng.getrandbits(128):032x}"
                    start = ts0 + timedelta(milliseconds=rng.uniform(0, 59000))
                    self._walk(entry, m, start, None, tid, rng, docs, logs, seq[0])
        docs.sort(key=lambda d: (d["@timestamp"], d["trace"]["id"]))
        logs.sort(key=lambda d: d["@timestamp"])
        return docs, logs

    def _ln(self, rng: random.Random, mean: float, sigma: float = SELF_SIGMA) -> float:
        return rng.lognormvariate(math.log(max(mean, 1e-6)) - sigma * sigma / 2, sigma)

    def _common(self, svc: Svc, ts: datetime, tid: str) -> dict:
        return {"@timestamp": iso(ts), "trace": {"id": tid}, "service": {"name": svc.name, "environment": "prod", "version": svc.version},
                "labels": {"dept": "payments"}, "data_stream": {"type": "traces", "dataset": "apm", "namespace": "chaos"}}

    def _walk(self, name: str, m: int, start: datetime, parent: str | None, tid: str, rng: random.Random,
              docs: list, logs: list, seq: int) -> tuple[float, bool]:
        svc = self.svcs[name]
        tx_id = f"{rng.getrandbits(64):016x}"
        spans: list[dict] = []
        status, failed, err_msg, err_parent = 200, False, "", tx_id
        if rng.random() < float(svc.crash_p[m]):
            dur, failed, status = self._ln(rng, CRASH_MS), True, 500
            err_msg, err_parent = svc.error_message, tx_id
        else:
            self_total = self._ln(rng, float(svc.self_ms[m]))
            cursor = start + timedelta(milliseconds=self_total * PRE_CALL_SHARE)
            fork, slowest = cursor, 0.0  # 並行：每個呼叫都從 fork 出發，結束後 cursor 推到最慢的那個
            failed_call = False
            for c in svc.calls:
                if svc.parallel:
                    cursor = fork
                if rng.random() >= c.prob:
                    continue
                sid = f"{rng.getrandbits(64):016x}"
                if c.is_service():
                    net = float(c.ms[m]) * rng.uniform(0.8, 1.2)
                    child_ms, child_failed = self._walk(c.callee, m, cursor + timedelta(milliseconds=net / 2), sid, tid, rng, docs, logs, seq)
                    dur_c, sfailed = net + child_ms, child_failed
                    stype, ssub, res = "external", "http", f"{c.callee}:8080"
                else:
                    dur_c = self._ln(rng, float(c.ms[m]))
                    sfailed = rng.random() < float(c.fail_p[m])
                    stype, ssub, res = c.kind, c.subtype, c.callee
                spans.append({**self._common(svc, cursor, tid), "processor": {"event": "span"}, "timestamp": {"us": int(cursor.timestamp() * 1e6)},
                              "transaction": {"id": tx_id}, "parent": {"id": tx_id},
                              "span": {"id": sid, "name": c.name or f"call {c.callee}", "type": stype, "subtype": ssub,
                                       "duration": {"us": max(int(dur_c * 1000), 1)}, "destination": {"service": {"resource": res}}},
                              "event": {"outcome": "failure" if sfailed else "success"}} if not c.is_service() else
                             {**self._common(svc, cursor, tid), "processor": {"event": "span"}, "timestamp": {"us": int(cursor.timestamp() * 1e6)},
                              "transaction": {"id": tx_id}, "parent": {"id": tx_id},
                              "span": {"id": sid, "name": c.name or f"POST {c.callee}", "type": stype, "subtype": ssub,
                                       "duration": {"us": max(int(dur_c * 1000), 1)}, "destination": {"service": {"resource": res}}},
                              "event": {"outcome": "failure" if sfailed else "success"}})
                cursor += timedelta(milliseconds=dur_c)
                slowest = max(slowest, dur_c)
                if sfailed and not c.swallow:
                    failed_call = True
                    err_msg, err_parent = f"{c.callee} call failed ({c.name or c.subtype})", sid
            if svc.parallel:
                cursor = fork + timedelta(milliseconds=slowest)
            dur = (cursor - start).total_seconds() * 1000 + self_total * (1 - PRE_CALL_SHARE)
            own = rng.random() < float(svc.own_fail_p[m])
            failed = own or failed_call
            if own:
                err_msg, err_parent = svc.error_message, tx_id
            status = 500 if failed else 200
        tx = {**self._common(svc, start, tid), "processor": {"event": "transaction"}, "timestamp": {"us": int(start.timestamp() * 1e6)},
              "transaction": {"id": tx_id, "name": svc.transaction, "type": "request", "result": f"HTTP {status // 100}xx",
                              "duration": {"us": max(int(dur * 1000), 1)}, "span_count": {"started": len(spans)}},
              "http": {"response": {"status_code": status}}, "event": {"outcome": "failure" if failed else "success"}}
        if parent:
            tx["parent"] = {"id": parent}
        docs.append(tx)
        docs.extend(spans)
        if failed:
            logs.append({"@timestamp": iso(start + timedelta(milliseconds=dur)), "trace": {"id": tid}, "service": {"name": name, "environment": "prod"},
                         "processor": {"event": "error"}, "parent": {"id": err_parent}, "transaction": {"id": tx_id},
                         "error": {"id": f"{rng.getrandbits(64):016x}", "exception": {"type": svc.error_type, "message": f"{err_msg} (request {seq})"},
                                   "culprit": f"{name} {svc.transaction}"},
                         "message": f"{err_msg} (request {seq})"})
        return dur, failed
