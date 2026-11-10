"""世界模擬器：服務呼叫圖 + 每分鐘參數陣列 -> 指標陣列與 APM trace 文件。

所有時變參數（請求率、自身耗時、依賴耗時、失敗率…）都是長度 = 分鐘數的 numpy 陣列，場景用 world.set(...) 在區間內改值。
指標用「解析期望值 + 取樣噪音」向量化算出（幾萬分鐘也很快）；trace 只在需要的時窗用同一組參數逐請求走訪。

訊息佇列（Call(kind="messaging", consumer=...)）：生產者的 send span 很快就回來，訊息進一條先進先出的佇列，
消費者依自己的處理能力（workers／capacity）消化；每分鐘的累計發布、累計消費兩條曲線決定「這則訊息幾點被處理」，
指標（消費者每分鐘處理幾筆、失敗幾筆）與 trace（消費者 transaction 的時間、message.age、parent／span.links）
用同一組曲線，兩邊對得起來。沒有 messaging 呼叫的世界，亂數序列與輸出跟加這段之前完全一樣。
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
    # ── 以下只有 kind="messaging" 用：callee 是 broker 上的資源名（kafka/order-events），consumer 是消費它的服務 ──
    consumer: str = ""             # 消費者服務名（不寫 = 只有 send span，沒有人消費）
    queue: str = ""                # transaction.message.queue.name；不寫用 callee
    workers: int = 0               # >0：處理能力 = workers × 60000 / 消費者每則訊息的期望耗時（下游變慢，能力自然掉）
    capacity: float | np.ndarray | None = None   # 直接給每分鐘能處理幾則；workers 與 capacity 都沒給 = 無上限
    delay_ms: float = 0.0          # 固定延遲（delay queue、排程批次）：發布後至少這麼久才會被消費
    lag_ms: float = 200.0          # 正常投遞延遲，每則訊息乘 uniform(0.5, 1.5)
    poison: float | np.ndarray = 0.0   # 依「發布那一分鐘」：這則訊息會讓消費者處理失敗的機率（payload 壞掉）
    poison_message: str = "invalid message payload (schema mismatch)"
    link_mode: str = "parent"      # parent：消費者沿用同一條 trace、parent 指到 send span；links：新 trace＋span.links；none：都沒有
    emit_age: bool = True          # 消費者 transaction 帶不帶 transaction.message.age.ms

    def is_service(self) -> bool:
        return self.kind == "service"

    def is_messaging(self) -> bool:
        return self.kind == "messaging"


@dataclass
class Queue:
    """一條 (生產者, broker 資源) 佇列在整個世界時間軸上的流體模型結果。cum_* 長度 = 分鐘數 + 1（第 m 分鐘開頭的累計值）。"""
    cum_pub: np.ndarray
    cum_srv: np.ndarray
    served: np.ndarray         # 每分鐘消費幾則
    pub_at: np.ndarray         # 第 m 分鐘消費的訊息（取中間那則）是世界第幾分鐘發的（浮點）

    def lag_minutes(self) -> np.ndarray:
        """每分鐘被消費的訊息已經在佇列裡等了幾分鐘（真值，場景拿來對平台算出來的延遲）。"""
        return np.arange(len(self.served)) + 0.5 - self.pub_at

    def backlog(self) -> np.ndarray:
        """每分鐘結束時還積在佇列裡（含固定延遲中）的訊息數。"""
        return self.cum_pub[1:] - self.cum_srv[1:]


def _inv(cum: np.ndarray, pos: float) -> float:
    """累計曲線的反函數：第 pos 則訊息落在第幾分鐘（浮點）。曲線有平段（那分鐘是 0 則）也給得出確定的答案。"""
    j = int(np.searchsorted(cum, pos, side="right")) - 1
    j = min(max(j, 0), len(cum) - 2)
    step = float(cum[j + 1] - cum[j])
    return j + ((pos - float(cum[j])) / step if step > 0 else 0.0)


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
                if c.is_messaging():
                    if c.link_mode not in ("parent", "links", "none"):
                        raise ValueError(f"link_mode 只能是 parent／links／none：{c.link_mode}")
                    if c.consumer and c.consumer not in self.svcs:
                        raise ValueError(f"messaging 呼叫的 consumer 不在世界裡：{c.consumer}")
                    c.poison = self._a(c.poison)
                    if c.capacity is not None:
                        c.capacity = self._a(c.capacity)
        self.queues: dict[tuple[str, str], Queue] = {}
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
                elif c.is_messaging() and c.consumer:  # 消費者排在生產者前面：算佇列處理能力要先知道它每則訊息花多久
                    visit(c.consumer)
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
        self.queues = {}
        for n in reversed(order):  # caller 在前
            s = self.svcs[n]
            for c in s.calls:
                if c.is_service():
                    rate[c.callee] += rate[n] * (1.0 - s.crash_p) * c.prob
                elif c.is_messaging() and c.consumer:
                    # 送出成功的才進佇列；消費者的請求率 = 佇列每分鐘吐出幾則（簡化：一個消費者服務只吃這一條佇列）
                    q = self._queue(rate[n] * (1.0 - s.crash_p) * c.prob * (1.0 - c.fail_p), c)
                    self.queues[(n, c.callee)] = q
                    rate[c.consumer] += q.served
                    at = np.clip(q.pub_at.astype(int), 0, self.minutes - 1)  # 這分鐘消費的訊息是哪一分鐘發的 -> 當時的毒訊息比例
                    self.p_fail[c.consumer] = 1.0 - (1.0 - self.p_fail[c.consumer]) * (1.0 - c.poison[at])
        self.rate = rate
        self._sample_metrics()
        self._built = True

    def _queue(self, pub: np.ndarray, c: Call) -> Queue:
        """先進先出的流體佇列：到達 = 發布往後推固定延遲；每分鐘最多消化 capacity 則，消化不完的留到下一分鐘。"""
        n = self.minutes
        edges = np.arange(n + 1, dtype=float)
        cum_pub = np.concatenate([[0.0], np.cumsum(pub)])
        arrive = np.diff(np.interp(edges - c.delay_ms / 60000.0, edges, cum_pub, left=0.0))
        if c.workers > 0:
            cap = c.workers * 60000.0 / np.maximum(self.e_dur[c.consumer], 1e-6)
        else:
            cap = c.capacity
        if cap is None:
            served = arrive
        else:
            served, waiting = np.zeros(n), 0.0
            for m in range(n):
                total = waiting + arrive[m]
                served[m] = min(total, float(cap[m]))
                waiting = total - served[m]
        cum_srv = np.concatenate([[0.0], np.cumsum(served)])
        pub_at = np.interp((cum_srv[:-1] + cum_srv[1:]) / 2.0, cum_pub, edges)
        return Queue(cum_pub, cum_srv, served, pub_at)

    def published_at(self, producer: str, callee: str, consumed: datetime) -> datetime:
        """真值：在 consumed 這個時間點被消費的那則訊息，當初是幾點發布的（場景拿來對平台還原的發布時窗）。"""
        self.build()
        q = self.queues[(producer, callee)]
        edges = np.arange(self.minutes + 1, dtype=float)
        pos = float(np.interp((consumed - self.t0).total_seconds() / 60.0, edges, q.cum_srv))
        return self.t0 + timedelta(minutes=_inv(q.cum_pub, pos))

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

    def _deliver(self, producer: str, c: Call, published: datetime, sid: str, tid: str, rng: random.Random,
                 docs: list, logs: list, seq: int) -> None:
        """一則送出成功的訊息：照佇列曲線算出它幾點被消費，在那個時間點走訪消費者（用消費當下那一分鐘的參數）。
        世界結束時還在佇列裡的訊息沒有消費者 transaction。亂數先抽完再判斷要不要丟，三種 link_mode 的世界其他部分才會一樣。"""
        q = self.queues[(producer, c.callee)]
        t_pub = (published - self.t0).total_seconds() / 60.0
        poisoned = rng.random() < float(c.poison[min(max(int(t_pub), 0), self.minutes - 1)])
        jitter = rng.uniform(0.5, 1.5) * c.lag_ms
        own_tid = f"{rng.getrandbits(128):032x}"
        pos = float(np.interp(t_pub, np.arange(self.minutes + 1, dtype=float), q.cum_pub))
        if pos >= float(q.cum_srv[-1]) - 1e-9:
            return
        t_srv = max(_inv(q.cum_srv, pos), t_pub + c.delay_ms / 60000.0)
        at = self.t0 + timedelta(minutes=t_srv, milliseconds=jitter)
        m = self.m(at)
        if m >= self.minutes:
            return
        linked = c.link_mode == "parent"
        msg = {"queue": c.queue or c.callee, "poisoned": poisoned, "poison_message": c.poison_message,
               "age_ms": (at - published).total_seconds() * 1000 if c.emit_age else None,
               "links": [(tid, sid)] if c.link_mode == "links" else []}
        self._walk(c.consumer, m, at, sid if linked else None, tid if linked else own_tid, rng, docs, logs, seq, msg)

    def _walk(self, name: str, m: int, start: datetime, parent: str | None, tid: str, rng: random.Random,
              docs: list, logs: list, seq: int, msg: dict | None = None) -> tuple[float, bool]:
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
                if c.is_messaging() and c.consumer and not sfailed:  # send span 的起點就是發布時間
                    self._deliver(name, c, cursor, sid, tid, rng, docs, logs, seq)
                cursor += timedelta(milliseconds=dur_c)
                slowest = max(slowest, dur_c)
                if sfailed and not c.swallow:
                    failed_call = True
                    err_msg, err_parent = f"{c.callee} call failed ({c.name or c.subtype})", sid
            if svc.parallel:
                cursor = fork + timedelta(milliseconds=slowest)
            dur = (cursor - start).total_seconds() * 1000 + self_total * (1 - PRE_CALL_SHARE)
            own = rng.random() < float(svc.own_fail_p[m])
            poisoned = bool(msg and msg["poisoned"])  # 毒訊息：消費者自己處理失敗（不是下游的錯）
            failed = own or poisoned or failed_call
            if own or poisoned:
                err_msg, err_parent = (svc.error_message if own else msg["poison_message"]), tx_id
            status = 500 if failed else 200
        if msg is None:
            tx = {**self._common(svc, start, tid), "processor": {"event": "transaction"}, "timestamp": {"us": int(start.timestamp() * 1e6)},
                  "transaction": {"id": tx_id, "name": svc.transaction, "type": "request", "result": f"HTTP {status // 100}xx",
                                  "duration": {"us": max(int(dur * 1000), 1)}, "span_count": {"started": len(spans)}},
                  "http": {"response": {"status_code": status}}, "event": {"outcome": "failure" if failed else "success"}}
        else:  # 訊息消費者（Elastic APM）：transaction.type = messaging、沒有 http，佇列名與訊息年齡在 transaction.message
            message: dict = {"queue": {"name": msg["queue"]}}
            if msg["age_ms"] is not None:
                message["age"] = {"ms": int(msg["age_ms"])}
            tx = {**self._common(svc, start, tid), "processor": {"event": "transaction"}, "timestamp": {"us": int(start.timestamp() * 1e6)},
                  "transaction": {"id": tx_id, "name": svc.transaction, "type": "messaging", "result": "failure" if failed else "success",
                                  "duration": {"us": max(int(dur * 1000), 1)}, "span_count": {"started": len(spans)}, "message": message},
                  "event": {"outcome": "failure" if failed else "success"}}
            if msg["links"]:
                tx["span"] = {"links": [{"trace": {"id": t}, "span": {"id": s}} for t, s in msg["links"]]}
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
