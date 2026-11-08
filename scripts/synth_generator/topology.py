"""第 1 層：服務拓撲與依賴（呼叫 DAG）。

每個服務有自己的處理時間（self_ms）與對外呼叫清單 Call(callee, 呼叫機率, 基礎耗時 ms)。
callee 有兩種：
- 另一個服務（有自己的 transaction）：base_ms 是網路來回的額外時間，這次呼叫的總耗時 = base_ms + 下游自己的整段耗時。
- 外部依賴（redis、資料庫、第三方 API，沒有自己的 transaction）：base_ms 就是那次呼叫本身的耗時。
入口服務（entry）的每分鐘請求數由 waveforms 的日夜曲線決定；下游的請求數由呼叫機率一路乘下去，不另外造。
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Call:
    callee: str
    prob: float
    base_ms: float
    span_type: str = "external"   # Elastic APM 的 span.type：external／db／cache
    subtype: str = "http"         # span.subtype：http／postgresql／redis
    name: str = ""                # span.name；空字串就自動用 "<METHOD> <callee>"


@dataclass(frozen=True)
class Service:
    name: str
    self_ms: float                 # 自己的處理時間（不含對外呼叫）的平均
    transaction: str               # transaction.name（APM 的 API 名稱）
    agent: str = "java"
    version: str = "1.0.0"
    dept: str = "D3120"
    group: str = "shop"
    calls: tuple[Call, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Topology:
    services: dict[str, Service]
    entry: str

    def __post_init__(self):
        if self.entry not in self.services:
            raise ValueError(f"入口服務 {self.entry} 不在服務清單裡")
        for s in self.services.values():
            for c in s.calls:
                if not 0 < c.prob <= 1:
                    raise ValueError(f"{s.name} -> {c.callee} 的呼叫機率必須在 (0, 1]：{c.prob}")
                if c.callee == s.name:
                    raise ValueError(f"{s.name} 不能呼叫自己")
        self.order()  # 有環就在這裡丟錯

    def is_service(self, name: str) -> bool:
        return name in self.services

    def service_calls(self, name: str) -> list[Call]:
        return [c for c in self.services[name].calls if self.is_service(c.callee)]

    def order(self) -> list[str]:
        """由下游到上游（callee 一定排在 caller 前面）。有環就丟 ValueError。"""
        done: list[str] = []
        state: dict[str, int] = {}

        def visit(n: str, path: tuple[str, ...]):
            if state.get(n) == 2:
                return
            if state.get(n) == 1:
                raise ValueError(f"呼叫關係有環：{' -> '.join(path + (n,))}")
            state[n] = 1
            for c in self.service_calls(n):
                visit(c.callee, path + (n,))
            state[n] = 2
            done.append(n)

        for name in sorted(self.services):
            visit(name, ())
        return done

    def edges(self) -> set[tuple[str, str]]:
        """服務對服務的呼叫邊 (caller, callee)：trace 還原出來的拓撲應該剛好是這些（外部依賴不算服務）。"""
        return {(s.name, c.callee) for s in self.services.values() for c in s.calls if self.is_service(c.callee)}

    def downstream(self, name: str) -> set[str]:
        """name 直接或間接呼叫的所有服務。"""
        out: set[str] = set()
        stack = [name]
        while stack:
            for c in self.service_calls(stack.pop()):
                if c.callee not in out:
                    out.add(c.callee)
                    stack.append(c.callee)
        return out

    def upstream(self, name: str) -> set[str]:
        return {s for s in self.services if name in self.downstream(s)}


def _http(callee: str, prob: float, base_ms: float, path: str) -> Call:
    return Call(callee, prob, base_ms, "external", "http", f"{path.split(' ')[0]} {callee}{path.split(' ', 1)[1]}")


# 劇本預設的拓撲：api-gw → [cart-api, auth-svc]、cart-api → [ord-svc 0.8, redis-cache]、ord-svc → [bill-svc, inventory-db]、
# bill-svc → [billing-db, tax-gw]。redis-cache、inventory-db、billing-db、tax-gw 是外部依賴（只有 client span）。
DEFAULT_TOPOLOGY = Topology(
    entry="api-gw",
    services={
        "api-gw": Service("api-gw", 6.0, "GET /checkout", agent="nodejs", version="3.2.0", calls=(
            _http("auth-svc", 1.0, 1.5, "GET /verify"),
            _http("cart-api", 1.0, 1.5, "GET /cart/{id}"),
        )),
        "auth-svc": Service("auth-svc", 9.0, "GET /verify", agent="go", version="2.0.4", calls=(
            Call("redis-cache", 1.0, 1.2, "cache", "redis", "GET session"),
        )),
        "cart-api": Service("cart-api", 35.0, "GET /cart/{id}", version="1.8.1", calls=(
            Call("redis-cache", 1.0, 1.5, "cache", "redis", "HGETALL cart"),
            _http("ord-svc", 0.8, 2.0, "POST /orders"),
        )),
        "ord-svc": Service("ord-svc", 45.0, "POST /orders", version="4.1.0", calls=(
            Call("inventory-db", 1.0, 12.0, "db", "postgresql", "SELECT inventory"),
            _http("bill-svc", 1.0, 2.0, "POST /bills"),
        )),
        "bill-svc": Service("bill-svc", 30.0, "POST /bills", agent="python", version="0.9.7", calls=(
            Call("billing-db", 1.0, 10.0, "db", "postgresql", "INSERT bill"),
            _http("tax-gw", 1.0, 25.0, "GET /tax"),
        )),
    },
)
