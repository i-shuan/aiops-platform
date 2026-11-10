"""同一把鑰匙的工作同時只跑一份：第一個到的請求真的去做，做完之前到的請求等它、拿同一份結果（或同一個例外）。

用途：RCA 報告。一份報告要撈 trace、撈日誌、可能還要呼叫 LLM；告警一響，值班的人、Grafana 面板、自動化腳本
同時對同一個事件群按「產生報告」，沒有這層就是 N 次 ES 查詢加 N 次 LLM 呼叫，算出 N 份一樣的東西。

範圍與限制（寫清楚，不要誤用）：
- 只合併「正在跑」的那一段：leader 一結束就把鑰匙拿掉，下一個請求會重新算。這不是快取，沒有過期與失效問題。
- 只在同一個行程內有效（threading）。API 開多個副本時，每個副本各自最多跑一份；要跨副本得靠資料庫（目前沒做）。
- 狀態掛在實例上，實例由 app 建立時產生（interfaces/app.create_app），不是模組或類別層級的可變狀態。
"""
from __future__ import annotations

import threading
from collections.abc import Callable, Hashable
from typing import Any


class _Flight:
    __slots__ = ("done", "result", "error", "waiters")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.waiters = 0
        self.result: Any = None
        self.error: BaseException | None = None


class SingleFlight:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._flights: dict[Hashable, _Flight] = {}

    def do(self, key: Hashable, fn: Callable[[], Any]) -> Any:
        """key 相同且已經有人在跑：等它，回傳同一份結果；它失敗就丟同一個例外（不各自重跑一次去撞同一面牆）。"""
        with self._lock:
            flight = self._flights.get(key)
            leader = flight is None
            if flight is None:
                flight = self._flights[key] = _Flight()
            else:
                flight.waiters += 1
        if not leader:
            flight.done.wait()
            if flight.error is not None:
                raise flight.error
            return flight.result
        try:
            flight.result = fn()
            return flight.result
        except BaseException as exc:
            flight.error = exc
            raise
        finally:
            with self._lock:  # 先拿掉鑰匙再放行等待者：之後才到的請求會重新算，不會拿到舊結果
                del self._flights[key]
            flight.done.set()

    def in_flight(self) -> int:
        with self._lock:
            return len(self._flights)

    def waiting(self) -> int:
        """正在等別人結果的請求數（觀測用）。"""
        with self._lock:
            return sum(f.waiters for f in self._flights.values())
