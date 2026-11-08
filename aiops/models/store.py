import copy
import itertools
import threading
from typing import Any


class InMemoryTable:
    """暫代資料表。之後換 SQLAlchemy 時只改 models/ 與各領域 dao.py，Command 不用動。"""

    def __init__(self, name: str):
        self.name = name
        self._lock = threading.Lock()
        self.clear()

    def clear(self) -> None:
        self._rows: dict[int, dict[str, Any]] = {}
        self._seq = itertools.count(1)

    def insert(self, row: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            new = {**copy.deepcopy(row), "id": next(self._seq)}
            self._rows[new["id"]] = new
            return copy.deepcopy(new)

    def get(self, row_id: int) -> dict[str, Any] | None:
        row = self._rows.get(row_id)
        return copy.deepcopy(row) if row else None

    def update(self, row_id: int, **fields: Any) -> dict[str, Any]:
        with self._lock:
            self._rows[row_id].update(copy.deepcopy(fields))
            return copy.deepcopy(self._rows[row_id])

    def list(self, **filters: Any) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(r) for r in self._rows.values() if all(r.get(k) == v for k, v in filters.items())
        ]
