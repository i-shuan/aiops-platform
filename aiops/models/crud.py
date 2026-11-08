"""DAO 共用的存取函式：回傳一律是 dict（Command 不會拿到 ORM 物件）。

寫入類：create / create_many / update / upsert（用 write_session）
讀取類：get / find / find_one / max_of（用 read_session）
*ReadDAO 只能呼叫讀取類（tests/test_architecture.py 檢查）。
"""
from typing import Any

from sqlalchemy import func, select

from aiops.models.core import row_to_dict, utc_naive
from aiops.models.db import read_session, write_session


def _clean(fields: dict[str, Any]) -> dict[str, Any]:
    return {k: utc_naive(v) for k, v in fields.items()}


def _where(model: Any, filters: dict[str, Any]):
    return [getattr(model, k) == utc_naive(v) for k, v in filters.items()]


# ── 寫入 ──


def create(model: Any, **fields: Any) -> dict[str, Any]:
    with write_session() as s:
        row = model(**_clean(fields))
        s.add(row)
        s.flush()
        return row_to_dict(row)


def create_many(model: Any, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with write_session() as s:
        s.add_all([model(**_clean(r)) for r in rows])
    return len(rows)


def update(model: Any, row_id: Any, **fields: Any) -> dict[str, Any]:
    with write_session() as s:
        row = s.get(model, row_id)
        if row is None:
            raise KeyError(f"{model.__tablename__} 沒有 id={row_id}")
        for k, v in _clean(fields).items():
            setattr(row, k, v)
        s.flush()
        return row_to_dict(row)


def upsert(model: Any, match: dict[str, Any], **fields: Any) -> dict[str, Any]:
    """依 match 找一筆；有就更新 fields，沒有就用 match + fields 新增。"""
    with write_session() as s:
        row = s.scalars(select(model).where(*_where(model, match))).first()
        if row is None:
            row = model(**_clean({**match, **fields}))
            s.add(row)
        else:
            for k, v in _clean(fields).items():
                setattr(row, k, v)
        s.flush()
        return row_to_dict(row)


# ── 讀取 ──


def get(model: Any, row_id: Any) -> dict[str, Any] | None:
    with read_session() as s:
        row = s.get(model, row_id)
        return row_to_dict(row) if row is not None else None


def find(model: Any, *, after_id: int | None = None, limit: int | None = None, **filters: Any) -> list[dict[str, Any]]:
    """依 id 由小到大；after_id 給了就只取 id 比它大的（讀游標用）。"""
    stmt = select(model).where(*_where(model, filters))
    if after_id is not None:
        stmt = stmt.where(model.id > after_id)
    stmt = stmt.order_by(model.id)
    if limit is not None:
        stmt = stmt.limit(limit)
    with read_session() as s:
        return [row_to_dict(r) for r in s.scalars(stmt)]


def find_one(model: Any, **filters: Any) -> dict[str, Any] | None:
    rows = find(model, limit=1, **filters)
    return rows[0] if rows else None


def find_in(model: Any, column: str, values: list[Any]) -> list[dict[str, Any]]:
    if not values:
        return []
    stmt = select(model).where(getattr(model, column).in_(values)).order_by(model.id)
    with read_session() as s:
        return [row_to_dict(r) for r in s.scalars(stmt)]


def max_of(model: Any, column: str, **filters: Any) -> Any:
    stmt = select(func.max(getattr(model, column))).where(*_where(model, filters))
    with read_session() as s:
        return s.scalar(stmt)
