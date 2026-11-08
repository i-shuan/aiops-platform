"""DAO 共用的存取函式：回傳一律是 dict（Command 不會拿到 ORM 物件）。

寫入類：create / create_many / update / upsert（用 write_session）
讀取類：get / find / find_one / max_of（用 read_session）
節點之間不透過資料庫傳資料（tests/test_architecture.py 檢查）。
"""
from typing import Any

from sqlalchemy import func, select
from sqlalchemy import update as sql_update
from sqlalchemy.exc import IntegrityError

from aiops.storage.models import row_to_dict, utc_naive
from aiops.storage.db import read_session, write_session


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


def insert_if_absent(model: Any, **fields: Any) -> dict[str, Any] | None:
    """新增一筆；撞到唯一鍵（別人已經先寫了）回傳 None，不拋錯也不覆蓋。"""
    try:
        return create(model, **fields)
    except IntegrityError:
        return None


def update_if_version(model: Any, match: dict[str, Any], expected_version: int, **fields: Any) -> dict[str, Any] | None:
    """樂觀鎖更新：只有該列的 version 仍等於 expected_version 才會更新並把 version 加一。
    沒有符合的列（別人已經改過、或列不存在）回傳 None，不覆蓋。"""
    with write_session() as s:
        result = s.execute(
            sql_update(model).where(*_where(model, match), model.version == expected_version)
            .values(**_clean(fields), version=expected_version + 1))
        if result.rowcount != 1:
            return None
        return row_to_dict(s.scalars(select(model).where(*_where(model, match))).one())


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


def find_latest(model: Any, limit: int, **filters: Any) -> list[dict[str, Any]]:
    """id 最大的 limit 筆（由新到舊）。"""
    stmt = select(model).where(*_where(model, filters)).order_by(model.id.desc()).limit(limit)
    with read_session() as s:
        return [row_to_dict(r) for r in s.scalars(stmt)]


def find_matching(model: Any, equals: dict[str, Any], any_of: dict[str, list[Any]]) -> list[dict[str, Any]]:
    """一次查詢：equals 的欄位都相等、且 any_of 的每個欄位都落在給定的值之中（WHERE col IN (...)）。"""
    stmt = select(model).where(*_where(model, equals),
                               *(getattr(model, col).in_(values) for col, values in any_of.items()))
    with read_session() as s:
        return [row_to_dict(r) for r in s.scalars(stmt.order_by(model.id))]


def find_in(model: Any, column: str, values: list[Any]) -> list[dict[str, Any]]:
    if not values:
        return []
    stmt = select(model).where(getattr(model, column).in_(values)).order_by(model.id)
    with read_session() as s:
        return [row_to_dict(r) for r in s.scalars(stmt)]


def find_between(model: Any, column: str, start: Any, end: Any, *, in_column: str | None = None,
                 in_values: list[Any] | None = None) -> list[dict[str, Any]]:
    """column 落在 [start, end] 的列（依 column 排序）；可再限定 in_column 屬於 in_values。"""
    col = getattr(model, column)
    stmt = select(model).where(col >= utc_naive(start), col <= utc_naive(end))
    if in_column is not None:
        if not in_values:
            return []
        stmt = stmt.where(getattr(model, in_column).in_(in_values))
    with read_session() as s:
        return [row_to_dict(r) for r in s.scalars(stmt.order_by(col))]


def max_of(model: Any, column: str, **filters: Any) -> Any:
    stmt = select(func.max(getattr(model, column))).where(*_where(model, filters))
    with read_session() as s:
        return s.scalar(stmt)
