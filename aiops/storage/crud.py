"""DAO 共用的存取函式：回傳一律是 dict（Command 不會拿到 ORM 物件）。

寫入類：create / create_many / create_many_ignoring_duplicates / update / upsert（用 write_session）
讀取類：get / find / find_one / max_of（用 read_session）
節點之間不透過資料庫傳資料（tests/test_architecture.py 檢查）。
"""
import json
from typing import Any

from sqlalchemy import JSON, cast, func, literal, select, text
from sqlalchemy import delete as sql_delete
from sqlalchemy import update as sql_update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
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


def create_many_ignoring_duplicates(model: Any, rows: list[dict[str, Any]]) -> int:
    """整批新增，撞到唯一鍵的列跳過（INSERT ... ON CONFLICT DO NOTHING）：同一批寫兩次，第二次什麼都不會多。
    不指定衝突的欄位：資料表還沒有唯一鍵（舊資料庫還沒跑 migration）時就是一般的 INSERT，不會因為找不到對應的鍵而失敗。
    不拋 IntegrityError，所以可以放在 run_in_transaction 的交易裡（PostgreSQL 交易中一旦出錯，後面的語句全部作廢）。
    每一列的鍵必須相同（executemany）。回傳這次要求寫入的筆數，不是實際新增的筆數（executemany 的 rowcount 不可靠）。"""
    if not rows:
        return 0
    with write_session() as s:
        dialect = s.get_bind().dialect.name
        if dialect in ("postgresql", "sqlite"):
            insert = (pg_insert if dialect == "postgresql" else sqlite_insert)(model)
            s.execute(insert.on_conflict_do_nothing(), [_clean(r) for r in rows])
        else:  # 其他資料庫沒有共通語法：退回一般新增（撞鍵會拋錯）
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
    """依 match（必須是這張表的唯一鍵）新增或更新 fields。

    PostgreSQL／SQLite 走 INSERT ... ON CONFLICT DO UPDATE，由資料庫一次完成：多個 worker 同時「第一次」寫同一筆
    不會有人撞唯一鍵（破壞性驗收場景 10：以前先查再寫，10 個 worker 有 4～6 個拿到 IntegrityError）。
    其他方言退回先查再寫。回傳寫完的那一列。"""
    values = _clean({**match, **fields})
    changes = _clean(fields) or {k: values[k] for k in match}  # 沒有要改的欄位時 DO UPDATE 仍要有 set：寫回 match 本身（等於不動）
    # 只給部分欄位（例如只改 verdict）就不是「建立」：ON CONFLICT 的 INSERT 會先撞 NOT NULL；這種呼叫走純 UPDATE，列不存在就報錯
    required = [c.name for c in model.__table__.columns
                if not c.nullable and not c.primary_key and c.default is None and c.server_default is None]
    missing = [c for c in required if c not in values]
    with write_session() as s:
        dialect = s.get_bind().dialect.name
        if missing:
            result = s.execute(sql_update(model).where(*_where(model, match)).values(**changes))
            if result.rowcount == 0:
                raise KeyError(f"{model.__tablename__} 沒有 {match}，而且缺少建立它所需的欄位 {missing}")
            s.expire_all()
            row = s.scalars(select(model).where(*_where(model, match))).first()
        elif dialect in ("postgresql", "sqlite"):
            insert = (pg_insert if dialect == "postgresql" else sqlite_insert)(model).values(**values)
            s.execute(insert.on_conflict_do_update(index_elements=list(match), set_=changes))
            row = s.scalars(select(model).where(*_where(model, match))).first()
        else:
            row = s.scalars(select(model).where(*_where(model, match))).first()
            if row is None:
                row = model(**values)
                s.add(row)
            else:
                for k, v in changes.items():
                    setattr(row, k, v)
        s.flush()
        return row_to_dict(row)


def merge_json(model: Any, match: dict[str, Any], column: str, patch: dict[str, Any], **fields: Any) -> dict[str, Any]:
    """原子追加 JSON 欄位的頂層鍵（同鍵以 patch 為準），由資料庫在 UPDATE 裡合併，不經 Python 讀改寫：
    多個 worker 同時追加不會互相覆蓋（破壞性驗收場景 10：讀改寫 50 次只留 5～7 個鍵）。

    PostgreSQL：context || patch（欄位還是 json 型別的舊資料庫先轉 jsonb 再轉回去）。
    SQLite：json_patch（RFC 7396；巢狀物件會往下合併，比 PostgreSQL 的頂層合併多合一層——呼叫端只能靠頂層鍵區分，
    不要依賴巢狀合併）。其他方言退回讀改寫（不保證併發安全）。找不到那一列丟 KeyError。"""
    payload = json.dumps(patch, ensure_ascii=False)
    with write_session() as s:
        dialect = s.get_bind().dialect.name
        col = getattr(model, column)
        if dialect == "postgresql":
            kind = s.execute(text("SELECT data_type FROM information_schema.columns WHERE table_name = :t AND column_name = :c"),
                             {"t": model.__tablename__, "c": column}).scalar()
            expr = cast(col, JSONB).op("||")(cast(literal(payload), JSONB))
            new_value: Any = cast(expr, JSON) if kind == "json" else expr
        elif dialect == "sqlite":
            new_value = func.json_patch(col, payload)
        else:
            row = s.scalars(select(model).where(*_where(model, match))).first()
            if row is None:
                raise KeyError(f"{model.__tablename__} 沒有 {match}")
            new_value = {**(getattr(row, column) or {}), **patch}
        result = s.execute(sql_update(model).where(*_where(model, match)).values({column: new_value, **_clean(fields)}))
        if result.rowcount == 0:
            raise KeyError(f"{model.__tablename__} 沒有 {match}")
        s.flush()
        s.expire_all()  # 欄位是資料庫算出來的，要重新讀
        row = s.scalars(select(model).where(*_where(model, match))).first()
        return row_to_dict(row)


def increment(model: Any, match: dict[str, Any], column: str, amount: int, **fields: Any) -> None:
    """原子累加：列不存在就以 amount 建立，存在就 column = column + amount（同時寫入 fields）。
    PostgreSQL／SQLite 用 ON CONFLICT DO UPDATE 由資料庫算，兩個 worker 同時累加同一列不會互相蓋掉（讀改寫會少算一次）。
    match 必須是唯一鍵。其他方言退回讀改寫（不保證併發安全）。"""
    values = _clean({**match, **fields, column: amount})
    with write_session() as s:
        dialect = s.get_bind().dialect.name
        col = getattr(model, column)
        if dialect in ("postgresql", "sqlite"):
            insert = (pg_insert if dialect == "postgresql" else sqlite_insert)(model).values(**values)
            s.execute(insert.on_conflict_do_update(
                index_elements=list(match), set_={column: col + insert.excluded[column], **_clean(fields)}))
            return
        row = s.scalars(select(model).where(*_where(model, match))).first()
        if row is None:
            s.add(model(**values))
            return
        setattr(row, column, (getattr(row, column) or 0) + amount)
        for k, v in _clean(fields).items():
            setattr(row, k, v)


def delete_before(model: Any, column: str, before: Any, **filters: Any) -> int:
    """刪掉 column < before（且符合 filters 各欄相等）的列，回傳刪了幾筆。column 是 NULL 的列不會被刪（SQL 的 NULL < x 不成立）。"""
    with write_session() as s:
        result = s.execute(sql_delete(model).where(getattr(model, column) < utc_naive(before), *_where(model, filters)))
        return int(result.rowcount or 0)


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


def find_latest(model: Any, limit: int, *, not_equal: dict[str, Any] | None = None,
                **filters: Any) -> list[dict[str, Any]]:
    """id 最大的 limit 筆（由新到舊）。not_equal：這些欄位不等於給定值（例如 resolution != ''）。"""
    stmt = select(model).where(*_where(model, filters),
                               *(getattr(model, k) != v for k, v in (not_equal or {}).items()))
    stmt = stmt.order_by(model.id.desc()).limit(limit)
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


# ── 狀態區間（資料健康度）──


def record_interval(model: Any, key: dict[str, Any], status: str, at: Any, counter: str,
                    **fields: Any) -> dict[str, Any] | None:
    """把「key 這條線在 at 這個時間是 status」記成狀態區間（model 要有 status、started_at、last_checked_at、is_current 與 counter 欄）。
    最新的區間（is_current）狀態相同：last_checked_at 推到 at、counter 加一、fields 蓋成這一次的；
    狀態不同（或還沒有任何區間）：舊的 is_current 關掉、開一列新的。at 比最新區間的 last_checked_at 還早（回放舊時間）就不寫，回 None。
    讀了再寫：同一條 key 同時只能有一個寫入者（同一條管線同時只跑一輪）；要放在 run_in_transaction 裡。"""
    at = utc_naive(at)
    with write_session() as s:
        open_rows = list(s.scalars(select(model).where(*_where(model, key), model.is_current.is_(True))
                                   .order_by(model.id.desc())))
        current = open_rows[0] if open_rows else None
        if current is not None and at < current.last_checked_at:
            return None
        for stale in open_rows[1:]:  # 兩輪重疊執行時可能各開了一列：只留最新的，其餘關掉，不然舊的會永遠被當成「目前狀態」
            stale.is_current = False
        if current is not None and current.status == status:
            current.last_checked_at = at
            setattr(current, counter, getattr(current, counter) + 1)
            for k, v in _clean(fields).items():
                setattr(current, k, v)
            s.flush()
            return row_to_dict(current)
        if current is not None:
            current.is_current = False
        row = model(**_clean(key), status=status, started_at=at, last_checked_at=at, is_current=True,
                    **{counter: 1}, **_clean(fields))
        s.add(row)
        s.flush()
        return row_to_dict(row)


def find_intervals(model: Any, *, since: Any = None, until: Any = None, limit: int = 500,
                   **filters: Any) -> list[dict[str, Any]]:
    """跟 [since, until] 有重疊的狀態區間（started_at <= until 且 last_checked_at >= since），由新到舊最多 limit 筆。
    filters 裡值是 None 的欄位不篩（API 的選填參數原樣傳進來）。"""
    stmt = select(model).where(*_where(model, {k: v for k, v in filters.items() if v is not None}))
    if since is not None:
        stmt = stmt.where(model.last_checked_at >= utc_naive(since))
    if until is not None:
        stmt = stmt.where(model.started_at <= utc_naive(until))
    with read_session() as s:
        return [row_to_dict(r) for r in s.scalars(stmt.order_by(model.started_at.desc(), model.id.desc()).limit(limit))]


def find_recent(model: Any, column: str, *, since: Any = None, until: Any = None, limit: int = 500,
                **filters: Any) -> list[dict[str, Any]]:
    """column 落在 [since, until] 的列，由新到舊最多 limit 筆。filters 裡值是 None 的欄位不篩。"""
    col = getattr(model, column)
    stmt = select(model).where(*_where(model, {k: v for k, v in filters.items() if v is not None}))
    if since is not None:
        stmt = stmt.where(col >= utc_naive(since))
    if until is not None:
        stmt = stmt.where(col <= utc_naive(until))
    with read_session() as s:
        return [row_to_dict(r) for r in s.scalars(stmt.order_by(col.desc(), model.id.desc()).limit(limit))]
