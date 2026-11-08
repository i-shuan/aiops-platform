"""資料庫連線與 session。SQLite（開發／測試）與 PostgreSQL（正式）共用同一套 ORM。

AIOPS_DATABASE_URL：
- 預設 sqlite:///./aiops-local.db
- 正式例：postgresql+psycopg://user:pass@host:5432/aiops（需另裝 psycopg）

資料庫連不上一律轉成 InfrastructureError，由 initialization 回 503 並走看守機制。
"""
import os
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import StaticPool

from aiops.infrastructure.errors import InfrastructureError

DEFAULT_URL = "sqlite:///./aiops-local.db"


class Base(DeclarativeBase):
    pass


_url: str | None = None
_engine: Engine | None = None
_factory: sessionmaker[Session] | None = None


def configure(url: str | None = None) -> None:
    """換資料庫（測試、或重新讀環境變數）。舊連線會被關掉，下次使用時重建。"""
    global _url, _engine, _factory
    if _engine is not None:
        _engine.dispose()
    _url, _engine, _factory = url, None, None


def database_url() -> str:
    return _url or os.environ.get("AIOPS_DATABASE_URL", DEFAULT_URL)


def get_engine() -> Engine:
    global _engine, _factory
    if _engine is None:
        url = database_url()
        kwargs: dict = {}
        if url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}
            if url in ("sqlite://", "sqlite:///:memory:"):
                kwargs["poolclass"] = StaticPool  # 記憶體 DB 要共用同一條連線，不然每次都是空的
        _engine = create_engine(url, **kwargs)
        _factory = sessionmaker(bind=_engine, expire_on_commit=False)
    return _engine


def _new_session() -> Session:
    get_engine()
    assert _factory is not None
    return _factory()


def _unavailable(e: Exception) -> InfrastructureError:
    return InfrastructureError(f"資料庫無法使用：{getattr(e, 'orig', e)}")


@contextmanager
def write_session() -> Iterator[Session]:
    """會 commit 的 session。只有擁有該資料表的領域 DAO 能用（tests/test_architecture.py 檢查）。"""
    session = _new_session()
    try:
        yield session
        session.commit()
    except (OperationalError, InterfaceError) as e:
        _quiet_rollback(session)
        raise _unavailable(e) from e
    except Exception:
        _quiet_rollback(session)
        raise
    finally:
        session.close()


@contextmanager
def read_session() -> Iterator[Session]:
    """唯讀 session：結束一律 rollback，不 commit。*ReadDAO 只能用這個。"""
    session = _new_session()
    try:
        yield session
    except (OperationalError, InterfaceError) as e:
        raise _unavailable(e) from e
    finally:
        _quiet_rollback(session)
        session.close()


def _quiet_rollback(session: Session) -> None:
    try:
        session.rollback()
    except (OperationalError, InterfaceError):
        pass


def create_all() -> None:
    from aiops.models import core  # noqa: F401  確保所有資料表都註冊到 Base.metadata

    try:
        Base.metadata.create_all(get_engine())
    except (OperationalError, InterfaceError) as e:
        raise _unavailable(e) from e


def drop_all() -> None:
    from aiops.models import core  # noqa: F401

    Base.metadata.drop_all(get_engine())
