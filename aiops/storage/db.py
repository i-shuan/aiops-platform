"""資料庫連線與 session。SQLite（開發／測試）與 PostgreSQL（正式）共用同一套 ORM。

AIOPS_DATABASE_URL：
- 預設 sqlite:///./aiops-local.db
- 正式例：postgresql+psycopg://user:pass@host:5432/aiops（需另裝 psycopg）

資料庫連不上一律轉成 InfrastructureError，由 interfaces/bootstrap 回 503 並走看守機制。
"""
import logging
import os
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TypeVar

from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import StaticPool

from aiops.engine.errors import InfrastructureError

DEFAULT_URL = "sqlite:///./aiops-local.db"
# 單一交易寫入遇到資料庫短暫斷線時的重試間隔（秒）；跟資料源 sources/_http 一致
RETRY_DELAYS = (0.5, 1.5)
logger = logging.getLogger("aiops.storage")
T = TypeVar("T")

# transaction() 進行中的 session：裡面所有 repository 呼叫都共用它，最後一起 commit
_ambient: ContextVar[Session | None] = ContextVar("aiops_db_ambient_session", default=None)


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
    """會 commit 的 session。只有擁有該資料表的領域 DAO 能用（tests/test_architecture.py 檢查）。
    已經在 transaction() 裡時共用那個 session，不自己 commit：由交易統一 commit 或整筆回復。"""
    outer = _ambient.get()
    if outer is not None:
        yield outer
        return
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
    """唯讀 session：結束一律 rollback，不 commit。純查詢一律用這個。在 transaction() 裡則共用交易的 session（看得到交易內剛寫的）。"""
    outer = _ambient.get()
    if outer is not None:
        yield outer
        return
    session = _new_session()
    try:
        yield session
    except (OperationalError, InterfaceError) as e:
        raise _unavailable(e) from e
    finally:
        _quiet_rollback(session)
        session.close()


@contextmanager
def transaction() -> Iterator[Session]:
    """單一交易：裡面所有 repository／crud 呼叫共用同一個 session，結束一起 commit；任何例外整筆回復，不留半套。不可巢狀。"""
    if _ambient.get() is not None:
        raise RuntimeError("transaction() 不可巢狀")
    session = _new_session()
    token = _ambient.set(session)
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
        _ambient.reset(token)
        session.close()


def run_in_transaction(fn: Callable[[], T], *, retries: int = len(RETRY_DELAYS)) -> T:
    """fn() 在單一交易裡執行。資料庫短暫斷線（OperationalError／InterfaceError）就整筆回復、拿全新的 session 重跑 fn，
    最多重試 retries 次；其他例外不重試。fn 每次都從頭再跑，所以 fn 不能改動它讀的輸入（例如先把要寫的清單 pop 空）。"""
    for attempt in range(retries + 1):
        try:
            with transaction():
                return fn()
        except InfrastructureError as e:
            if not isinstance(e.__cause__, (OperationalError, InterfaceError)) or attempt >= retries:
                raise InfrastructureError(f"{e}（已重試 {attempt} 次）") from e
            logger.warning("資料庫寫入失敗，第 %d 次重試：%s", attempt + 1, e)
            time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)])
    raise AssertionError("unreachable")


def _quiet_rollback(session: Session) -> None:
    try:
        session.rollback()
    except (OperationalError, InterfaceError):
        pass


def create_all() -> None:
    from aiops.storage import models  # noqa: F401  確保所有資料表都註冊到 Base.metadata

    try:
        Base.metadata.create_all(get_engine())
    except (OperationalError, InterfaceError) as e:
        raise _unavailable(e) from e


def drop_all() -> None:
    from aiops.storage import models  # noqa: F401

    Base.metadata.drop_all(get_engine())
