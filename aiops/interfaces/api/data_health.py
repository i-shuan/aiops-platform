"""資料健康度查詢：每條資料流（管線 × 實體 × 查詢）與每條管線的狀態區間。只查 storage，不做任何判斷。

狀態的意思見 aiops/engine/health.py。區間 [started_at, last_checked_at] 內每一輪都是同一個狀態；
沒有被任何區間蓋到的時段 = 平台那段時間沒有檢查到（管線沒跑、或紀錄寫不進資料庫），不是「正常」。
"""
from datetime import datetime

from fastapi import APIRouter, Query

from aiops.storage.repository import RunHealth, StreamHealth

router = APIRouter(prefix="/api/v1/data-health", tags=["data-health"])


@router.get("/streams")
def list_stream_health(pipeline: str | None = None, entity: str | None = None, status: str | None = None,
                       current: bool | None = None, since: datetime | None = None, until: datetime | None = None,
                       limit: int = Query(500, ge=1, le=5000)):
    """資料流的狀態區間，由新到舊。current=true 只看每條資料流現在的狀態；since／until 取跟這段時間有重疊的區間
    （查「那個時間點有沒有資料」：since = until = 那個時間）。"""
    return {"result": StreamHealth.list(pipeline, entity, status, current, since, until, limit)}


@router.get("/runs")
def list_run_health(pipeline: str | None = None, status: str | None = None, current: bool | None = None,
                    since: datetime | None = None, until: datetime | None = None,
                    limit: int = Query(500, ge=1, le=5000)):
    """管線執行結果的狀態區間：ok／degraded／infrastructure_error／workflow_error／error。"""
    return {"result": RunHealth.list(pipeline, status, current, since, until, limit)}
