"""資料健康度查詢：每條資料流（管線 × 實體 × 查詢）的狀態區間、管線每一輪的執行結果、第一次收到資料的時間。
只查 storage，不做任何判斷。

狀態的意思見 aiops/engine/health.py。區間 [started_at, last_checked_at] 內每一輪都是同一個狀態；
沒有被任何區間蓋到的時段 = 平台那段時間沒有檢查到（管線沒跑、或紀錄寫不進資料庫），不是「正常」。
"""
from datetime import datetime

from fastapi import APIRouter, Query

from aiops.storage.repository import PipelineRuns, StreamFirstSeen, StreamHealth

router = APIRouter(prefix="/api/v1/data-health", tags=["data-health"])


@router.get("/streams")
def list_stream_health(pipeline: str | None = None, entity: str | None = None, status: str | None = None,
                       current: bool | None = None, since: datetime | None = None, until: datetime | None = None,
                       limit: int = Query(500, ge=1, le=5000)):
    """資料流的狀態區間，由新到舊。current=true 只看每條資料流現在的狀態；since／until 取跟這段時間有重疊的區間
    （查「那個時間點有沒有資料」：since = until = 那個時間）。"""
    return {"result": StreamHealth.list(pipeline, entity, status, current, since, until, limit)}


@router.get("/runs")
def list_pipeline_runs(pipeline: str | None = None, status: str | None = None, failure_type: str | None = None,
                       since: datetime | None = None, until: datetime | None = None,
                       limit: int = Query(500, ge=1, le=5000)):
    """管線每一輪的執行結果（一輪一列，由新到舊）：status = ok／degraded／failed；failed 的 failure_type =
    infrastructure_error／workflow_error／error。那個時間沒有列 = 那一輪沒有跑（或紀錄寫不進資料庫）。"""
    return {"result": PipelineRuns.list(pipeline, status, failure_type, since, until, limit)}


@router.get("/first-seen")
def list_first_seen(pipeline: str | None = None, entity: str | None = None, limit: int = Query(500, ge=1, le=5000)):
    """每條資料流第一次收到資料的時間；沒有列在這裡的資料流 = 平台還沒收過它的資料。"""
    return {"result": StreamFirstSeen.list(pipeline, entity, limit)}
