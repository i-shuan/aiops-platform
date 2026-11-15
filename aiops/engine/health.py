"""資料健康度：這一輪每條資料流（管線 × 實體 × 查詢）與整輪管線的狀態怎麼判（純函式，不碰資料庫）。

引擎（engine/pipeline.py）在每條序列跑完、以及整輪結束或失敗時呼叫這裡，把結果放在這一輪的 PipelineContext
（ctx.stream_checks、ctx.run_check）；寫進資料庫是 storage 的事（StorageHooks.record_health，存成「狀態區間」）。
目的是事後查得到「那個時間點資料有沒有進來、是誰的問題」，不是在引擎裡替資料源補洞（CLAUDE.md §1.1 第 4 點）。

資料流狀態（STREAM_STATUSES）——只看「資料有沒有抓回來」，跟後面的偵測節點有沒有出錯無關：
  ok            這一輪抓回了資料（之後偵測器自己出錯也算 ok：資料是有進來的）
  no_data       查詢合法但沒有資料（資料源丟 NoDataError、或回 200 但零筆），而且這條資料流以前有過資料 = 斷流
  never_seen    同上，但這條資料流從來沒有過資料（沒有過 ok 的紀錄、沒有基準線、沒有存過異常點）= 多半是還沒埋點或查詢對不到東西
  config_error  設定或查詢錯誤：資料源說查詢不合法、欄位不存在、回應格式不對（BadRequest／InvalidQueryError）
  unreachable   抓這條資料流時資料源連不上（InfrastructureError；整輪也會失敗）
  error         抓到資料之前出了其他錯（查不到東西的 NotFound、資料源或抓取程式的非預期例外）

管線狀態（RUN_STATUSES）：
  ok                    整輪成功、沒有任何序列出錯
  degraded              整輪完成，但有序列（或旁路節點）記了錯誤
  infrastructure_error  整輪失敗：資料庫、資料源、Alertmanager 連不上，或資料斷流比例過高（心跳不續命）
  workflow_error        管線定義載入失敗（YAML 寫錯、算子參數不合法）：這一輪根本沒跑
  error                 整輪因為其他例外失敗
"""
from __future__ import annotations

from dataclasses import dataclass

from aiops.engine.errors import BadRequest, InfrastructureError, NoDataError, WorkflowError

STREAM_STATUSES: tuple[str, ...] = ("ok", "no_data", "never_seen", "config_error", "unreachable", "error")
RUN_STATUSES: tuple[str, ...] = ("ok", "degraded", "infrastructure_error", "workflow_error", "error")
MESSAGE_CHARS = 1000  # 訊息欄位長度（storage 的欄位同寬）；資料源的錯誤訊息可能帶一大段回應內容


@dataclass(frozen=True)
class StreamCheck:
    """一條資料流這一輪的檢查結果。"""

    entity: str
    query: str
    source_type: str
    status: str
    message: str = ""


@dataclass(frozen=True)
class RunCheck:
    """一條管線這一輪的執行結果。targets／failed 是這一輪的序列數與出錯的序列數（載入失敗時都是 0）。"""

    status: str
    message: str = ""
    targets: int = 0
    failed: int = 0


def _clip(text: str) -> str:
    return text if len(text) <= MESSAGE_CHARS else text[: MESSAGE_CHARS - 1] + "…"


def stream_status(*, fetched: bool, empty: bool, error: BaseException | None, seen_before: bool) -> tuple[str, str] | None:
    """這條序列這一輪的資料流狀態 (狀態, 訊息)；None = 這一輪沒有抓資料也沒有出錯（管線沒有抓取節點），不記。
    fetched：抓取節點有把資料放進 ctx（raw_df 不是 None）；empty：抓回來是空的；error：這條序列丟出的例外；
    seen_before：這條資料流以前有過資料。"""
    if fetched and not empty:
        return "ok", ""
    missing = "never_seen" if not seen_before else "no_data"
    if fetched:
        return missing, "資料源回應成功，但這段時間零筆資料"
    if error is None:
        return None
    message = _clip(str(error) or type(error).__name__)
    if isinstance(error, NoDataError):
        return missing, message
    if isinstance(error, InfrastructureError):
        return "unreachable", message
    if isinstance(error, BadRequest):
        return "config_error", message
    return "error", _clip(f"{type(error).__name__}: {error}")


def run_status(error: BaseException | None, *, targets: int, failed_entities: list[str]) -> RunCheck:
    """整輪的執行結果。error：讓這一輪失敗的例外（None = 跑完了）；failed_entities：這一輪記了錯誤的實體（可重複）。"""
    failed = len(set(failed_entities))
    if error is None:
        if not failed_entities:
            return RunCheck("ok", "", targets, 0)
        shown = "、".join(sorted(set(failed_entities))[:10]) + ("…" if failed > 10 else "")
        return RunCheck("degraded", _clip(f"{failed} 個實體或節點有錯誤：{shown}"), targets, failed)
    message = _clip(str(error) or type(error).__name__)
    if isinstance(error, InfrastructureError):
        return RunCheck("infrastructure_error", message, targets, failed)
    if isinstance(error, WorkflowError):
        return RunCheck("workflow_error", message, targets, failed)
    return RunCheck("error", _clip(f"{type(error).__name__}: {error}"), targets, failed)
