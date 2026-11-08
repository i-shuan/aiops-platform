"""資料源共用的 HTTP 呼叫：把連線失敗、認證失敗、伺服器錯誤轉成 InfrastructureError。

短暫抖動會重試：連線失敗／逾時（httpx.TransportError）與 429、502、503、504，最多重試 len(RETRY_DELAYS) 次，
間隔依序是 RETRY_DELAYS 秒。資料源的節點重啟、選主、負載過高通常是幾秒內的事，不該讓整輪偵測或整份 RCA 因此失敗。
不重試：認證失敗（401／403，重試也一樣）、查詢錯誤（400／404／422，交給呼叫端）、其他 5xx（多半是查詢把資料源打掛，重試只會更糟）。
只用在唯讀查詢（_search、query_range），重送不會有副作用。
"""
import logging
import os
import time
from typing import Any

import httpx

from aiops.engine.errors import InfrastructureError

logger = logging.getLogger("aiops.sources")
RETRY_STATUSES = frozenset({429, 502, 503, 504})
RETRY_DELAYS = (0.5, 1.5)


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise InfrastructureError(f"沒有設定 {name}")
    return value


def request(client: httpx.Client, source: str, method: str, url: str, **kwargs: Any) -> httpx.Response:
    """回傳 2xx 或 400／404（查詢錯誤，交給呼叫端解讀）；其餘一律 InfrastructureError。短暫抖動先重試（見模組說明）。"""
    attempts = len(RETRY_DELAYS) + 1
    for attempt in range(1, attempts + 1):
        try:
            resp = client.request(method, url, **kwargs)
        except httpx.TransportError as e:
            if attempt < attempts:
                logger.warning("%s 連線失敗，第 %d 次重試（%s）", source, attempt, e)
                time.sleep(RETRY_DELAYS[attempt - 1])
                continue
            raise InfrastructureError(f"{source} 連不上（已重試 {attempts - 1} 次）：{e}") from e
        except httpx.HTTPError as e:
            raise InfrastructureError(f"{source} 連不上：{e}") from e
        if resp.status_code in RETRY_STATUSES and attempt < attempts:
            logger.warning("%s 回應 HTTP %d，第 %d 次重試", source, resp.status_code, attempt)
            time.sleep(RETRY_DELAYS[attempt - 1])
            continue
        break
    if resp.status_code in (401, 403):
        raise InfrastructureError(f"{source} 認證失敗（HTTP {resp.status_code}），請檢查帳號或 token")
    if resp.status_code in (400, 404, 422) or resp.is_success:
        return resp
    retried = f"（已重試 {attempts - 1} 次）" if resp.status_code in RETRY_STATUSES else ""
    raise InfrastructureError(f"{source} 回應錯誤（HTTP {resp.status_code}）{retried}：{resp.text[:200]}")


def check(client: httpx.Client, source: str, url: str, **kwargs: Any) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        resp = request(client, source, "GET", url, **kwargs)
    except InfrastructureError as e:
        return {"ok": False, "detail": str(e)}
    elapsed = round((time.perf_counter() - started) * 1000, 1)
    if not resp.is_success:
        return {"ok": False, "detail": f"HTTP {resp.status_code}", "latency_ms": elapsed}
    return {"ok": True, "detail": resp.text[:120].strip(), "latency_ms": elapsed}
