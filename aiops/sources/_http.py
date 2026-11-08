"""資料源共用的 HTTP 呼叫：把連線失敗、認證失敗、伺服器錯誤轉成 InfrastructureError。"""
import os
import time
from typing import Any

import httpx

from aiops.engine.errors import InfrastructureError


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise InfrastructureError(f"沒有設定 {name}")
    return value


def request(client: httpx.Client, source: str, method: str, url: str, **kwargs: Any) -> httpx.Response:
    """回傳 2xx 或 400／404（查詢錯誤，交給呼叫端解讀）；其餘一律 InfrastructureError。"""
    try:
        resp = client.request(method, url, **kwargs)
    except httpx.HTTPError as e:
        raise InfrastructureError(f"{source} 連不上：{e}") from e
    if resp.status_code in (401, 403):
        raise InfrastructureError(f"{source} 認證失敗（HTTP {resp.status_code}），請檢查帳號或 token")
    if resp.status_code in (400, 404, 422) or resp.is_success:
        return resp
    raise InfrastructureError(f"{source} 回應錯誤（HTTP {resp.status_code}）：{resp.text[:200]}")


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
