"""看守機制：主要存放（DB）掛掉時，故障資訊仍要送得出去。

屬於基礎設施，不綁在任何業務外掛裡：
- 一律寫本機日誌（設定 AIOPS_FALLBACK_LOG 時寫檔，否則走 stderr）
- 設定 AIOPS_FALLBACK_WEBHOOK 時直接打 webhook（不經 DB、不經告警流程）
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("aiops.fallback")


def init_fallback_logging() -> None:
    path = os.environ.get("AIOPS_FALLBACK_LOG")
    if path and not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        logger.addHandler(logging.FileHandler(path, encoding="utf-8"))
    logger.setLevel(logging.INFO)


def report_failure(exc: Exception, context: dict[str, Any]) -> None:
    payload = {
        "at": datetime.now(timezone.utc).isoformat(),
        "error": type(exc).__name__,
        "message": str(exc),
        **context,
    }
    logger.error(json.dumps(payload, ensure_ascii=False))
    url = os.environ.get("AIOPS_FALLBACK_WEBHOOK")
    if url:
        try:
            httpx.post(url, json=payload, timeout=3)
        except httpx.HTTPError:
            logger.exception("fallback webhook 送出失敗")
