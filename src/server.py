"""常駐服務：收 Grafana 的告警 Webhook，等 debounce 時間到再跑一輪（src/main.process）。`python -m src.server`。

為什麼不收到就跑：上下游的告警會差幾十秒才陸續響、日誌也要時間寫進 VictoriaLogs；第 0 秒就分析，LLM 拿到的是殘缺的證據，
一場事故連發好幾個 Webhook 還會同時跑好幾次。所以收到第一個 Webhook 後固定等 debounce_seconds，
這段時間進來的告警全部收進同一批，時間到才一起收斂、撈證據、問一次 LLM。視窗不會因為後面一直有告警進來而延後。

  POST /webhook   Grafana Contact Point（Webhook）推告警進來，要帶 Authorization: Bearer <config.yaml 的 webhook.token>
                  先存進資料庫才回 200：存不進去回 503，Grafana 會重送
  GET  /healthz   給 Kubernetes 探針用，不用 Token。工作執行緒死了、或同一輪跑超過 healthcheck.stuck_seconds（卡死）回 503，
                  K8s 連續失敗幾次就會重啟 Pod；HTTP 服務本身沒回應也一樣

同一時間只會有一輪在跑（單一工作執行緒）；跑的途中進來的告警排進下一個視窗。有進行中事故時，沒有新 Webhook 也會每隔
followup_seconds 再跑一輪（結案、補寫註記、重試 LLM）。服務重啟時若資料庫裡還有沒處理的告警或進行中事故，啟動後自動排一輪。
只能跑 1 個副本：debounce 的計時在記憶體裡，兩個副本會各跑各的。
"""
from __future__ import annotations

import hmac
import json
import logging
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from src.db import Database
from src.grafana import WebhookError, parse_webhook
from src.main import ConfigError, Settings, log_summary, process, setup_logging

log = logging.getLogger("aiops_lite.server")

# 這個檔案沒有寫死的門檻，都在 config.yaml（選填，預設值見 main.Settings）：
#   webhook.max_body_bytes       Webhook 內容大小上限（1 MB）
#   retry_failed_round_seconds   一輪失敗（資料庫斷線）之後多久再試（30 秒）
#   healthcheck.stuck_seconds    一輪跑多久算卡死（600 秒）。正常的一輪是幾秒；最慢的情況是每個進行中事故各等一次日誌查詢與 LLM 的逾時，
#                                所以預設抓 10 分鐘：寧可晚一點重啟，也不要把「LLM 很慢」當成卡死而砍掉正在跑的分析。
#                                調大了 llm.timeout_seconds 的話這個也要跟著調大


class Debounce:
    """什麼時候該跑下一輪（純邏輯，時間由呼叫端傳入）。

    webhook()：沒有視窗在等才開一個（到期時間 = 現在 + debounce）；視窗已經開著就不動，不會被後面的告警一直往後推。
    followup()：沒有任何排程時才排（不會蓋掉、也不會提前已經開著的 debounce 視窗）。
    """

    def __init__(self, debounce_seconds: float):
        self.debounce_seconds = debounce_seconds
        self.due: float | None = None
        self.window_open = False

    def webhook(self, now: float) -> None:
        if not self.window_open:
            self.due, self.window_open = now + self.debounce_seconds, True

    def followup(self, now: float, delay: float) -> None:
        if self.due is None:
            self.due = now + delay

    def wait_seconds(self, now: float) -> float | None:
        """還要等幾秒（0 = 該跑了）；None = 沒有排程，等下一個 Webhook。"""
        return None if self.due is None else max(0.0, self.due - now)

    def start_round(self) -> None:
        self.due, self.window_open = None, False


class Worker(threading.Thread):
    """唯一的工作執行緒：等到期、跑一輪、視結果排下一輪。"""

    def __init__(self, settings: Settings, run_round: Callable[[], dict[str, Any]],
                 clock: Callable[[], float] = time.monotonic):
        super().__init__(name="aiops-lite-worker", daemon=True)
        self.settings, self.run_round, self.clock = settings, run_round, clock
        self.debounce = Debounce(settings.debounce_seconds)
        self.wake = threading.Condition()
        self.stopping = False
        self.rounds = 0
        self.round_started: float | None = None  # 正在跑的那一輪是什麼時候開始的；沒在跑是 None

    def notify_webhook(self) -> None:
        with self.wake:
            self.debounce.webhook(self.clock())
            self.wake.notify()

    def schedule(self, delay: float) -> None:
        with self.wake:
            self.debounce.followup(self.clock(), delay)
            self.wake.notify()

    def stop(self) -> None:
        with self.wake:
            self.stopping = True
            self.wake.notify()

    def health(self) -> str | None:
        """健康回 None，不健康回原因。is_alive 抓得到「執行緒死了」，抓不到「還活著但卡住」，所以另外看這一輪跑了多久。"""
        if not self.is_alive():
            return "worker dead"
        started = self.round_started
        if started is not None and self.clock() - started > self.settings.healthcheck_stuck_seconds:
            return f"round stuck for {int(self.clock() - started)}s"
        return None

    def run(self) -> None:
        while True:
            with self.wake:
                while not self.stopping:
                    wait = self.debounce.wait_seconds(self.clock())
                    if wait == 0:
                        break
                    self.wake.wait(timeout=wait)
                if self.stopping:
                    return
                self.debounce.start_round()  # 從這一刻起進來的告警屬於下一個視窗
            self.round_started = self.clock()
            try:
                summary = self.run_round()
                log_summary(summary)
                delay = self.settings.followup_seconds if summary["needs_followup"] else None
            except Exception:  # process 已經記了完整的錯誤；告警還在 inbox，晚點再試
                delay = self.settings.retry_failed_round_seconds
            self.round_started = None
            self.rounds += 1
            if delay is not None:
                self.schedule(delay)


def make_handler(settings: Settings, db: Database, worker: Worker) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _reply(self, status: int, body: dict[str, Any]) -> None:
            raw = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            if self.path != "/healthz":
                return self._reply(404, {"error": "not found"})
            reason = worker.health()
            if reason:
                return self._reply(503, {"status": "unhealthy", "reason": reason})
            self._reply(200, {"status": "ok"})

        def do_POST(self) -> None:
            if self.path.split("?")[0] != "/webhook":
                return self._reply(404, {"error": "not found"})
            given = self.headers.get("Authorization", "")
            if not hmac.compare_digest(given.encode(), f"Bearer {settings.webhook_token}".encode()):
                return self._reply(401, {"error": "Authorization 不對"})
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                return self._reply(411, {"error": "要有 Content-Length"})
            if length > settings.webhook_max_body_bytes:
                return self._reply(413, {"error": f"內容超過 {settings.webhook_max_body_bytes} bytes"})
            try:
                alerts, problems = parse_webhook(json.loads(self.rfile.read(length)), settings.labels)
            except (WebhookError, ValueError) as e:  # ValueError 含 JSON 解析失敗、時間格式不對
                return self._reply(400, {"error": str(e)})
            for line in problems:
                log.warning("%s", line)
            try:
                db.enqueue(alerts, datetime.now(timezone.utc))
            except Exception:
                log.exception("告警存不進資料庫")
                return self._reply(503, {"error": "資料庫暫時寫不進去，請重送"})
            if alerts:
                worker.notify_webhook()
            self._reply(200, {"accepted": len(alerts), "problems": problems})

        def log_message(self, fmt: str, *args: Any) -> None:
            log.debug("%s %s", self.address_string(), fmt % args)

    return Handler


def serve(settings: Settings, *, host: str | None = None, run_round: Callable[[], dict[str, Any]] | None = None
          ) -> tuple[ThreadingHTTPServer, Worker, Database]:
    """起 HTTP 服務與工作執行緒，回傳 (server, worker, db)；呼叫端負責 server.serve_forever()。"""
    db = Database(settings.database_url)
    worker = Worker(settings, run_round or (lambda: process(settings)))
    server = ThreadingHTTPServer((host or settings.webhook_host, settings.port), make_handler(settings, db, worker))
    worker.start()
    ids, _alerts = db.pending()
    if ids or db.open_incidents() or db.unannotated_incidents():
        worker.notify_webhook() if ids else worker.schedule(0)  # 重啟前留下來的工作
    return server, worker, db


def main() -> int:
    setup_logging()
    try:
        settings = Settings.load()
        setup_logging(settings.log_level)
    except ConfigError as e:
        log.error("設定錯誤：%s", e)
        return 2
    server, worker, db = serve(settings)
    log.info("aiops-lite 啟動：port=%d debounce=%ds followup=%ds close_after=%ds edges=%d", settings.port,
             settings.debounce_seconds, settings.followup_seconds, settings.close_after_seconds, len(settings.edges))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()
        server.server_close()
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
