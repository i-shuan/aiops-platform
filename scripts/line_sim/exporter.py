"""產線指標模擬的即時 /metrics（Prometheus 文字格式），給演練環境的 Prometheus 抓。只用標準函式庫。

  python exporter.py            # 聽 0.0.0.0:9464

注入故障：建立 /tmp/fault（內容不重要，以檔案的修改時間當故障開始）；刪掉就恢復，積壓的訊息每分鐘消 600 筆。
  kubectl -n line-sim exec deploy/line-sim -- touch /tmp/fault
  kubectl -n line-sim exec deploy/line-sim -- rm -f /tmp/fault
"""
from __future__ import annotations

import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import model  # noqa: E402

FAULT_FILE = "/tmp/fault"


class State:
    """積壓的訊息數要逐分鐘累加（故障中 +280、恢復後 −600），其他值只看現在是哪一分鐘。"""

    def __init__(self) -> None:
        self.backlog = 0.0
        self.last = time.time()

    def step(self, now: float) -> float | None:
        """回傳故障已經進行幾分鐘（沒有故障回 None），並把積壓推進到 now。"""
        minutes = max(0.0, (now - self.last) / 60)
        self.last = now
        try:
            started = os.path.getmtime(FAULT_FILE)
        except OSError:
            self.backlog = max(0.0, self.backlog - model.DRAIN_PER_MINUTE * minutes)
            return None
        self.backlog = min(500000.0, self.backlog + model.BACKLOG_PER_MINUTE * minutes)
        return max(0.0, (now - started) / 60)


STATE = State()


def render(now: float) -> str:
    elapsed, ts, lines = STATE.step(now), model.utc(now), []
    for name, labels, help_text in model.SERIES:
        if elapsed is not None:
            value = model.faulted(name, ts, elapsed, STATE.backlog)
        elif name == "mq_queue_depth":
            value = model.normal(name, ts) + round(STATE.backlog)  # 恢復後還沒消完的積壓
        else:
            value = model.normal(name, ts)
        label_text = ",".join(f'{k}="{v}"' for k, v in labels.items())
        lines += [f"# HELP {name} {help_text}", f"# TYPE {name} gauge", f"{name}{{{label_text}}} {value}"]
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = render(time.time()).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args) -> None:  # 每 15 秒一筆抓取紀錄沒有用
        return


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9464), Handler).serve_forever()
