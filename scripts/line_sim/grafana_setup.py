"""在演練用的 Grafana 上建立產線模擬要用的資料源、資料夾與告警規則（可重複執行：已經有的就更新）。

這是「使用者在自己的 Grafana 上會做的事」的示範：每個要交給平台演算法看的指標建一條告警規則，貼上
  aiops_detect: <偵測管線名稱>、severity: P0～P3（不貼 = 管線的預設 P1）、service: <實體>
規則本身設成暫停（isPaused）：它只是用來告訴平台「看這條查詢」，Grafana 自己不會拿它發告警。

  AIOPS_GRAFANA_URL=http://127.0.0.1:13001 AIOPS_GRAFANA_USER=admin AIOPS_GRAFANA_PASSWORD=... python grafana_setup.py

只給本機演練叢集用。帳密從環境變數讀，不印出來。
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib

import httpx

DATASOURCE = {"uid": "line-prometheus", "name": "line-prometheus", "type": "prometheus", "access": "proxy",
              "url": "http://prometheus.line-sim.svc:9090"}
FOLDER = {"uid": "aiops-line-sim", "title": "AIOps 產線模擬"}
GROUP = "line-sim"
# (uid, 標題, 查詢, 實體, 嚴重度〔None = 不貼，用管線的預設〕)
# 查詢用 max by (line) 彙總成一條：一條規則只能是一條序列（回補的歷史沒有 instance／job 標籤、即時抓的有，
# 不彙總會查回兩條，平台會拒絕並要求收斂）
RULES = [
    ("line-mq-queue-depth", "MQ 佇列深度", 'max by (line) (mq_queue_depth{line=\"fab12\"})', "mq-broker", "P1"),
    ("line-mq-consumers", "MQ 消費者連線數", 'max by (line) (mq_consumer_count{line=\"fab12\"})', "mq-broker", "P0"),
    ("line-mq-msg-rate", "MQ 每秒處理筆數", 'max by (line) (mq_msg_rate{line=\"fab12\"})', "mq-broker", "P1"),
    ("line-cim-connected", "CIM 在線機台數", 'max by (line) (cim_connected_eqp_count{line=\"fab12\"})', "cim-eap", "P0"),
    ("line-cim-timeouts", "CIM 通訊逾時次數", 'max by (line) (cim_timeout_count{line=\"fab12\"})', "cim-eap", "P2"),
    ("line-amhs-duration", "AMHS 搬運耗時", 'max by (line) (amhs_transfer_duration_seconds{line=\"fab12\"})', "amhs-mcs", "P2"),
    ("line-amhs-aborts", "AMHS 搬運中止次數", 'max by (line) (amhs_abort_count{line=\"fab12\"})', "amhs-mcs", "P1"),
    ("line-amhs-vehicles", "AMHS 可用天車數", 'max by (line) (amhs_active_vehicles{line=\"fab12\"})', "amhs-mcs", None),
]


def rule_body(uid: str, title: str, expr: str, service: str, severity: str | None, pipeline: str) -> dict:
    labels = {"aiops_detect": pipeline, "service": service}
    if severity:
        labels["severity"] = severity
    return {
        "uid": uid, "title": title, "folderUID": FOLDER["uid"], "ruleGroup": GROUP, "orgID": 1, "condition": "C",
        "for": "5m", "noDataState": "OK", "execErrState": "OK", "isPaused": True, "labels": labels,
        "annotations": {"summary": "交給 AIOps 平台的演算法看的序列（產線模擬）"},
        "data": [
            {"refId": "A", "datasourceUid": DATASOURCE["uid"], "relativeTimeRange": {"from": 300, "to": 0},
             "model": {"refId": "A", "expr": expr, "instant": True, "intervalMs": 1000, "maxDataPoints": 43200}},
            # 寬鬆到平常不會成立的門檻：規則要有一個條件才存得進去，判斷交給平台
            {"refId": "C", "datasourceUid": "__expr__", "relativeTimeRange": {"from": 0, "to": 0},
             "model": {"refId": "C", "type": "threshold", "expression": "A",
                       "conditions": [{"evaluator": {"type": "gt", "params": [1e12]}}]}},
        ],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pipeline", default="grafana_detect", help="aiops_detect 標籤的值（偵測管線名稱）")
    args = ap.parse_args()
    base = os.environ["AIOPS_GRAFANA_URL"].rstrip("/")
    client = httpx.Client(base_url=base, auth=(os.environ["AIOPS_GRAFANA_USER"], os.environ["AIOPS_GRAFANA_PASSWORD"]),
                          timeout=20, headers={"X-Disable-Provenance": "true"})

    r = client.get(f"/api/datasources/uid/{DATASOURCE['uid']}")
    r = client.post("/api/datasources", json=DATASOURCE) if r.status_code == 404 else \
        client.put(f"/api/datasources/uid/{DATASOURCE['uid']}", json=DATASOURCE)
    print("資料源", DATASOURCE["uid"], r.status_code)
    r.raise_for_status()

    r = client.get(f"/api/folders/{FOLDER['uid']}")
    if r.status_code == 404:
        r = client.post("/api/folders", json=FOLDER)
    print("資料夾", FOLDER["uid"], r.status_code)
    r.raise_for_status()

    for uid, title, expr, service, severity in RULES:
        body = rule_body(uid, title, expr, service, severity, args.pipeline)
        exists = client.get(f"/api/v1/provisioning/alert-rules/{uid}").status_code == 200
        r = client.put(f"/api/v1/provisioning/alert-rules/{uid}", json=body) if exists else \
            client.post("/api/v1/provisioning/alert-rules", json=body)
        print("規則", uid, "更新" if exists else "新增", r.status_code, "" if r.is_success else r.text[:200])
        r.raise_for_status()

    # 儀表板：產線指標的時序圖（上面會出現平台寫的事件群註記），以及 chart 裡那份「平台判出的異常與根因」
    r = client.post("/api/dashboards/db", json={"dashboard": line_dashboard(), "folderUid": FOLDER["uid"], "overwrite": True})
    print("儀表板 aiops-line-sim", r.status_code, "" if r.is_success else r.text[:200])
    r.raise_for_status()
    incidents = json.loads((pathlib.Path(__file__).resolve().parents[2] / "deploy" / "helm" / "aiops-grafana" / "files"
                            / "dashboards" / "aiops-incidents.json").read_text(encoding="utf-8"))
    incidents.pop("id", None)
    r = client.post("/api/dashboards/db", json={"dashboard": incidents, "folderUid": FOLDER["uid"], "overwrite": True})
    print("儀表板 aiops-incidents", r.status_code, "" if r.is_success else r.text[:200])
    r.raise_for_status()


def line_dashboard() -> dict:
    """三張時序圖（MQ／CIM／AMHS），儀表板層級查 aiops 標籤的註記：事件群會以紅色區間標在圖上，滑鼠移上去看摘要與根因。"""
    def panel(pid: int, title: str, exprs: list[tuple[str, str]], y: int) -> dict:
        return {"id": pid, "type": "timeseries", "title": title, "datasource": {"type": "prometheus", "uid": DATASOURCE["uid"]},
                "gridPos": {"x": 0, "y": y, "w": 24, "h": 8},
                "targets": [{"refId": chr(65 + i), "expr": f'max by (line) ({expr}{{line="fab12"}})', "legendFormat": legend}
                            for i, (expr, legend) in enumerate(exprs)]}
    return {
        "uid": "aiops-line-sim", "title": "產線指標（模擬）＋ AIOps 事件群", "timezone": "browser", "schemaVersion": 39,
        "time": {"from": "now-3h", "to": "now"}, "refresh": "1m",
        "annotations": {"list": [{"name": "AIOps 事件群", "enable": True, "iconColor": "red",
                                  "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                                  "target": {"type": "tags", "tags": ["aiops"], "matchAny": False, "limit": 200}}]},
        "panels": [
            panel(1, "MQ：佇列深度、消費者數", [("mq_queue_depth", "佇列深度"), ("mq_consumer_count", "消費者數")], 0),
            panel(2, "CIM：在線機台數、逾時次數", [("cim_connected_eqp_count", "在線機台數"), ("cim_timeout_count", "逾時次數")], 8),
            panel(3, "AMHS：搬運耗時、可用天車數、中止次數", [("amhs_transfer_duration_seconds", "搬運耗時（秒）"),
                                                    ("amhs_active_vehicles", "可用天車數"), ("amhs_abort_count", "中止次數")], 16),
        ],
    }


if __name__ == "__main__":
    main()
