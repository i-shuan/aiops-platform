"""連線檢查（src/checks.py）。Grafana 與 ES 的回應是從真的 Grafana 13.0.7 + Elasticsearch 8.19.23 錄下來的
（tests/fixtures/grafana13/org*.json、probe_logs_*.json，錄製腳本 record_checks.py）：11 筆沒拆欄位的 K8s 容器日誌。"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone

import httpx
import pytest

from src import checks
from src.db import Database
from src.grafana import Grafana, LogFields
from src.main import Settings
from tests.conftest import recorded

NOW = datetime(2026, 11, 24, 13, 40, tzinfo=timezone.utc)
BASE = Settings(grafana_url="http://grafana.test", grafana_token="gf-s3cret-token", webhook_token="w",
                es_datasource_uid="es-uid", log_index="fluentd-demo", edges=(("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")))
CONTAINER = replace(BASE, log_fields=LogFields(service="kubernetes.container_name", level=""))


def grafana_replaying(probe: str, org: str = "org", tempo: httpx.Response | None = None):
    seen = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/api/org":
            return httpx.Response(recorded(org)["status"], json=recorded(org)["response"])
        if request.url.path.endswith("/_msearch"):
            return httpx.Response(200, json=recorded(probe)["response"])
        if request.url.path.endswith("/api/search"):
            return tempo or httpx.Response(200, json=recorded("tempo_search")["response"])
        return httpx.Response(404, json={"message": "Not found"})

    return httpx.MockTransport(handle), seen


def by_key(items):
    return {i["key"]: i for i in items}


def test_logs_with_the_default_field_names_on_unparsed_container_logs_warn_and_say_what_to_change(db_url):
    transport, _seen = grafana_replaying("probe_logs_fields_missing")
    database = Database(db_url)
    got = by_key(checks.run_all(BASE, database, now=NOW, grafana_transport=transport, include_llm=False))
    database.close()
    assert got["database"]["status"] == "ok" and got["grafana"]["status"] == "ok" and "Main Org." in got["grafana"]["detail"]
    logs = got["logs"]
    assert logs["status"] == "warn" and "11 筆" in logs["detail"] and "錯誤日誌的 0 筆" in logs["detail"]
    assert any("沒有 service.name 這個欄位" in h for h in logs["hints"])
    assert any("沒有 log.level 這個欄位" in h and "留空" in h for h in logs["hints"])
    assert got["tempo"]["status"] == "skipped" and "llm" not in got


def test_logs_with_the_container_field_and_no_level_field_are_ok_and_list_the_services():
    transport, seen = grafana_replaying("probe_logs_container")
    grafana = Grafana("http://grafana.test", "t", transport=transport)
    logs = checks.check_logs(grafana, CONTAINER, NOW)
    assert logs["status"] == "ok" and logs["hints"] == []
    assert "錯誤日誌的 6 筆" in logs["detail"] and "cim-eap（4）" in logs["detail"] and "沒有等級欄位" in logs["detail"]
    # 送出去的查詢跟錄製時的一模一樣（除了時間窗）：錄下來的回應才對得上
    sent = [json.loads(line) for line in seen[0].content.decode().splitlines()]
    was = [json.loads(line) for line in recorded("probe_logs_container")["request"]["body"].splitlines()]
    strip = lambda q: json.loads(json.dumps(q).replace(json.dumps(q.get("query", {}).get("bool", {}).get("filter", [{}])[0]), '"window"'))
    assert [strip(q) for q in sent] == [strip(q) for q in was]


def test_an_edge_name_that_is_not_among_the_log_services_is_pointed_out():
    transport, _seen = grafana_replaying("probe_logs_container")
    grafana = Grafana("http://grafana.test", "t", transport=transport)
    settings = replace(CONTAINER, edges=(("mq-broker", "cim-eap"), ("mes-db", "cim-eap")))
    logs = checks.check_logs(grafana, settings, NOW)
    assert logs["status"] == "warn" and any("mes-db" in h and "service" in h for h in logs["hints"])


def test_a_bad_grafana_token_fails_that_check_and_does_not_leak_the_token(db_url):
    transport, _seen = grafana_replaying("probe_logs_container", org="org_bad_token")
    database = Database(db_url)
    items = checks.run_all(BASE, database, now=NOW, grafana_transport=transport, include_llm=False)
    database.close()
    got = by_key(items)
    assert got["grafana"]["status"] == "fail" and "Invalid API key" in got["grafana"]["detail"]
    assert "gf-s3cret-token" not in json.dumps(items, ensure_ascii=False)


def test_tempo_is_checked_only_when_configured():
    transport, seen = grafana_replaying("probe_logs_container")
    grafana = Grafana("http://grafana.test", "t", transport=transport)
    assert checks.check_tempo(grafana, BASE, NOW)["status"] == "skipped" and not seen
    ok = checks.check_tempo(grafana, replace(BASE, tempo_datasource_uid="tempo-uid"), NOW)
    assert ok["status"] == "ok" and seen[0].url.path == "/api/datasources/proxy/uid/tempo-uid/api/search"
    bad, _ = grafana_replaying("probe_logs_container", tempo=httpx.Response(404, json={"message": "Data source not found"}))
    failed = checks.check_tempo(Grafana("http://grafana.test", "t", transport=bad, retries=0), replace(BASE, tempo_datasource_uid="x"), NOW)
    assert failed["status"] == "fail" and "Data source not found" in failed["detail"]


@pytest.mark.parametrize("response, status, text", [
    (httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]}), "ok", "有回應"),
    (httpx.Response(502, text="Bad Gateway"), "fail", "502"),
    (httpx.Response(401, json={"error": {"message": "Incorrect API key provided: sk-s3cret-llm-key"}}), "fail", "401"),
    (httpx.Response(200, json={"unexpected": True}), "fail", "KeyError"),
])
def test_llm_check_reports_the_outcome_and_never_echoes_the_key(response, status, text):
    settings = replace(BASE, llm_api_url="http://llm.test/v1", llm_api_key="sk-s3cret-llm-key", llm_model="m")
    sent = []

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return response

    item = checks.check_llm(settings, httpx.MockTransport(handle))
    assert item["status"] == status and text in item["detail"]
    assert "sk-s3cret-llm-key" not in json.dumps(item, ensure_ascii=False)
    assert "temperature" not in sent[0]   # 沒設 temperature 就不送（有些模型只接受預設值）
    assert checks.check_llm(replace(settings, llm_model=""))["status"] == "skipped"


def test_database_check_reports_a_broken_connection():
    class Broken:
        engine = None

        def ping(self):
            raise ConnectionError("connection refused")

    assert checks.check_database(Broken())["status"] == "fail"
