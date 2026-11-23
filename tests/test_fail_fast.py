"""設定錯誤在碰資料庫、碰外網之前就擋下（稽核 Q7／Q15，docs/ARCH_AUDIT.md）。

判準：報錯之前「0 句 SQL、0 次對外 HTTP」，而且沒走到 StorageHooks.load_batch。
SQL 用 SQLAlchemy 的 before_cursor_execute 計數（含 SELECT），對外 HTTP 計 httpx.Client.send 裡 host 不是 testserver 的請求。
對照組（正常的 workflow）一定有 SQL：證明計數器本身是活的，不是永遠 0。
"""
import shutil
from datetime import datetime, timezone

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine

from aiops.engine.errors import WorkflowError
from aiops.interfaces import runner
from aiops.storage.hooks import StorageHooks

T0 = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)

GOOD = ("name: good\nnodes:\n  - {name: fetch, operator: ingestion.fetch}\n  - {name: detect, operator: detector.hourly_baseline}\n"
        "  - {name: group, operator: alert.same_entity}\n  - {name: notify, operator: notifier.log}\n"
        "targets:\n  - {source_type: mock, query: q, entity: e}\n")
BROKEN = {
    "bad_yaml": "name: bad_yaml\nnodes: [\n",
    "bad_operator": "name: bad_operator\nnodes:\n  - {name: x, operator: detector.nope}\n",
    "bad_scope_order": "name: bad_scope_order\nnodes:\n  - {name: n, operator: notifier.log}\n  - {name: fetch, operator: ingestion.fetch}\n",
    "bad_detectors": ("name: bad_detectors\nnodes:\n  - {name: fetch, operator: ingestion.fetch}\n"
                      "  - {name: detect, operator: detector.hourly_baseline}\n"
                      "targets:\n  - {source_type: mock, query: q, entity: e, detectors: [detcet]}\n"),
    "bad_target_field": ("name: bad_target_field\nnodes:\n  - {name: fetch, operator: ingestion.fetch}\n"
                         "  - {name: detect, operator: detector.hourly_baseline}\n"
                         "targets:\n  - {source_type: mock, query: q, entity: e, detector: [detect]}\n"),
    "good": GOOD,
}
OK_TARGET = {"source_type": "mock", "query": "q", "entity": "e"}


@pytest.fixture
def side_effects(monkeypatch):
    """SQL 句數、對外 HTTP、load_batch 呼叫次數。"""
    seen = {"sql": 0, "http_out": 0, "load_batch": 0}

    def on_sql(*_a):
        seen["sql"] += 1

    event.listen(Engine, "before_cursor_execute", on_sql)
    real_send = httpx.Client.send

    def send(self, request, *a, **k):
        if request.url.host != "testserver":
            seen["http_out"] += 1
        return real_send(self, request, *a, **k)

    monkeypatch.setattr(httpx.Client, "send", send)
    real_load = StorageHooks.load_batch

    def load_batch(self, ctx):
        seen["load_batch"] += 1
        return real_load(self, ctx)

    monkeypatch.setattr(StorageHooks, "load_batch", load_batch)
    yield seen
    event.remove(Engine, "before_cursor_execute", on_sql)


@pytest.fixture
def workflows(tmp_path, monkeypatch):
    """內建 pipelines/ 的暫存複本，加上幾條寫壞的 workflow；不動真的 pipelines/。"""
    shutil.copytree(runner.WORKFLOW_DIR, tmp_path / "pipelines")
    for name, text in BROKEN.items():
        # 放在 samples/（測試裡准許拿偵測器跑呼叫端序列的資料夾，tests/conftest.py）：這些檔會跑偵測器、API 會帶 targets，
        # 放 detection/ 會先被偵測入口政策擋成 403（使用者管線不准跑偵測），就測不到「設定錯誤在碰資料庫之前就 400」
        (tmp_path / "pipelines" / "samples" / f"{name}.yaml").write_text(text, encoding="utf-8")
    monkeypatch.setattr(runner, "WORKFLOW_DIR", tmp_path / "pipelines")
    return tmp_path / "pipelines"


def _reset(seen):
    for k in seen:
        seen[k] = 0


@pytest.mark.parametrize("body, status", [
    ({"workflow": "../x"}, 422),                                      # 1. Pydantic：名稱 pattern
    ({"workflow": "x.yaml"}, 422),
    ({"workflow": "nope"}, 404),                                      # 3. workflow_path：找不到
    ({"workflow": "bad_yaml"}, 400),                                  # 4. load_workflow：YAML 語法
    ({"workflow": "bad_target_field"}, 400),                          # 4. load_workflow：target extra=forbid
    ({"workflow": "bad_operator"}, 400),                              # 5. Pipeline.__init__：未註冊算子
    ({"workflow": "bad_scope_order"}, 400),                           # 5. Pipeline.__init__：scope 順序
    ({"workflow": "bad_detectors"}, 400),                             # 5. Pipeline.__init__：YAML target 的 detectors
    ({"workflow": "good", "targets": [{**OK_TARGET, "detector": ["detect"]}]}, 422),   # 1. API target 欄位拼錯
    ({"workflow": "good", "targets": [{**OK_TARGET, "detectors": []}]}, 422),          # 1. 空白名單
    ({"workflow": "good", "targets": [OK_TARGET, {**OK_TARGET, "entity": "e2", "detectors": ["detcet"]}]}, 400),  # 6. run() 開頭
    ({"workflow": "good", "targets": [{**OK_TARGET, "overrides": {"detect": {"threshold": 999}}}]}, 400),
], ids=["dotdot", "dot-yaml", "missing", "yaml-syntax", "yaml-target-field", "unregistered-operator", "scope-order",
        "yaml-detectors-typo", "api-target-field", "api-empty-whitelist", "api-detectors-typo-2nd-target",
        "api-override-out-of-range"])
def test_config_errors_are_rejected_before_any_database_or_network_access(client, workflows, side_effects, body, status):
    _reset(side_effects)
    r = client.post("/api/v1/runs", json=body)
    assert r.status_code == status, r.text
    assert side_effects == {"sql": 0, "http_out": 0, "load_batch": 0}


def test_the_counters_are_live_a_good_run_does_touch_the_database(client, workflows, side_effects):
    _reset(side_effects)
    r = client.post("/api/v1/runs", json={"workflow": "good"})
    assert r.status_code == 200, r.text
    assert side_effects["sql"] > 0 and side_effects["load_batch"] == 1 and side_effects["http_out"] == 0


def test_api_target_errors_stop_the_whole_run_not_just_that_target(client, workflows, side_effects, monkeypatch):
    """第 2 條 target 寫錯：第 1 條正常的也不會被跑（沒有任何 fetch）。"""
    from aiops.sources.mock import MockSpec

    fetched: list = []
    real = MockSpec.fetch_data
    monkeypatch.setattr(MockSpec, "fetch_data", lambda self, *a, **k: (fetched.append(a[0]), real(self, *a, **k))[1])
    _reset(side_effects)
    r = client.post("/api/v1/runs", json={"workflow": "good", "targets": [
        OK_TARGET, {**OK_TARGET, "entity": "e2", "overrides": {"nope": {}}}]})
    assert r.status_code == 400 and "nope" in r.json()["message"]
    assert fetched == [] and side_effects["sql"] == 0


@pytest.mark.parametrize("name", ["bad_detectors", "bad_operator"])
def test_cli_with_its_own_yaml_file_also_fails_before_touching_the_database(workflows, side_effects, name):
    _reset(side_effects)
    with pytest.raises(WorkflowError):
        runner.run_context(str(workflows / "samples" / f"{name}.yaml"), allow_file=True, strict=False, now=T0)
    assert side_effects == {"sql": 0, "http_out": 0, "load_batch": 0}
