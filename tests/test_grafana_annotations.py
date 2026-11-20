"""把事件群與 RCA 結論標在 Grafana 圖表的時間軸上（notifier.grafana_annotations）。

使用者 2026-11-20 的決定：RCA 結果直接顯示在 Grafana，不做平台自己的網頁。一個事件群一筆註記，
告警那一段與 RCA 那一段各自更新、互不覆蓋；寫失敗不影響通知與報告。
真實的 Grafana 13.0.7 上跑過（兩個事件群各一筆、RCA 補上結論）；這裡用假的 Grafana 測更新的規則。
"""
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aiops.engine.context import PipelineContext, RootCandidate
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.notification import notifiers
from aiops.stages.notification.transport import GrafanaAnnotations

T = datetime(2026, 11, 20, 12, 8, tzinfo=timezone.utc)
RULE = "https://grafana.example.internal/alerting/grafana/line-mq-consumers/view"


class FakeGrafana:
    """只會註記的假 Grafana：GET 依 tags（AND）找、POST 新增、PATCH 更新。"""

    def __init__(self, fail: int | None = None):
        self.rows: dict[int, dict] = {}
        self.calls: list[tuple[str, str]] = []
        self.fail = fail

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path))
        if self.fail:
            return httpx.Response(self.fail, json={"message": "nope"})
        if request.method == "GET":
            wanted = set(request.url.params.get_list("tags"))
            return httpx.Response(200, json=[r for r in self.rows.values() if wanted <= set(r["tags"])][:1])
        body = json.loads(request.content)
        if request.method == "POST":
            new_id = len(self.rows) + 1
            self.rows[new_id] = {"id": new_id, **body}
            return httpx.Response(200, json={"id": new_id, "message": "Annotation added"})
        self.rows[int(request.url.path.rsplit("/", 1)[1])].update(body)
        return httpx.Response(200, json={"message": "Annotation patched"})


@pytest.fixture
def grafana(monkeypatch):
    fake = FakeGrafana()
    client = GrafanaAnnotations("http://grafana.test", {"Authorization": "Bearer t"}, httpx.Client(transport=httpx.MockTransport(fake)))
    monkeypatch.setattr(notifiers, "_grafana_annotations", lambda: client)
    return fake


def _incident(minutes: int = 6) -> Incident:
    attrs = {"severity": "P0", "grafana_rule_url": RULE}
    events = [AnomalyEvent("mq-broker", "line-mq-consumers", T + timedelta(minutes=m), 4.0, attrs, 0.0, "detector.hourly_baseline", "detect_hourly")
              for m in range(minutes + 1)]
    return Incident([*events, AnomalyEvent("cim-eap", "line-cim-timeouts", T + timedelta(minutes=3), 9.0, {}, 5.0,
                                           "detector.sparse_count", "detect_sparse")], score=9.0)


def _alert_round(incident: Incident) -> PipelineContext:
    ctx = PipelineContext(pipeline="grafana_detect", now=incident.end + timedelta(minutes=1))
    ctx.incidents, ctx.incidents_built = [incident], True
    ctx = OperatorRegistry.create("alert.notify_payload", {}).execute(ctx)
    return OperatorRegistry.create("notifier.grafana_annotations", {}).execute(ctx)


def _rca_round(incident: Incident, candidates, conclusion: str, reason: str = "") -> PipelineContext:
    ctx = PipelineContext(pipeline="rca_report", now=incident.end + timedelta(minutes=5))
    ctx.incidents = [incident]
    ctx.rca_root_algorithm, ctx.rca_root_candidates = "rca.root_cause.evidence", list(candidates)
    ctx.rca_conclusion_type, ctx.rca_conclusion_reason = conclusion, reason
    return OperatorRegistry.create("notifier.grafana_annotations", {}).execute(ctx)


def test_a_new_card_becomes_one_annotation_spanning_the_incident(grafana):
    incident = _incident()
    ctx = _alert_round(incident)
    (row,) = grafana.rows.values()
    assert (row["time"], row["timeEnd"]) == (int(T.timestamp() * 1000), int((T + timedelta(minutes=6)).timestamp() * 1000))
    assert row["tags"] == sorted(["aiops", f"incident:{incident.uid}", "severity:P0", "entity:cim-eap", "entity:mq-broker"])
    assert row["text"].startswith("[P0] cim-eap, mq-broker 異常")
    # 連結排成一行、文字是規則的 uid（一張卡有好幾條規則時分得出誰是誰）
    assert row["text"].endswith(f'<br>看規則、調整優先權：<a href="{RULE}">line-mq-consumers</a>')
    assert ctx.errors == []


def test_the_same_card_next_round_updates_its_annotation_instead_of_adding_one(grafana):
    incident = _incident(6)
    _alert_round(incident)
    longer = Incident([*incident.events, AnomalyEvent("mq-broker", "line-mq-consumers", T + timedelta(minutes=20), 4.0, {"severity": "P0"}, 0.0,
                                                    "detector.hourly_baseline", "detect_hourly")], score=9.0, uid=incident.uid)
    _alert_round(longer)
    (row,) = grafana.rows.values()
    assert row["timeEnd"] == int((T + timedelta(minutes=20)).timestamp() * 1000)
    assert [m for m, _ in grafana.calls].count("POST") == 1 and [m for m, _ in grafana.calls].count("PATCH") == 1


def test_rca_adds_its_conclusion_and_neither_half_overwrites_the_other(grafana):
    incident = _incident()
    _alert_round(incident)
    _rca_round(incident, [RootCandidate("mq-broker", 1.0, "首次異常 12:08（事件群中最早）；上游沒有其他異常")], "topology_inference")
    (row,) = grafana.rows.values()
    alert, mark, rca = row["text"].partition(GrafanaAnnotations.RCA_MARK)
    assert alert.startswith("[P0]") and mark and rca.startswith("<b>根因（依關係圖推測）：mq-broker</b>") and "事件群中最早" in rca
    assert row["timeEnd"] == int((T + timedelta(minutes=6)).timestamp() * 1000)   # RCA 不動時間範圍
    _alert_round(incident)                                                        # 下一輪偵測更新告警那一段
    assert "根因（依關係圖推測）：mq-broker" in grafana.rows[1]["text"]            # RCA 那一段還在
    _rca_round(incident, [RootCandidate("cim-eap", 1.0, "重跑後的結論")], "confirmed")
    text = grafana.rows[1]["text"]
    assert "根因（有直接證據）：cim-eap" in text and "mq-broker</b>" not in text and text.startswith("[P0]")  # 重跑 RCA 換掉舊結論
    assert len(grafana.rows) == 1


def test_rca_without_a_candidate_says_it_could_not_tell_and_why(grafana):
    incident = _incident()
    _rca_round(incident, [], "undetermined", "證據不足，不指認根因：入口請求不足")
    (row,) = grafana.rows.values()                       # 這個事件群之前沒有註記：告警那一段也補上
    assert row["text"].startswith("[") and "<b>根因：無法判定</b><br>證據不足，不指認根因：入口請求不足" in row["text"]


def test_text_from_data_is_escaped(grafana):
    incident = _incident()
    _rca_round(incident, [RootCandidate("svc<script>", 1.0, 'reason "<img src=x>"')], "confirmed")
    text = next(iter(grafana.rows.values()))["text"]
    assert "<script>" not in text and "<img" not in text and "svc&lt;script&gt;" in text


def test_a_grafana_failure_is_recorded_and_never_breaks_the_round(monkeypatch):
    fake = FakeGrafana(fail=403)
    client = GrafanaAnnotations("http://grafana.test", {}, httpx.Client(transport=httpx.MockTransport(fake)))
    monkeypatch.setattr(notifiers, "_grafana_annotations", lambda: client)
    ctx = _alert_round(_incident())
    assert len(ctx.notifications) == 1 and len(ctx.errors) == 1               # 通知還在，註記的失敗只是一筆錯誤
    assert ctx.errors[0].entity == "node:grafana_annotations" and "Grafana 註記沒寫成" in ctx.errors[0].message


def test_without_grafana_settings_nothing_is_written_and_nothing_is_an_error(monkeypatch):
    for name in ("AIOPS_GRAFANA_URL", "AIOPS_GRAFANA_TOKEN", "AIOPS_GRAFANA_USER", "AIOPS_GRAFANA_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    ctx = _alert_round(_incident())
    assert ctx.errors == [] and "grafana_annotations:skip:not-configured" in ctx.audit


def test_the_pipelines_that_should_annotate_do():
    from aiops.interfaces.runner import build_pipeline
    for name in ("grafana_detect", "rca_report", "rca_report_llm"):
        nodes = {n.name: n.operator for n in build_pipeline(name).workflow.nodes}
        assert nodes.get("annotate") == "notifier.grafana_annotations", name


def test_when_a_card_changes_labels_the_annotation_shows_the_new_state_not_the_closing_notice(grafana):
    """實體變多（或升級）時 alert.heartbeat 先送一則舊標籤的結束通知、再送新的：註記要跟著最後一則。
    2026-11-20 在演練叢集看到的：註記停在「[P0] cim-eap, mq-broker 異常：已恢復 7 分鐘，累計 24 筆」，
    實際上那張卡當時有三個實體、86 筆、還在發生。"""
    incident = _incident()
    ctx = PipelineContext(pipeline="grafana_detect", now=incident.end + timedelta(minutes=1))
    ctx.incidents, ctx.incidents_built = [incident], True
    ctx = OperatorRegistry.create("alert.notify_payload", {}).execute(ctx)
    (current,) = ctx.notifications
    stale = type(current)(labels={**current.labels, "entities": "mq-broker"},
                          annotations={**current.annotations, "summary": "[P0] mq-broker 異常：已恢復 7 分鐘，累計 3 筆",
                                       "last_seen": current.annotations["first_seen"]},
                          **{k: getattr(current, k) for k in current.__dataclass_fields__ if k not in ("labels", "annotations")})
    ctx.notifications = [stale, current]
    OperatorRegistry.create("notifier.grafana_annotations", {}).execute(ctx)
    (row,) = grafana.rows.values()
    assert row["text"].startswith("[P0] cim-eap, mq-broker 異常") and "已恢復" not in row["text"]
    assert "entity:cim-eap" in row["tags"] and row["timeEnd"] == int(incident.end.timestamp() * 1000)

