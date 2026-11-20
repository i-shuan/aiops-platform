"""把事件群與 RCA 結論標在 Grafana 圖表的時間軸上（notifier.grafana_annotations）。

使用者 2026-11-20 的決定：RCA 結果直接顯示在 Grafana，不做平台自己的網頁。一個事件群一筆註記，固定三行
（幾個實體併成一件、RCA 結論、起訖時間與調整優先權的連結），各自更新、互不覆蓋；寫失敗不影響通知與報告。
同一天改過一次格式：原本的文字是給工程師看的（完整 uid 的標籤、每條規則一個連結、UTC 時間），
改成三行給人看的摘要、時間用 AIOPS_DISPLAY_TZ 的時區。結論等級照實寫：推測就寫推測。
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


LIST = "https://grafana.example.internal/alerting/list?search=label%3Aaiops_detect%3Dgrafana_detect"


@pytest.fixture
def grafana(monkeypatch):
    monkeypatch.setenv("AIOPS_DISPLAY_TZ", "Asia/Taipei")
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
    ctx.incidents, ctx.incident_pipeline = [incident], "grafana_detect"
    ctx.rca_root_algorithm, ctx.rca_root_candidates = "rca.root_cause.evidence", list(candidates)
    ctx.rca_conclusion_type, ctx.rca_conclusion_reason = conclusion, reason
    return OperatorRegistry.create("notifier.grafana_annotations", {}).execute(ctx)


def _lines(row: dict) -> list[str]:
    return row["text"].split("<br>")


def test_a_new_card_becomes_one_three_line_annotation_spanning_the_incident(grafana):
    incident = _incident()
    ctx = _alert_round(incident)
    (row,) = grafana.rows.values()
    assert (row["time"], row["timeEnd"]) == (int(T.timestamp() * 1000), int((T + timedelta(minutes=6)).timestamp() * 1000))
    # 標籤：事件群用前 8 碼（完整的 uid 在浮出視窗上只是一長串看不懂的字）
    assert row["tags"] == sorted(["aiops", "inc:" + incident.uid.removeprefix("inc-")[:8], "severity:P0", "entity:cim-eap", "entity:mq-broker"])
    assert _lines(row) == [
        "[P0 跨系統異常收斂] cim-eap / mq-broker 累計 8 筆異常 → 自動合併為 1 件",
        "RCA：尚未分析",
        f'異常起訖：20:08 ~ 20:14（台灣時間）｜<a href="{LIST}">點此調整告警優先權</a>',   # T 是 12:08 UTC
    ]
    assert ctx.errors == []


def test_a_single_entity_card_does_not_claim_anything_was_merged(grafana):
    one = Incident([AnomalyEvent("mq-broker", "line-mq-consumers", T, 4.0, {}, 0.0, "detector.hourly_baseline", "detect_hourly")], score=4.0)
    _alert_round(one)
    (row,) = grafana.rows.values()
    assert _lines(row)[0] == "[P1 異常] mq-broker 累計 1 筆異常" and "合併" not in row["text"]
    assert _lines(row)[2] == "異常起訖：20:08 ~ 20:08（台灣時間）"            # 事件沒有帶規則連結：沒有調整優先權那一段


def test_times_follow_the_display_timezone_and_default_to_utc(grafana, monkeypatch):
    monkeypatch.delenv("AIOPS_DISPLAY_TZ")
    _alert_round(_incident())
    assert _lines(next(iter(grafana.rows.values())))[2].startswith("異常起訖：12:08 ~ 12:14（UTC）")
    monkeypatch.setenv("AIOPS_DISPLAY_TZ", "Not/AZone")                        # 寫錯的時區：退回 UTC，不讓註記寫不出來
    _alert_round(_incident())
    assert all("（UTC）" in _lines(r)[2] for r in grafana.rows.values())


def test_the_same_card_next_round_updates_its_annotation_instead_of_adding_one(grafana):
    incident = _incident(6)
    _alert_round(incident)
    longer = Incident([*incident.events, AnomalyEvent("mq-broker", "line-mq-consumers", T + timedelta(minutes=20), 4.0, {"severity": "P0"}, 0.0,
                                                    "detector.hourly_baseline", "detect_hourly")], score=9.0, uid=incident.uid)
    _alert_round(longer)
    (row,) = grafana.rows.values()
    assert row["timeEnd"] == int((T + timedelta(minutes=20)).timestamp() * 1000)
    assert "累計 9 筆異常" in _lines(row)[0] and "20:08 ~ 20:28" in _lines(row)[2]
    assert [m for m, _ in grafana.calls].count("POST") == 1 and [m for m, _ in grafana.calls].count("PATCH") == 1


def test_rca_fills_the_middle_line_and_neither_side_overwrites_the_other(grafana):
    incident = _incident()
    _alert_round(incident)
    _rca_round(incident, [RootCandidate("mq-broker", 1.0, "首次異常 2026-11-20T12:08:00+00:00（事件群中最早），最高分數 9.00；上游沒有其他異常")],
               "topology_inference")
    (row,) = grafana.rows.values()
    headline, rca, footer = _lines(row)
    assert headline.startswith("[P0 跨系統異常收斂]") and footer.startswith("異常起訖：20:08 ~ 20:14")
    # 結論等級照實寫（推測）；理由裡的 UTC 時間換成顯示時區；同一張卡的其他實體列為連帶影響
    assert rca == ("RCA 根因（依關係圖推測）：<b>mq-broker</b>（首次異常 20:08（事件群中最早），最高分數 9.00；上游沒有其他異常"
                   "；連帶影響 cim-eap）")
    assert row["timeEnd"] == int((T + timedelta(minutes=6)).timestamp() * 1000)   # RCA 不動時間範圍
    _alert_round(incident)                                                        # 下一輪偵測更新第一、三行
    assert _lines(grafana.rows[1])[1] == rca                                      # RCA 那一行還在
    _rca_round(incident, [RootCandidate("cim-eap", 1.0, "重跑後的結論")], "confirmed")
    again = _lines(grafana.rows[1])
    assert again[1] == "RCA 根因（有直接證據）：<b>cim-eap</b>（重跑後的結論；連帶影響 mq-broker）" and again[0] == headline
    assert len(grafana.rows) == 1


def test_rca_on_a_card_without_an_annotation_writes_all_three_lines_from_the_incident(grafana):
    incident = _incident()
    _rca_round(incident, [], "undetermined", "證據不足，不指認根因：入口請求不足")
    (row,) = grafana.rows.values()
    assert _lines(row) == [
        f"[{incident.severity} 跨系統異常收斂] cim-eap / mq-broker 累計 8 筆異常 → 自動合併為 1 件",
        "RCA：無法判定（證據不足，不指認根因：入口請求不足）",
        f'異常起訖：20:08 ~ 20:14（台灣時間）｜<a href="{LIST}">點此調整告警優先權</a>',
    ]


def test_text_from_data_is_escaped_and_cannot_break_the_three_lines(grafana):
    incident = _incident()
    _rca_round(incident, [RootCandidate("svc<script>", 1.0, 'reason "<img src=x>" a<br>b')], "confirmed")
    row = next(iter(grafana.rows.values()))
    assert "<script>" not in row["text"] and "<img" not in row["text"] and "svc&lt;script&gt;" in row["text"]
    assert len(_lines(row)) == 3


def test_an_annotation_written_in_another_format_is_replaced_not_half_reused(grafana):
    incident = _incident()
    grafana.rows[1] = {"id": 1, "time": 0, "timeEnd": 0, "text": "舊格式的一段文字<br>── RCA ──<br>根因<br>更多行",
                       "tags": ["aiops", "inc:" + incident.uid.removeprefix("inc-")[:8]]}
    _alert_round(incident)
    assert _lines(grafana.rows[1])[1] == "RCA：尚未分析" and len(_lines(grafana.rows[1])) == 3


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
    2026-11-20 在演練叢集看到的：註記停在升級前的兩個實體、24 筆、「已恢復」，實際上那張卡當時三個實體、86 筆、還在發生。"""
    incident = _incident()
    ctx = PipelineContext(pipeline="grafana_detect", now=incident.end + timedelta(minutes=1))
    ctx.incidents, ctx.incidents_built = [incident], True
    ctx = OperatorRegistry.create("alert.notify_payload", {}).execute(ctx)
    (current,) = ctx.notifications
    stale = type(current)(labels={**current.labels, "entities": "mq-broker"},
                          annotations={**current.annotations, "event_count": "3", "last_seen": current.annotations["first_seen"]},
                          **{k: getattr(current, k) for k in current.__dataclass_fields__ if k not in ("labels", "annotations")})
    ctx.notifications = [stale, current]
    OperatorRegistry.create("notifier.grafana_annotations", {}).execute(ctx)
    (row,) = grafana.rows.values()
    assert _lines(row)[0] == "[P0 跨系統異常收斂] cim-eap / mq-broker 累計 8 筆異常 → 自動合併為 1 件"
    assert "entity:cim-eap" in row["tags"] and row["timeEnd"] == int(incident.end.timestamp() * 1000)
