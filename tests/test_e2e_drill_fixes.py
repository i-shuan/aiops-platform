"""端到端演練（docs/E2E_DRILL.md）抓到的三個問題的回歸測試。

問題 5：重送晚到，故障還在事故卡就結案 -> 出貨的偵測管線 resolve_after_minutes 要蓋得住文件允許的重送間隔。
問題 6：外部告警的事故卡只有一個時間點，RCA 的事件期間沒有 span -> incident_traces 要撈到最後一點那一分鐘的結尾。
看門狗：平台自己的問題最高只到 P2（誤報不能是 P0／P1）。
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from aiops.engine.context import PipelineContext
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.ingestion import fetch

PIPELINES = Path(__file__).resolve().parents[1] / "aiops" / "pipelines"
T = datetime(2026, 11, 16, 3, 4, tzinfo=timezone.utc)

# docs/EXTERNAL_ALERTS.md 第 2 步允許的上限（分鐘）
MAX_REPEAT_INTERVAL, MAX_GROUP_INTERVAL = 15, 5
MINUTE_ROUNDING, DELIVERY = 1, 1


def _nodes(path: Path) -> dict[str, dict]:
    return {n["operator"]: n for n in yaml.safe_load(path.read_text(encoding="utf-8"))["nodes"]}


@pytest.mark.parametrize("path", sorted((PIPELINES / "detection").glob("*.yaml")), ids=lambda p: p.stem)
def test_shipped_pipelines_keep_a_card_open_across_the_longest_documented_resend_gap(path):
    """兩次送達最多隔 repeat interval + group interval；告警點取整到分、再加送達時間。結案門檻比這個短，
    重送還沒到事故卡就先結案（演練：門檻 15、重送隔 16 分鐘，故障還在卡就結案，後面的重送開成第二張）。"""
    nodes = _nodes(path)
    heartbeat = nodes["alert.heartbeat"]["params"]["resolve_after_minutes"]
    absorb = nodes["alert.absorb_open"]["params"]["resolve_after_minutes"]
    assert heartbeat == absorb, "absorb_open 與 heartbeat 的 resolve_after_minutes 要一樣"
    assert heartbeat >= MAX_REPEAT_INTERVAL + MAX_GROUP_INTERVAL + MINUTE_ROUNDING + DELIVERY


class _Spans:
    """假資料源：每 10 秒一筆入口 span，只回查詢區間內的。記下每次查詢的區間。"""

    def __init__(self):
        self.queries = []

    def fetch_spans(self, start, end, services=None, max_spans=20000, **_):
        self.queries.append((start, end))
        first = datetime(2026, 11, 16, 2, 0, tzinfo=timezone.utc)
        out = []
        for i in range(2 * 60 * 6):
            ts = first + timedelta(seconds=10 * i)
            if start <= ts <= end:
                out.append({"trace_id": f"t{i}", "span_id": f"s{i}", "parent_id": None, "service": "wip-worker",
                            "ts": ts, "duration_ms": 10.0, "error": False})
        return out, False


def test_a_single_point_incident_gets_the_spans_of_its_own_minute(monkeypatch):
    """外部告警的事故卡開始＝結束（一個點，代表 03:04 這一分鐘）。事件期間 [03:04, 03:05) 的 span 要撈得到：
    以前只撈到 03:04:00，事件期間剩下剛好落在 03:04:00 的那一筆，根因演算法只能回「入口請求不足」。"""
    spec = _Spans()
    monkeypatch.setattr(fetch, "get_datasource_spec", lambda _t: spec)
    monkeypatch.setattr(fetch, "is_configured", lambda _t: True)
    ctx = PipelineContext(pipeline="rca", now=T + timedelta(minutes=10))
    ctx.incidents = [Incident([AnomalyEvent("wip-worker", "WipWorkerProbeSlow", T, 6.0)])]
    OperatorRegistry.create("ingestion.incident_traces", {"before_minutes": 15, "after_minutes": 0}).execute(ctx)

    assert spec.queries[0] == (T - timedelta(minutes=15), T + timedelta(minutes=1))
    during = [s for s in ctx.rca_spans if T <= s.ts < T + timedelta(minutes=1)]
    assert len(during) == 6  # 每 10 秒一筆，整分鐘都在
