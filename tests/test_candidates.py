"""根因分析列出 1～3 個可能的原因，每個最後帶信心分數（0～100）。分數是模型給的；程式只做一件事：
沒有直接證據指向那個系統，分數最高只到 30。分數不是機率。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import db as dbmod
from src import rca, report
from src.grafana import Grafana
from tests.conftest import collect_evidence
from src.grouping import Alert, converge
from tests.test_main import rows, world  # noqa: F401（fixture）

T0 = datetime(2026, 11, 21, 2, 41, 10, tzinfo=timezone.utc)
NOW = T0 + timedelta(seconds=64)
EDGES = [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs"), ("erp-core", "mq-broker")]


def incident(*services):
    alerts = [Alert(f"f{i}", "firing", f"r{i}", f"{s} 告警", s, "P2", T0 + timedelta(seconds=i), None, {"A": 1})
              for i, s in enumerate(services or ("mq-broker", "cim-eap", "amhs-mcs"))]
    [inc] = converge([], alerts, [], EDGES, now=NOW, close_after_seconds=300)
    return inc


def evidence(fake, es="es"):
    grafana = Grafana("http://grafana.test", "t", transport=fake.transport, retries=0)
    return collect_evidence(incident(), EDGES, grafana, logs_datasource_uid=es, id_fields=["eqp_id", "lot_id", "carrier_id"], now=NOW)


def answer(*candidates, summary="mq-broker 最可能是源頭"):
    return json.dumps({"summary": summary, "systems": ["mq-broker"], "ids": [],
                       "candidates": [{"source": s, "confidence": c, "reason": r} for s, c, r in candidates]}, ensure_ascii=False)


def test_candidates_are_kept_in_order_with_their_confidence(fake):
    ev = evidence(fake)
    summary, got = rca.read_answer(answer(("mq-broker", 90, "它的錯誤日誌寫 consumer lag"), ("cim-eap", 55, "T3 逾時"),
                                          ("amhs-mcs", 20, "只是下游")), ev)
    assert summary == "mq-broker 最可能是源頭"
    assert [(c["source"], c["confidence"]) for c in got] == [("mq-broker", 90), ("cim-eap", 55), ("amhs-mcs", 20)]
    assert rca.candidates_text(got).splitlines() == ["1. mq-broker：它的錯誤日誌寫 consumer lag（信心 90）",
                                                     "2. cim-eap：T3 逾時（信心 55）", "3. amhs-mcs：只是下游（信心 20）"]


def test_a_system_with_no_direct_evidence_is_capped(fake):
    """erp-core 只出現在關係邊裡（沒有它的日誌、Trace、指標）：模型給 95 也沒用，程式壓到 30。本來就低的不動。"""
    ev = evidence(fake)
    _summary, got = rca.read_answer(answer(("erp-core", 95, "它在上游"), ("mq-broker", 95, "有錯誤日誌")), ev)
    assert [(c["source"], c["confidence"]) for c in got] == [("erp-core", rca.NO_DIRECT_EVIDENCE_CAP), ("mq-broker", 95)]
    assert rca.NO_DIRECT_EVIDENCE_CAP < rca.CONFIDENCE_MEDIUM   # 上限落在「只是推測」那一段
    _summary, low = rca.read_answer(answer(("erp-core", 10, "它在上游")), ev)
    assert low[0]["confidence"] == 10


def test_with_only_alerts_every_candidate_is_low(fake):
    ev = evidence(fake, es="")
    _summary, got = rca.read_answer(answer(("mq-broker", 90, "它的告警最先響"), ("cim-eap", 60, "告警數值偏高")), ev)
    assert [c["confidence"] for c in got] == [30, 30]


def test_a_system_named_only_in_another_systems_log_message_counts_as_direct_evidence(fake):
    ev = evidence(fake)
    ev["error_logs"].append({"time": "2026-11-21T02:41:07Z", "system": "mq-broker", "message": "connection refused erp-core:5432"})
    _summary, got = rca.read_answer(answer(("erp-core", 88, "mq-broker 的錯誤日誌寫連不上它")), ev)
    assert got[0]["confidence"] == 88


@pytest.mark.parametrize("given, score", [
    (85, 85), ("85", 85), ("85%", 85), (85.4, 85), (150, 100), (-5, 0),
    ("", 0), ("很高", 0), (None, 0), ([1], 0),            # 沒給、不是數字：0，不替它猜
    ("高", 85), ("中", 55), ("低", 25),                   # 舊格式換成各段的中間值
])
def test_confidence_is_read_as_a_whole_number_from_0_to_100(fake, given, score):
    _summary, got = rca.read_answer(answer(("mq-broker", given, "x")), evidence(fake))
    assert got[0]["confidence"] == score


def test_at_most_three_candidates_are_kept(fake):
    _summary, got = rca.read_answer(answer(*[("mq-broker", 20, f"理由{i}") for i in range(6)]), evidence(fake))
    assert len(got) == rca.MAX_CANDIDATES == 3


@pytest.mark.parametrize("bad, message", [
    (("nonexistent-svc", 90, "x"), "nonexistent-svc"),           # 編出來的系統：整份退回
    (("mq-broker", 90, "集中在機台 EQP-LITHO-11"), "EQP-LITHO-11"),  # 理由裡編了一台機台
])
def test_invented_names_in_candidates_reject_the_whole_answer(fake, bad, message):
    with pytest.raises(ValueError, match=message):
        rca.read_answer(answer(bad), evidence(fake))


def test_answers_without_candidates_still_work(fake):
    old = json.dumps({"summary": "mq-broker 可能是源頭", "systems": ["mq-broker"], "ids": []}, ensure_ascii=False)
    assert rca.read_answer(old, evidence(fake)) == ("mq-broker 可能是源頭", [])
    assert rca.check_answer(old, evidence(fake)) == "mq-broker 可能是源頭"


def test_annotation_lists_causes_under_the_analysis_line_and_escapes_them():
    inc = incident()
    inc.summary = "[LLM 推論] 最可能是 mq-broker"
    inc.candidates = "1. mq-broker：<b>磁碟滿</b>（信心 90）\n2. cim-eap：只是下游（信心 20）"
    inc.evidence_note = "證據｜已有告警 3 筆；沒接呼叫鏈"
    l1, l2, c1, c2, proof, last = report.render(inc, "http://g", "Asia/Taipei")["text"].split("<br>")
    assert l2 == "根因分析｜[LLM 推論] 最可能是 mq-broker"
    assert c1 == "1. mq-broker：&lt;b&gt;磁碟滿&lt;/b&gt;（信心 90）" and c2 == "2. cim-eap：只是下游（信心 20）"
    assert proof == "證據｜已有告警 3 筆；沒接呼叫鏈" and "台灣時間" in last   # 證據只有一行，放在列點之後


def llm_answering(payload):
    calls = []

    def handle(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": payload}}]})

    return httpx.MockTransport(handle), calls


def test_a_round_stores_the_causes_and_writes_them_to_grafana(world):  # noqa: F811
    w = world
    transport, calls = llm_answering(answer(("mq-broker", 90, "它的錯誤日誌寫 consumer lag exceeded"), ("cim-eap", 20, "下游")))
    w.receive("firing_mq", "firing_cim")
    assert w.run(60, llm=transport)["status"] == "ok"
    [inc] = rows(w, dbmod.incidents)
    assert inc["candidates"].splitlines() == ["1. mq-broker：它的錯誤日誌寫 consumer lag exceeded（信心 90）", "2. cim-eap：下游（信心 20）"]
    note = w.fake.annotations[inc["annotation_id"]]["text"]
    assert note.index("根因分析｜") < note.index("<br>1. mq-broker：") < note.index("<br>2. cim-eap：") < note.index("<br>證據｜已有")
    assert note.count("證據｜") == 1   # 證據只佔一行
    assert "就算只有告警，也要" in calls[0]["messages"][0]["content"]


def test_a_failed_analysis_has_no_causes_and_clears_the_old_ones(world):  # noqa: F811
    w = world
    good, _ = llm_answering(answer(("mq-broker", 90, "它的錯誤日誌寫 consumer lag exceeded")))
    w.receive("firing_mq")
    w.run(60, llm=good)
    assert rows(w, dbmod.incidents)[0]["candidates"]
    w.receive("firing_cim")   # 成員變了：重新分析，這次模型掛了
    w.run(120, llm=httpx.MockTransport(lambda r: httpx.Response(502, text="bad gateway")))
    [inc] = rows(w, dbmod.incidents)
    assert inc["summary"].startswith("[未經分析]") and not inc["candidates"]
    assert "信心" not in w.fake.annotations[inc["annotation_id"]]["text"]


# ── 關係邊是加分的，不是白名單：日誌原文裡指名的元件可以當成可能的原因（現場 AP1105 回報，2026-11-26）──

def field_evidence(*messages):
    """只有一個系統告警、沒有任何關係邊；錯誤日誌的訊息裡提到別的元件。形狀跟 collect_evidence 產生的一樣。"""
    logs = [{"time": f"2026-11-26T04:51:2{n}Z", "system": "instana-agent", "message": m} for n, m in enumerate(messages)]
    patterns = [{"system": "instana-agent", "pattern": rca.log_pattern(m), "example": m, "first_seen": "2026-11-26T04:51:20Z",
                 "during": 3, "before": 0, "new": True} for m in messages]
    return {"anomalies": [{"system": "instana-agent", "rule": "錯誤日誌異常", "severity": "P2", "started": "2026-11-26T04:50:00Z",
                           "status": "firing", "values": {}}],
            "edges": [], "error_logs": logs, "error_patterns": patterns, "warnings": [], "id_counts": {}, "timelines": [],
            "call_chains": [], "metrics": [], "notes": [], "sources": {"logs": "ok"}}


def reply(summary, *candidates, systems=()):
    return json.dumps({"summary": summary, "systems": list(systems), "ids": [],
                       "candidates": [{"source": s, "confidence": c, "reason": r} for s, c, r in candidates]}, ensure_ascii=False)


def test_a_component_named_in_the_log_text_may_be_the_cause_without_any_edge():
    ev = field_evidence("MariaDb | com.instana.sensor-mariadb - 1.1.33 | Cannot connect to MariaDb at db-host:3306")
    summary, candidates = rca.read_answer(reply("最可能是 MariaDB 連不上", ("MariaDB", 82, "日誌寫 Cannot connect to MariaDb"),
                                                 systems=["instana-agent", "MariaDB"]), ev)
    assert [(c["source"], c["confidence"]) for c in candidates] == [("MariaDB", 82)]   # 大小寫跟日誌不同也算；日誌是直接證據，分數不壓
    assert summary == "最可能是 MariaDB 連不上"


def test_names_with_digits_and_ports_are_matched_against_the_original_text_not_the_masked_pattern():
    message = 'group "kubernetes-apps": datasource vmselect-vmstack-victoria-metrics-k8s-stack:8481 returned 503'
    ev = field_evidence(message)
    assert "k*s" in ev["error_patterns"][0]["pattern"]                  # 歸類用的樣板把數字換成 *
    prompt = rca.build_prompt(ev)
    assert "k*s" not in prompt and "pattern" not in prompt and "vmselect-vmstack-victoria-metrics-k8s-stack:8481" in prompt   # 模型只看到原文
    for name in ("vmselect-vmstack-victoria-metrics-k8s-stack", "vmselect-vmstack-victoria-metrics-k8s-stack:8481"):
        _s, candidates = rca.read_answer(reply(f"最可能是 {name} 回 503", (name, 75, "日誌寫它回 503")), ev)
        assert candidates[0]["source"] == name
    with pytest.raises(ValueError, match="k\\*s"):                       # 樣板裡的寫法不是真的名稱：照抄它不算有憑據
        rca.read_answer(reply("vmselect 回 503", ("vmselect-vmstack-victoria-metrics-k*s-stack", 75, "x")), ev)


def test_a_name_that_is_nowhere_in_the_evidence_is_still_refused_when_it_is_the_conclusion():
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    with pytest.raises(ValueError, match="PostgreSQL"):                 # 排第一的可能原因沒有憑據：整份不能用
        rca.read_answer(reply("最可能是資料庫", ("PostgreSQL", 80, "x"), ("MariaDb", 60, "y")), ev)
    with pytest.raises(ValueError, match="PostgreSQL"):                 # 結論的句子提到它、清單也列了它
        rca.read_answer(reply("MariaDb 與 PostgreSQL 都連不上", ("MariaDb", 80, "y"), systems=["MariaDb", "PostgreSQL"]), ev)


def test_an_ungrounded_runner_up_is_dropped_instead_of_throwing_everything_away():
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    summary, candidates = rca.read_answer(reply("最可能是 MariaDb 連不上", ("MariaDb", 80, "日誌寫連不上"), ("PostgreSQL", 40, "猜的"),
                                                 ("instana-agent", 30, "告警在它身上"), systems=["MariaDb", "PostgreSQL"]), ev)
    assert [c["source"] for c in candidates] == ["MariaDb", "instana-agent"]   # 沒憑據的那一個拿掉，其餘照用
    assert "PostgreSQL" not in rca.candidates_text(candidates) and summary.startswith("最可能是 MariaDb")


def test_very_short_names_are_not_matched_by_text_and_known_systems_keep_working():
    ev = field_evidence("db at host:3306 refused")
    with pytest.raises(ValueError, match="db"):                          # 兩個字的名稱在任何文字裡都找得到：不算憑據
        rca.read_answer(reply("db 掛了", ("db", 80, "x")), ev)
    _s, candidates = rca.read_answer(reply("instana-agent 自己的問題", ("instana-agent", 60, "告警在它身上")), ev)
    assert candidates[0]["source"] == "instana-agent"
