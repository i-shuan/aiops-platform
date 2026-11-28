"""根因分析寫成一條因果鏈（由原因到現象）加上最多兩個別的解釋，每一步最後帶依據等級：直接證據／間接證據／推測。
舊格式的 candidates（自訂提示詞可能還在用）：第一個當成鏈的唯一一步，其餘當成別的解釋。這個檔案前半用舊格式測把關，後半測因果鏈。
原因是模型列的；**等級是程式照證據判定的**（gather.evidence_grade），同一份證據永遠得到同一個等級。模型不給分數：
它自評的分數同一份證據會飄（實測同一題給過 75、38、35），2026-11-28 起拿掉。模型給的 confidence 欄位一律不看。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src import evidence as gather
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


def test_candidates_get_the_grade_the_evidence_supports_whatever_score_the_model_sends(fake):
    """錄下來的樣本：三個系統各有事故前沒出現過的錯誤（new=true）→ 直接證據。模型給的分數（90、55、20）不影響任何東西。"""
    ev = evidence(fake)
    summary, got = rca.read_answer(answer(("mq-broker", 90, "它的錯誤日誌寫 consumer lag"), ("cim-eap", 55, "T3 逾時"),
                                          ("amhs-mcs", 20, "只是下游")), ev)
    assert summary == "mq-broker 最可能是源頭"
    assert [(c["source"], c["grade"]) for c in got] == [("mq-broker", "direct"), ("cim-eap", "direct"), ("amhs-mcs", "direct")]
    assert all("confidence" not in c for c in got)
    assert [c["role"] for c in got] == ["chain", "alternative", "alternative"]     # 舊格式：第一個是鏈，其餘是別的解釋
    assert report.chain_text(got).splitlines() == ["經過｜mq-broker：它的錯誤日誌寫 consumer lag（直接證據）",
                                                   "另一種可能｜cim-eap：T3 逾時（直接證據）", "另一種可能｜amhs-mcs：只是下游（直接證據）"]


def test_a_system_known_only_from_a_relation_is_a_guess_even_as_the_first_step(fake):
    """erp-core 只出現在關係邊裡（沒有它的日誌、Trace、指標）：模型給 95、放在鏈的第一步也沒用，等級是推測。
    鏈的順序是因果順序，程式不重排；別的解釋才照等級排。"""
    ev = evidence(fake)
    _summary, got = rca.read_answer(answer(("erp-core", 95, "它在上游"), ("amhs-mcs", 1, "只是下游"), ("mq-broker", 95, "有錯誤日誌")), ev)
    assert [(c["source"], c["role"], c["grade"]) for c in got] == [("erp-core", "chain", "inference"), ("amhs-mcs", "alternative", "direct"),
                                                                    ("mq-broker", "alternative", "direct")]
    assert report.chain_text(got).splitlines()[0] == "經過｜erp-core：它在上游（推測）"


def test_with_only_alerts_every_candidate_is_a_guess(fake):
    ev = evidence(fake, es="")
    _summary, got = rca.read_answer(answer(("mq-broker", 90, "它的告警最先響"), ("cim-eap", 60, "告警數值偏高")), ev)
    assert [c["grade"] for c in got] == ["inference", "inference"]


def test_a_system_named_in_another_systems_error_log_is_direct_evidence(fake):
    ev = evidence(fake)
    ev["error_logs"].append({"time": "2026-11-21T02:41:07Z", "system": "mq-broker", "message": "connection refused erp-core:5432"})
    _summary, got = rca.read_answer(answer(("erp-core", 10, "mq-broker 的錯誤日誌寫連不上它")), ev)
    assert got[0]["grade"] == "direct"


def graded(**packs):
    base = {"anomalies": [{"system": "api"}], "edges": [{"upstream": "maindb", "downstream": "api"}], "error_logs": [], "error_patterns": [],
            "warnings": [], "timelines": [], "call_chains": [], "metrics": [], "id_counts": {}}
    return {**base, **packs}


@pytest.mark.parametrize("packs, name, grade", [
    ({}, "api", "inference"),                                                                    # 只有告警
    ({}, "maindb", "inference"),                                                                     # 只有關係邊
    ({"error_patterns": [{"system": "api", "example": "boom", "during": 3, "before": 0, "new": True}]}, "api", "direct"),
    ({"error_patterns": [{"system": "api", "example": "boom", "during": 3, "before": 9, "new": False}]}, "api", "indirect"),   # 平常就有的錯誤
    ({"error_patterns": [{"system": "api", "example": "boom"}]}, "api", "indirect"),              # 數不出次數：不知道是不是新的
    ({"warnings": [{"system": "api", "example": "pool 92%", "during": 2, "before": 0, "new": True}]}, "api", "indirect"),      # 只有警告
    ({"metrics": [{"system": "maindb", "metric": "connections"}]}, "maindb", "indirect"),                 # 只有指標變化
    ({"error_logs": [{"system": "api", "message": "connect to maindb:5432 refused"}]}, "maindb", "direct"),   # 別的系統的錯誤指名它
    ({"error_logs": [{"system": "maindb", "message": "maindb is starting"}]}, "maindb", "indirect"),          # 自己提到自己不算被指名
    ({"error_logs": [{"system": "api", "message": "connect to maindb-replica refused"}]}, "maindb", "indirect"),   # 名稱只是別的字的一部分：不算指名，但字面出現在日誌裡
    ({"call_chains": [{"spans": [{"service": "api", "status": "error", "origin": True, "attributes": {"peer.service": "maindb"}}]}]}, "maindb", "direct"),
    ({"call_chains": [{"spans": [{"service": "maindb", "status": "error", "origin": True}]}]}, "maindb", "direct"),
    ({"call_chains": [{"spans": [{"service": "maindb", "status": "ok"}, {"service": "api", "status": "error", "origin": True}]}]}, "maindb", "indirect"),
    ({"error_logs": [{"system": "api", "message": "connect to db refused"}], "edges": [{"upstream": "db", "downstream": "api"}]}, "db", "indirect"),   # 名稱太短（少於 3 個字）不用文字比對指名
])
def test_the_grade_follows_fixed_rules_about_what_kind_of_evidence_points_at_the_system(packs, name, grade):
    ev = graded(**packs)
    assert gather.evidence_grade(name, ev) == grade
    assert gather.evidence_grade(name.upper(), ev) == grade                 # 不分大小寫
    assert gather.evidence_grade(name, ev) == gather.evidence_grade(name, ev)   # 同一份證據，同一個等級


def test_two_systems_blaming_each_other_are_both_direct_the_grade_cannot_say_who_was_first():
    """已知限制：等級評的是證據，不是推理。互相指名時兩邊都是直接證據，誰先誰後要看模型的文字。"""
    ev = graded(anomalies=[{"system": "a-svc"}, {"system": "b-svc"}], edges=[], error_logs=[
        {"system": "a-svc", "message": "no reply from b-svc"}, {"system": "b-svc", "message": "no reply from a-svc"}])
    assert (gather.evidence_grade("a-svc", ev), gather.evidence_grade("b-svc", ev)) == ("direct", "direct")


def test_the_chain_and_the_alternatives_are_limited(fake):
    _summary, got = rca.read_answer(answer(*[("mq-broker", 20, f"理由{i}") for i in range(6)]), evidence(fake))
    assert [c["role"] for c in got] == ["chain"] + ["alternative"] * rca.MAX_ALTERNATIVES            # 舊格式：一步＋兩個別的解釋
    long = json.dumps({"summary": "x", "systems": [], "ids": [], "chain": [{"system": "mq-broker", "what": f"第{i}步"} for i in range(9)],
                       "alternatives": [{"system": "cim-eap", "what": f"別的{i}"} for i in range(9)]}, ensure_ascii=False)
    _summary, got = rca.read_answer(long, evidence(fake))
    assert [c["role"] for c in got] == ["chain"] * rca.MAX_CHAIN + ["alternative"] * rca.MAX_ALTERNATIVES and (rca.MAX_CHAIN, rca.MAX_ALTERNATIVES) == (4, 2)


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
    inc.candidates = "經過｜mq-broker：<b>磁碟滿</b>（直接證據）\n↳ cim-eap：只是下游（推測）"
    inc.evidence_note = "證據｜已有告警 3 筆；沒接呼叫鏈"
    l1, l2, c1, c2, proof, last = report.render(inc, "http://g", "Asia/Taipei")["text"].split("<br>")
    assert l2 == "根因分析｜[LLM 推論] 最可能是 mq-broker"
    assert c1 == "經過｜mq-broker：&lt;b&gt;磁碟滿&lt;/b&gt;（直接證據）" and c2 == "↳ cim-eap：只是下游（推測）"
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
    assert inc["candidates"].splitlines() == ["經過｜mq-broker：它的錯誤日誌寫 consumer lag exceeded（直接證據）", "另一種可能｜cim-eap：下游（直接證據）"]
    note = w.fake.annotations[inc["annotation_id"]]["text"]
    assert note.index("根因分析｜") < note.index("<br>經過｜mq-broker：") < note.index("<br>另一種可能｜cim-eap：") < note.index("<br>證據｜已有")
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
    note = w.fake.annotations[inc["annotation_id"]]["text"]
    assert "（直接證據）" not in note and "（推測）" not in note and "信心" not in note


# ── 關係邊是加分的，不是白名單：日誌原文裡指名的元件可以當成可能的原因（現場 AP1105 回報，2026-11-26）──

def field_evidence(*messages):
    """只有一個系統告警、沒有任何關係邊；錯誤日誌的訊息裡提到別的元件。形狀跟 collect_evidence 產生的一樣。"""
    logs = [{"time": f"2026-11-26T04:51:2{n}Z", "system": "instana-agent", "message": m} for n, m in enumerate(messages)]
    patterns = [{"system": "instana-agent", "pattern": gather.log_pattern(m), "example": m, "first_seen": "2026-11-26T04:51:20Z",
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
    assert [(c["source"], c["grade"]) for c in candidates] == [("MariaDB", "direct")]   # 大小寫跟日誌不同也算；被別的系統的錯誤指名＝直接證據
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
    assert "PostgreSQL" not in report.chain_text(candidates) and summary.startswith("最可能是 MariaDb")


def test_very_short_names_are_not_matched_by_text_and_known_systems_keep_working():
    ev = field_evidence("db at host:3306 refused")
    with pytest.raises(ValueError, match="db"):                          # 兩個字的名稱在任何文字裡都找得到：不算憑據
        rca.read_answer(reply("db 掛了", ("db", 80, "x")), ev)
    _s, candidates = rca.read_answer(reply("instana-agent 自己的問題", ("instana-agent", 60, "告警在它身上")), ev)
    assert candidates[0]["source"] == "instana-agent"


# ── 推測可以列，但要標出來（使用者的決定，2026-11-26）──

def test_a_cause_the_model_marks_as_inferred_is_a_guess_whatever_evidence_the_system_has():
    """模型自己標成 inference 的一律是推測：就算那個系統有直接證據（instana-agent 有新的錯誤），這一條講的機制是猜的。
    只會往下調；模型標 evidence 不會讓等級變高（db-host 只是字面出現在日誌裡 → 間接證據）。"""
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    raw = json.dumps({"summary": "instana-agent 的感測器連不上 MariaDb，所以一直報錯", "systems": [], "ids": [], "candidates": [
        {"source": "instana-agent", "kind": "inference", "confidence": 85, "reason": "也可能是感測器的帳密設定錯了，日誌沒有寫"},
        {"source": "MariaDb", "kind": "evidence", "confidence": 20, "reason": "日誌直接寫 Cannot connect to MariaDb"},
        {"source": "db-host", "kind": "evidence", "confidence": 99, "reason": "主機可能關機了"}]}, ensure_ascii=False)
    _summary, candidates = rca.read_answer(raw, ev)
    # 舊格式：第一個（instana-agent，模型標成推測）是鏈；其餘是別的解釋，照等級排
    assert [(c["source"], c["role"], c["grade"]) for c in candidates] == [
        ("instana-agent", "chain", "inference"), ("MariaDb", "alternative", "direct"), ("db-host", "alternative", "direct")]
    assert report.chain_text(candidates).splitlines() == [
        "經過｜instana-agent：也可能是感測器的帳密設定錯了，日誌沒有寫（推測）",
        "另一種可能｜MariaDb：日誌直接寫 Cannot connect to MariaDb（直接證據）",
        "另一種可能｜db-host：主機可能關機了（直接證據）"]


def test_answers_without_the_kind_field_keep_working_and_the_prompt_asks_for_why():
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    _s, candidates = rca.read_answer(reply("連不上 MariaDb", ("MariaDb", 80, "日誌寫的")), ev)   # 自訂提示詞可能沒有 kind
    assert candidates == [{"source": "MariaDb", "role": "chain", "grade": "direct", "reason": "日誌寫的"}]
    for part in ("為什麼會發生", "kind", "inference", "不要給分數", "由程式照證據判定", "不要只寫哪個系統是源頭", "有多少證據就分析多少"):
        assert part in rca.SYSTEM_PROMPT
    assert "confidence" not in rca.SYSTEM_PROMPT and "信心" not in rca.SYSTEM_PROMPT


# ── 下一步建議：LLM 寫給值班人員看的文字，程式不會執行（使用者要求，2026-11-27）──

def with_actions(*actions):
    return json.dumps({"summary": "instana-agent 的感測器連不上 MariaDb", "systems": [], "ids": [], "actions": list(actions),
                       "candidates": [{"source": "MariaDb", "kind": "evidence", "confidence": 80, "reason": "日誌寫 Cannot connect to MariaDb"}]},
                      ensure_ascii=False)


def test_next_steps_are_listed_cut_and_limited():
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    steps = rca.read_actions(with_actions("確認 db-host:3306 的 MariaDb 有沒有在聽連線", "  檢查 instana-agent 的\n感測器帳密  ", "x" * 500, "第四條"), ev)
    assert steps[:2] == ["確認 db-host:3306 的 MariaDb 有沒有在聽連線", "檢查 instana-agent 的 感測器帳密"]
    assert len(steps) == rca.MAX_ACTIONS and len(steps[2]) == rca.ACTION_CHARS
    assert report.actions_text(steps[:2]) == "下一步建議｜1) 確認 db-host:3306 的 MariaDb 有沒有在聽連線；2) 檢查 instana-agent 的 感測器帳密"
    assert report.actions_text([]) == ""
    for missing in (json.dumps({"summary": "x"}), with_actions(), json.dumps({"summary": "x", "actions": "重啟"}), with_actions(7, None, "")):
        assert rca.read_actions(missing, ev) == []                       # 選填：沒有、不是清單、不是文字，都當成沒有


def test_a_step_that_names_equipment_not_in_the_evidence_is_dropped_but_the_analysis_stays():
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    answer = with_actions("重啟機台 EQP-LITHO-11", "確認 db-host:3306 的連線")
    assert rca.read_actions(answer, ev) == ["確認 db-host:3306 的連線"]   # 編出來的機台：那一條拿掉
    summary, candidates = rca.read_answer(answer, ev)
    assert summary and candidates                                        # 根因分析不受影響
    assert rca.read_actions(answer, ev, id_pattern="") == ["重啟機台 EQP-LITHO-11", "確認 db-host:3306 的連線"]   # 關掉樣式檢查就不擋


def test_the_prompt_asks_for_concrete_reasons_and_concrete_next_steps():
    for part in ("actions", "下一步建議", "它怎麼造成下一步", "證據為什麼比較不支持它", "不會被自動執行", "對任何事故都成立的話"):
        assert part in rca.SYSTEM_PROMPT


# ── 因果鏈：由原因到現象，兩端共同構成的問題不用硬選一個源頭（2026-11-28）──

def chain_answer(summary, chain, alternatives=()):
    step = lambda s: {"system": s[0], "kind": s[2] if len(s) > 2 else "evidence", "what": s[1]}
    return json.dumps({"summary": summary, "systems": [], "ids": [], "chain": [step(s) for s in chain],
                       "alternatives": [step(s) for s in alternatives]}, ensure_ascii=False)


def test_a_chain_keeps_its_causal_order_and_each_step_gets_its_own_grade():
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    summary, steps = rca.read_answer(chain_answer(
        "MariaDb 連不上，instana-agent 的感測器啟用失敗",
        [("MariaDb", "db-host:3306 的 MariaDb 不接受連線"), ("instana-agent", "感測器連不上資料庫，一直重試並報錯")],
        [("instana-agent", "也可能是感測器的帳密設定錯了", "inference")]), ev)
    assert [(s["source"], s["role"], s["grade"]) for s in steps] == [
        ("MariaDb", "chain", "direct"), ("instana-agent", "chain", "direct"), ("instana-agent", "alternative", "inference")]
    assert report.chain_text(steps).splitlines() == [
        "經過｜MariaDb：db-host:3306 的 MariaDb 不接受連線（直接證據）",
        "↳ instana-agent：感測器連不上資料庫，一直重試並報錯（直接證據）",
        "另一種可能｜instana-agent：也可能是感測器的帳密設定錯了（推測）"]
    # 順序是模型說的因果順序：反過來寫，程式照樣不重排（等級高的不會被搬到前面）
    _s, reversed_steps = rca.read_answer(chain_answer("x", [("instana-agent", "先出事", "inference"), ("MariaDb", "後出事")]), ev)
    assert [(s["source"], s["grade"]) for s in reversed_steps] == [("instana-agent", "inference"), ("MariaDb", "direct")]


def test_an_invented_first_step_rejects_the_answer_and_an_invented_later_step_is_dropped():
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    with pytest.raises(ValueError, match="PostgreSQL"):                 # 鏈的第一步是結論說的源頭：沒憑據整份不採用
        rca.read_answer(chain_answer("資料庫掛了", [("PostgreSQL", "x"), ("instana-agent", "y")]), ev)
    _s, steps = rca.read_answer(chain_answer("MariaDb 連不上", [("MariaDb", "連不上"), ("kafka-proxy", "編的"), ("instana-agent", "報錯")],
                                             [("redis-cache", "也是編的")]), ev)
    assert [(s["source"], s["role"]) for s in steps] == [("MariaDb", "chain"), ("instana-agent", "chain")]   # 中間與別的解釋裡編的拿掉
    with pytest.raises(ValueError, match="EQP-LITHO-11"):               # 任何一步的文字裡編了機台：整份不採用
        rca.read_answer(chain_answer("MariaDb 連不上", [("MariaDb", "連不上"), ("instana-agent", "影響機台 EQP-LITHO-11")]), ev)
    with pytest.raises(ValueError, match="system"):
        rca.read_answer(json.dumps({"summary": "x", "chain": [{"what": "沒有寫是誰"}]}), ev)


def test_an_empty_or_missing_chain_is_allowed_and_the_prompt_asks_for_a_chain():
    ev = field_evidence("Cannot connect to MariaDb at db-host:3306")
    assert rca.read_answer(json.dumps({"summary": "目前只能推測", "chain": [], "alternatives": []}, ensure_ascii=False), ev) == ("目前只能推測", [])
    assert report.chain_text([]) == ""
    for part in ("chain", "由**原因到現象**排", "兩個都寫進鏈裡", "不用硬選一個當唯一的源頭", "alternatives", "不要硬湊成一條鏈"):
        assert part in rca.SYSTEM_PROMPT
    assert "candidates" not in rca.SYSTEM_PROMPT
