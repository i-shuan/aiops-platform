"""三行式止血摘要的封閉世界詞表（docs/HARDENING_DESIGN.md 第 3 節）。

守門規則：三行裡每個識別字 token（含英數、可用 - _ . : / @ 連接的片段，例如 payment-gw:443、k8s-node-99、
java.lang.IllegalStateException、RB-01）都要出現在 context 的值裡、或在 narrative.GENERIC_LEXICON 這份放行清單裡，
不分大小寫；不在就退回模板，briefing_rejected 寫出那個 token。數字另由 ±10% 檢查管。

死區（照實寫）：中文自由敘述不抽 token，所以「請重啟資料庫主機」這種不含任何識別字的中文幻覺，封閉詞表擋不到；
見 test_chinese_free_text_hallucination_is_a_dead_zone（xfail strict）。報告靠 narrative.briefing_generated_by 標明來源。
"""
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import LogRecord, PipelineContext, RootCandidate, SpanRecord
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.enrichment import narrative, summarizer
from aiops.stages.enrichment.base import BaseLLM

T0 = datetime(2026, 11, 3, 15, 40, tzinfo=timezone.utc)
TRACE = "dd487b019660078efe3b34680e74e6ed"
ATTR = {"resource": "tax-gw:443", "kind": "external", "subtype": "http", "share": 1.0, "by": "latency",
        "self_ms": {"before": 13.2, "during": 12.6}, "wait_ms": {"before": 13.3, "during": 101.4},
        "failures": {"before": 1, "during": 13}}
# 步驟二驗收時真實 OpenAI 回的三行（原文照抄，不修飾）
REAL_OPENAI = {
    "summary_line": "【一句話根因】bill-svc 的 POST /invoice 失敗，根因指向 tax-gw:443；等待時間由 13.3 ms 升至 101.4 ms，失敗數由 1 升至 13。",
    "action_line": "【立即止血動作】檢查 tax-gw:443 健康狀態與網路延遲，確認 bill-svc 的 HTTP client timeout 與 Max Connections，若持續惡化則評估降級或熔斷。",
    "verification_line": f"【驗證與排除項】追查 trace {TRACE}（POST /invoice，500），確認 tax-gw:443 等待時間及失敗數回落，並觀察 bill-svc 的 java.lang.IllegalStateException 錯誤。",
}


# PR B 複驗時真實 OpenAI（gpt-6-luna，真實 ES 回放 11/3 bill-svc）回的三行：照 prompt 的欄位名寫 trace_id、status_code
REAL_OPENAI_KEYS = {
    "summary_line": "【一句話根因】P0 事件持續 30 分鐘，bill-svc 的增量幾乎全因等待 tax-gw:443，等待時間由 13.3 ms 升至 101.4 ms，失敗由 1 次升至 13 次。",
    "action_line": "【立即止血動作】檢查 tax-gw:443 健康狀態與網路延遲，確認 bill-svc 的 HTTP client timeout 與 Max Connections，若持續惡化則評估降級或熔斷。",
    "verification_line": f"【驗證與排除項】追蹤 POST /invoice 與 trace_id {TRACE}，確認 status_code 500 與 tax-gw:443 失敗是否改善。",
}


def _incident(entity="bill-svc", metric="latency", signal="latency"):
    inc = Incident([AnomalyEvent(entity, metric, T0 + m * timedelta(minutes=1), 6.0, {"signal": signal} if signal else {})
                    for m in range(0, 30, 5)])
    inc.severity = "P0"
    return inc


def _ctx(entity="bill-svc", metric="latency", signal="latency", attr=None, dependency=True, candidate=True,
         sample=False, logs=False, root_note=""):
    ctx = PipelineContext(pipeline="rca_report", now=T0 + timedelta(hours=1))
    ctx.incidents = [_incident(entity, metric, signal)]
    ctx.incidents_built = True
    if candidate:
        a = dict(attr if attr is not None else ATTR)
        dep = f"{a['resource']}（{a['kind']}/{a['subtype']}）" if dependency and a else ""
        ctx.rca_root_candidates = [RootCandidate(entity, 1.0, "自身耗時 26.5→114.0", dep, a)]
    ctx.rca_root_note = root_note
    if sample:
        ctx.rca_spans = [SpanRecord(TRACE, "x", None, entity, T0 + timedelta(minutes=3), 237.0, True, status_code=500, name="POST /invoice"),
                         SpanRecord(TRACE, "w", "x", entity, T0 + timedelta(minutes=3), 223.0, True,
                                    kind="external", subtype="http", resource="tax-gw:443")]
    if logs:
        ctx.rca_logs = [LogRecord(T0 + timedelta(minutes=i), entity, "Invoice state invalid: tax response missing",
                                  "java.lang.IllegalStateException", TRACE) for i in range(13)]
        ctx.rca_log_window_minutes = (30.0, 60.0)
    OperatorRegistry.create("rca.section.suggested_actions", {}).execute(ctx)  # 正式 YAML 的順序：先算 Runbook
    return ctx


def _context(**kw):
    ctx = _ctx(**kw)
    return narrative.build_context(ctx, ctx.incidents[0])


def _real_context():
    return _context(sample=True, logs=True)


# 1. 真實 OpenAI 回文：HTTP client timeout／Max Connections 來自 RB-01 處方、IllegalStateException 來自日誌樣板 → 必須通過
def test_real_openai_briefing_still_passes():
    context = _real_context()
    assert context["matched_runbook"]["id"] == "RB-01"
    assert any("HTTP client timeout 與連線池上限（Max Connections）" in a for a in context["matched_runbook"]["actions"])
    assert context["top_error_logs"][0]["error_type"] == "java.lang.IllegalStateException"
    assert context["sample_trace"]["trace_id"] == TRACE and context["sample_trace"]["endpoint"] == "POST /invoice"
    assert narrative.validate_briefing(REAL_OPENAI, context, {"bill-svc"}) is None


def test_real_openai_briefing_using_context_key_names_passes():
    """context 的鍵名（trace_id、status_code、wait_ms…）是模型在 prompt 裡看得到的詞，不算虛構。
    固定資料的事件群是 25 分鐘（真實那次是 30），30 在 25 的 ±10% 外，所以這裡把時長對齊真實事件群。"""
    context = _real_context()
    context["incident_meta"]["duration_minutes"] = 30
    assert narrative.validate_briefing(REAL_OPENAI_KEYS, context, {"bill-svc"}) is None
    for key in ("trace_id", "status_code", "wait_ms", "failed_resource", "matched_runbook", "top_error_logs"):
        assert key in narrative.briefing_lexicon(context)


def test_identifier_tokens_cover_the_shapes_we_need():
    text = ("payment-gw:443 POST /invoice dd487b019660078efe3b34680e74e6ed k8s-node-99 gpu-farm-prod payment-gateway "
            "java.lang.IllegalStateException RB-01 Kafka，中文敘述不抽。user@host")
    assert narrative._IDENT.findall(text) == [
        "payment-gw:443", "POST", "invoice", "dd487b019660078efe3b34680e74e6ed", "k8s-node-99", "gpu-farm-prod",
        "payment-gateway", "java.lang.IllegalStateException", "RB-01", "Kafka", "user@host"]


# 2. 場景 6 的虛構名稱（不論有沒有數字、在不在舊黑名單）一律擋，原因寫出那個 token
@pytest.mark.parametrize("line, token", [
    ("【立即止血動作】請立即檢查 Kafka 佇列飽和度並重啟 k8s-node-99", "Kafka"),
    ("【立即止血動作】請立即重啟 k8s-node-99 並檢查 tax-gw:443", "k8s-node-99"),
    ("【立即止血動作】請立即重啟 gpu-farm-prod 節點並檢查 tax-gw:443", "gpu-farm-prod"),
    ("【立即止血動作】請檢查 payment-gateway 的設定並確認 bill-svc 逾時", "payment-gateway"),
    ("【立即止血動作】檢查 tax-gw 前面的 Envoy sidecar", "Envoy"),
    ("【立即止血動作】把 bill-svc 的 /checkout 流量切走", "checkout"),
])
def test_identifiers_outside_context_and_lexicon_are_rejected(line, token):
    reason = narrative.validate_briefing({**REAL_OPENAI, "action_line": line}, _real_context(), {"bill-svc"})
    assert reason is not None and "識別字" in reason and token in reason


def test_token_match_ignores_case_both_ways():
    context = _real_context()
    ok = {**REAL_OPENAI, "action_line": "【立即止血動作】檢查 TAX-GW:443 與 Bill-Svc 的 http CLIENT Timeout"}
    assert narrative.validate_briefing(ok, context, {"bill-svc"}) is None
    context["root_cause"]["attribution"]["resource"] = "kafka"
    assert narrative.validate_briefing({**REAL_OPENAI, "action_line": "【立即止血動作】檢查 KAFKA 消費延遲"}, context, set()) is None
    reason = narrative.validate_briefing({**REAL_OPENAI, "action_line": "【立即止血動作】檢查 Redis 連線"}, context, set())
    assert reason == "提到證據與通用詞表裡沒有的識別字：Redis（redis）"


def test_sub_parts_of_context_identifiers_and_units_glued_to_numbers_pass():
    reply = {**REAL_OPENAI, "verification_line": "【驗證與排除項】IllegalStateException 回落、tax-gw 等待回到 13.3ms 以下"}
    assert narrative.validate_briefing(reply, _real_context(), set()) is None
    reply = {**REAL_OPENAI, "verification_line": "【驗證與排除項】tax-gw 等待回到 13.3zz 以下"}
    assert "13.3zz" in narrative.validate_briefing(reply, _real_context(), set())


def test_known_entity_check_and_number_check_still_apply():
    bad_name = {**REAL_OPENAI, "summary_line": "【一句話根因】bill-svc 與 pay-api 一起變慢"}
    assert narrative.validate_briefing(bad_name, _real_context(), {"bill-svc", "pay-api"}) == "提到白名單以外的實體：pay-api"
    bad_num = {**REAL_OPENAI, "verification_line": "【驗證與排除項】tax-gw:443 失敗 150 次"}
    assert narrative.validate_briefing(bad_num, _real_context(), set()) == "數字 150 在證據中找不到（±10%）"


# 3. 誤拒率：模板只用 context 內容，10 組不同 context 一組都不能被擋
DB_ATTR = {"resource": "inventory-db", "kind": "db", "subtype": "postgresql", "share": 0.8, "by": "latency",
           "self_ms": {"before": 5.0, "during": 5.4}, "wait_ms": {"before": 10.2, "during": 88.0}, "failures": {"before": 0, "during": 4}}
WEAK_ATTR = {**ATTR, "share": 0.2, "by": ""}
LONG_NOTE = ("證據不足，不指認根因：" + "幾個服務一起變慢且程度相近，" * 6
             + "可能是 inventory-svc、ord-svc、pay-api 共用的 checkout-db.internal.example:5432")
TEMPLATE_CASES = {
    "歸因＋範例＋日誌": dict(sample=True, logs=True),
    "歸因、沒有範例": dict(),
    "歸因＋範例、沒有日誌": dict(sample=True),
    "資料庫歸因（RB-02）": dict(attr=DB_ATTR),
    "有候選、無主導下游": dict(attr=WEAK_ATTR, dependency=False, sample=True),
    "有候選、沒有資源葉節點": dict(attr={}, dependency=False),
    "錯誤訊號＋失敗範例（RB-03）": dict(signal="errors", attr={}, dependency=False, sample=True, logs=True),
    "證據不足（RB-06）": dict(candidate=False, root_note="證據不足，不指認根因：沒有任何服務的自身耗時明顯上升"),
    "證據不足、長說明被截斷": dict(candidate=False, root_note=LONG_NOTE),
    "工廠機台（RB-F1）": dict(entity="etch-03", metric="rf_power", signal="", candidate=False,
                         root_note="沒有 trace 可下鑽：chamber-A 的 rf_power 偏移"),
}


@pytest.mark.parametrize("label", list(TEMPLATE_CASES))
def test_template_briefings_are_never_rejected(label):
    context = _context(**TEMPLATE_CASES[label])
    briefing = narrative.briefing_template(context)
    assert narrative.validate_briefing(briefing, context, set(context["incident_meta"]["entities"])) is None, briefing


def test_template_cases_cover_distinct_runbooks_and_shapes():
    ids = {label: _context(**kw)["matched_runbook"]["id"] for label, kw in TEMPLATE_CASES.items()}
    assert {"RB-01", "RB-02", "RB-03", "RB-06", "RB-F1"} <= set(ids.values()), ids
    assert len(TEMPLATE_CASES) == 10


def test_clip_does_not_cut_an_identifier_in_half():
    assert narrative._clip("看 inventory-svc 的狀態", 13) == "看"
    assert narrative._clip("看 inventory-svc 的狀態", 15) == "看 inventory-svc"
    assert narrative._clip("短", 10) == "短"


# 4. 攔截時寫一行 HallucinationBlocked 日誌，briefing_rejected 照寫
class _Fake(BaseLLM):
    name = "fake"

    def __init__(self, reply):
        self.reply = reply

    def complete(self, system, prompt):
        return json.dumps(self.reply, ensure_ascii=False)


def test_rejection_is_logged_and_kept_in_briefing_rejected(monkeypatch, caplog):
    bad = {**REAL_OPENAI, "action_line": "【立即止血動作】請立即重啟 gpu-farm-prod 節點"}
    monkeypatch.setattr(summarizer, "build_llm", lambda *a, **k: _Fake(bad))
    ctx = _ctx(sample=True, logs=True)
    with caplog.at_level(logging.WARNING, logger="aiops.enrichment"):
        OperatorRegistry.create("enrichment.llm_briefing", {"provider": "openai"}).execute(ctx)
    assert ctx.llm_briefing_generated_by == "template" and "gpu-farm-prod" in ctx.llm_briefing_rejected
    lines = [r.getMessage() for r in caplog.records if r.name == "aiops.enrichment"]
    assert len(lines) == 1 and lines[0].startswith("HallucinationBlocked") and "gpu-farm-prod" in lines[0]


def test_accepted_real_reply_through_the_operator_logs_nothing(monkeypatch, caplog):
    monkeypatch.setattr(summarizer, "build_llm", lambda *a, **k: _Fake(REAL_OPENAI))
    ctx = _ctx(sample=True, logs=True)
    with caplog.at_level(logging.WARNING, logger="aiops.enrichment"):
        OperatorRegistry.create("enrichment.llm_briefing", {"provider": "openai"}).execute(ctx)
    assert ctx.llm_briefing_generated_by == "llm", ctx.llm_briefing_rejected
    assert not [r for r in caplog.records if r.name == "aiops.enrichment"]


# 5. 死區：不含識別字的中文幻覺擋不到（照實留存，修好了這個測試會變紅提醒更新文件）
@pytest.mark.xfail(strict=True, reason="封閉詞表只看識別字 token；純中文的虛構處置（重啟資料庫主機）沒有 token 可比對")
def test_chinese_free_text_hallucination_is_a_dead_zone():
    reply = {**REAL_OPENAI, "action_line": "【立即止血動作】請重啟資料庫主機並清空快取"}
    assert narrative.validate_briefing(reply, _real_context(), {"bill-svc"}) is not None
