"""送 LLM 之前的敏感資訊遮罩（aiops/stages/enrichment/masking.py）。

分四組：遮得到的形狀、刻意不遮的（RCA 要用）、遮不到的死區（strict xfail，CLAUDE.md §100.4）、
以及五條送 LLM 的路徑（extract_evidence、rca.root_cause.llm、rca_narrator、llm_summary、llm_briefing）實際送出的 prompt。
假供應商只登記在這個測試檔（§100.2）。
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from aiops.engine.context import LogRecord, PipelineContext, ReportSection, RootCandidate
from aiops.engine.rca_types import EvidenceItem, Fact
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import AnomalyEvent, Incident
from aiops.stages.enrichment import masking
from aiops.stages.enrichment.base import BaseLLM
from aiops.stages.enrichment.masking import Masker, MaskError, MaskPolicy, placeholder
from aiops.stages.enrichment.registry import register

NOW = datetime(2026, 11, 9, 12, 30, tzinfo=timezone.utc)


# ── 遮得到的 ──

@pytest.mark.parametrize("text, secret, category", [
    ("notify alice.wang@corp.example.com failed", "alice.wang@corp.example.com", "EMAIL"),
    ("connect from 192.168.10.23 refused", "192.168.10.23", "IP"),
    ("peer 192.168.10.23:5432 reset", "192.168.10.23", "IP"),
    ("Source Network Address: fe80::1ff:fe23:4567:890a%eth0", "fe80::1ff:fe23:4567:890a", "IP"),
    ("client 2001:0db8:85a3:0000:0000:8a2e:0370:7334 timed out", "2001:0db8:85a3:0000:0000:8a2e:0370:7334", "IP"),
    ("Authorization: Bearer abcDEF123456ghiJKL", "abcDEF123456ghiJKL", "TOKEN"),
    ("jwt=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.dBjftJeZ4CVPmB92K27uhbUJU1p1r", "eyJhbGciOiJIUzI1NiJ9", "TOKEN"),
    ("aws key AKIAIOSFODNN7EXAMPLE leaked", "AKIAIOSFODNN7EXAMPLE", "TOKEN"),
    ("password=Hunter2! retry", "Hunter2!", "SECRET"),
    ("pwd: 's3cr3t pass'", "s3cr3t pass", "SECRET"),
    ("?api_key=XyZ987&page=2", "XyZ987", "SECRET"),
    ("client_secret=QWERTY12345", "QWERTY12345", "SECRET"),
    ("jdbc:postgresql://app_user:S3cr3t@db01:5432/orders", "app_user:S3cr3t", "CRED"),
    ("amqp://guest@rabbit:5672", "guest", "CRED"),
    ("Account Name:  jsmith  Account Domain: CORP", "jsmith", "USER"),
    ("Server=db;User Id=sa;Password=P@ss;", "P@ss", "SECRET"),
    ("password authentication failed for user \"report_ro\"", "report_ro", "USER"),
    ("Access denied for user 'app'@'10.0.0.9'", "'app'", "USER"),
    ("CORP\\jsmith opened a handle", "jsmith", "ACCOUNT"),
    ("card 4111 1111 1111 1111 declined", "4111 1111 1111 1111", "CARD"),
    ("card 4111-1111-1111-1111 declined", "4111-1111-1111-1111", "CARD"),
    ("id A123456789 not found", "A123456789", "NATID"),
    ("ssn 123-45-6789 mismatch", "123-45-6789", "NATID"),
    ("sms to +886 912-345-678 failed", "912-345-678", "PHONE"),
    ("-----BEGIN RSA PRIVATE KEY-----\nMIIEow\n-----END RSA PRIVATE KEY-----", "MIIEow", "KEY"),
])
def test_masks_generic_sensitive_shapes(text, secret, category):
    m = Masker()
    out = m.text(text)
    assert secret not in out, out
    assert f"<{category}_" in out and m.counts[category] >= 1


def test_placeholders_are_stable_per_value_and_distinct_across_values():
    """同一個值不論在哪個 Masker（同一輪的抽證據、判根因、寫摘要各自建一個）都對到同一個代號；不同的值不同代號。"""
    a, b = Masker(), Masker()
    x = a.text("from alice@corp.com to bob@corp.com")
    y = b.text("retry alice@corp.com")
    p = placeholder("EMAIL", "alice@corp.com")
    assert x.count(p) == 1 and p in y and placeholder("EMAIL", "bob@corp.com") in x
    assert placeholder("EMAIL", "alice@corp.com") != placeholder("EMAIL", "bob@corp.com")


def test_masking_is_idempotent():
    m = Masker()
    once = m.text("user=jsmith password=x1 alice@corp.com 10.1.2.3 token=abc Bearer abcdefghij")
    assert m.text(once) == once


def test_counts_record_categories_only_never_the_values():
    m = Masker()
    m.text("alice@corp.com 10.1.2.3")
    assert m.summary() == "EMAIL 1、IP 1" and "alice" not in m.summary()


# ── 刻意不遮（RCA 要用）──

@pytest.mark.parametrize("text", [
    "CircuitBreaker 'payment' is OPEN and does not permit further calls",
    "GC(142) Pause Full (Allocation Failure) 3840ms",
    "org.postgresql.util.PSQLException: ERROR: deadlock detected",
    "java.lang.IllegalStateException at com.shop.cart.CartService.add(CartService.java:42)",
    "HTTP 503 after 3000 ms, 12 retries, 87% failed",
    "at 2026-11-09T12:00:04.305+00:00 (12:00:04)",
    "trace 4bf92f3577b34da6a3ce929d0e0e4736 span 00f067aa0ba902b7",
    "upstream tax-gw:443 slow; queue kafka/order-events lag 1200",
    "Foo::bar() threw; auth=ok",
    "inventory-db.prod.internal:5432 connection pool exhausted (max 50)",
])
def test_rca_signals_are_left_alone(text):
    assert Masker().text(text) == text


def test_known_names_shaped_like_addresses_are_kept():
    """APM 的資源名常常就是 IP:port；它是 RCA 要指認的對象，keep 裡的名稱不遮。不在 keep 裡的同形狀照遮。"""
    m = Masker(keep={"10.0.0.5:5432", "payment"})
    out = m.text("wait on 10.0.0.5:5432 3840ms; caller 10.0.0.77")
    assert "10.0.0.5:5432" in out and "10.0.0.77" not in out


def test_keep_does_not_shield_secret_values_next_to_ordinary_names():
    """只保護「自己會被遮到」的名稱：服務叫 auth 不能讓 auth_token=… 的值漏出去。"""
    out = Masker(keep={"auth"}).text("auth_token=Zz9Yy8Xx7 from auth")
    assert "Zz9Yy8Xx7" not in out and out.endswith("from auth")


def test_hostnames_are_kept_by_default_and_masked_only_when_configured():
    text = "call tax-gw.example.com and db01.prod.internal:5432; org.postgresql.util.PSQLException"
    assert Masker().text(text) == text
    out = Masker(MaskPolicy(mask_hostnames=True), keep={"tax-gw.example.com"}).text(text)
    assert "tax-gw.example.com" in out and "db01.prod.internal" not in out and "<HOST_" in out
    assert "org.postgresql.util.PSQLException" in out  # Java 套件名不是主機名


# ── 遮不到的死區（§100.4：留存，不宣稱已去識別化）──

@pytest.mark.xfail(strict=True, reason="自由文字裡的人名沒有固定形狀")
@pytest.mark.parametrize("text, secret", [
    ("王小明 下單失敗，請聯絡他", "王小明"),
    ("contact John Smith about the refund", "John Smith"),
])
def test_dead_zone_personal_names(text, secret):
    assert secret not in Masker().text(text)


@pytest.mark.xfail(strict=True, reason="沒有 user=、沒有引號的帳號：跟一般單字分不開")
def test_dead_zone_bare_account_names():
    assert "jsmith" not in Masker().text("jsmith logged in from the VPN")


@pytest.mark.xfail(strict=True, reason="沒有固定形狀的內部編號：跟一般數字、代碼分不開")
def test_dead_zone_internal_ids():
    assert "EMP004512" not in Masker().text("payroll for EMP004512 rejected")


@pytest.mark.xfail(strict=True, reason="不帶 + 的本地電話：跟一般數字分不開")
def test_dead_zone_local_phone_numbers():
    assert "0912-345-678" not in Masker().text("sms to 0912-345-678 failed")


@pytest.mark.xfail(strict=True, reason="無前綴的隨機 token 跟 trace id 同形狀，遮了會把 trace id 一起遮掉")
def test_dead_zone_unprefixed_random_tokens():
    assert "9f86d081884c7d659a2feaa0c55ad015" not in Masker().text("session 9f86d081884c7d659a2feaa0c55ad015 expired")


# ── 出口：送出前一定遮、遮罩失敗不送 ──

class _Recorder(BaseLLM):
    def __init__(self, reply="{}"):
        self.prompts, self.reply = [], reply

    def complete(self, system, prompt):
        self.prompts.append(prompt)
        return self.reply


def test_send_masks_the_whole_prompt_before_calling_the_provider():
    llm = _Recorder()
    masking.send(llm, "system", "user=jsmith password=hunter2 mail alice@corp.com", Masker())
    assert llm.prompts and not any(s in llm.prompts[0] for s in ("jsmith", "hunter2", "alice@corp.com"))


def test_mask_failure_never_reaches_the_provider(monkeypatch):
    def boom(self, text):
        raise RuntimeError("regex engine exploded")

    monkeypatch.setattr(Masker, "text", boom)
    llm = _Recorder()
    with pytest.raises(MaskError, match="不送出"):
        masking.send(llm, "system", "password=hunter2", Masker())
    assert llm.prompts == []


# ── 五條路徑實際送出的 prompt ──

class _Capture(BaseLLM):
    prompts: list[str] = []  # 每個測試開頭清空（autouse fixture）

    def __init__(self, mode):
        self.mode = mode

    def complete(self, system, prompt):
        _Capture.prompts.append(prompt)
        if self.mode == "quote_masked":  # 從送出去的（遮過的）原文抄 quote
            line = next(l for l in json.loads(prompt.split("日誌原文：\n", 1)[1].split("\n\n請輸出", 1)[0])
                        if l["service"] == "checkout")
            start = line["text"].index("login failed")
            return json.dumps({"evidence": [{"kind": "dependency_unreachable", "entity": "checkout", "confidence": 0.8,
                                             "line": line["id"], "quote": line["text"][start:]}]})
        if self.mode == "quote_raw":  # LLM 不可能看到原文；拿原文當 quote 就是沒有逐字出現在送入的日誌
            return json.dumps({"evidence": [{"kind": "dependency_unreachable", "entity": "checkout", "confidence": 0.8,
                                             "quote": "login failed for alice.wang@corp.example.com"}]})
        if self.mode == "candidates":
            return '{"candidates": [], "note": "證據不足"}'
        if self.mode == "summary":
            return '{"summary": "x", "claims": []}'
        return '{"evidence": []}'


@register("test-mask-capture")
def _capture_factory(model, base_url, api_key):
    return _Capture(model)


@pytest.fixture(autouse=True)
def _reset():
    _Capture.prompts = []


SECRETS = ("alice.wang@corp.example.com", "hunter2", "203.0.113.9", "4111 1111 1111 1111", "jsmith")


def _ctx() -> PipelineContext:
    ctx = PipelineContext(pipeline="rca_report_cognitive", now=NOW)
    ctx.incidents = [Incident(events=[AnomalyEvent(entity=e, metric="latency", ts=NOW - timedelta(minutes=5), score=5.0)
                                      for e in ("checkout", "payment")])]
    ctx.topology = [("checkout", "payment"), ("checkout", "10.0.0.5:5432")]
    t = NOW - timedelta(minutes=10)
    ctx.rca_logs = [
        LogRecord(t, "checkout", "login failed for alice.wang@corp.example.com from 203.0.113.9 (password=hunter2)", "AuthError"),
        LogRecord(t + timedelta(seconds=5), "checkout", "login failed for alice.wang@corp.example.com from 203.0.113.9 (password=hunter2)", "AuthError"),
        LogRecord(t + timedelta(seconds=9), "payment", "charge 4111 1111 1111 1111 timeout waiting 10.0.0.5:5432 after 3000 ms", "Timeout"),
        LogRecord(t + timedelta(seconds=12), "payment", "Account Name: jsmith locked", "SecurityEvent"),
    ]
    ctx.rca_log_window_minutes = (30.0, 60.0)
    return ctx


def _assert_clean(prompt: str):
    leaked = [s for s in SECRETS if s in prompt]
    assert not leaked, leaked


def test_extract_evidence_sends_only_masked_logs_and_keeps_rca_signals():
    ctx = _ctx()
    OperatorRegistry.create("enrichment.extract_evidence", {"provider": "test-mask-capture", "model": "empty"}).execute(ctx)
    [prompt] = _Capture.prompts
    _assert_clean(prompt)
    for keep in ("10.0.0.5:5432", "3000 ms", "AuthError", "checkout", "payment", "login failed"):
        assert keep in prompt, keep
    assert placeholder("EMAIL", "alice.wang@corp.example.com") in prompt
    assert ctx.rca_logs[0].message.startswith("login failed for alice.wang")  # ctx 裡的原文沒被改寫
    assert any(a.startswith("extract_evidence：送出前遮罩（") and "EMAIL" in a for a in ctx.audit)


def test_quote_is_validated_against_the_masked_text_and_stored_masked():
    ctx = OperatorRegistry.create("enrichment.extract_evidence", {"provider": "test-mask-capture", "model": "quote_masked"}).execute(_ctx())
    assert ctx.rca_status == "ok"
    [ev] = ctx.rca_evidence
    assert ev.quote.startswith("login failed for <EMAIL_") and "hunter2" not in ev.quote and "<SECRET_" in ev.quote


def test_a_quote_of_the_unmasked_original_is_rejected():
    ctx = OperatorRegistry.create("enrichment.extract_evidence", {"provider": "test-mask-capture", "model": "quote_raw"}).execute(_ctx())
    assert ctx.rca_status == "undetermined" and "逐字" in ctx.rca_undetermined_reason


def test_extract_evidence_mask_failure_is_undetermined_and_nothing_is_sent(monkeypatch):
    monkeypatch.setattr(Masker, "text", lambda self, t: (_ for _ in ()).throw(RuntimeError("boom")))
    ctx = OperatorRegistry.create("enrichment.extract_evidence", {"provider": "test-mask-capture", "model": "empty"}).execute(_ctx())
    assert ctx.rca_status == "undetermined" and ctx.rca_undetermined_reason.startswith("mask_failed：")
    assert _Capture.prompts == [] and ctx.rca_evidence == []


def test_mask_hostnames_param_is_checked_at_load_time():
    from aiops.engine.errors import WorkflowParamError

    for op in ("enrichment.extract_evidence", "enrichment.rca_narrator", "rca.root_cause.llm",
               "enrichment.llm_summary", "enrichment.llm_briefing"):
        with pytest.raises(WorkflowParamError, match="mask_hostnames"):
            OperatorRegistry.create(op, {"mask_hostnames": "yes"})


def test_root_cause_llm_masks_fact_templates_and_quotes_but_not_resources():
    ctx = _ctx()
    ctx.rca_facts = [
        Fact("log_template_surge", "checkout", 40.0, "ratio", "delta",
             {"template": "login failed for alice.wang@corp.example.com from <IP> (password=hunter2)", "during": 40}),
        Fact("wait_delta", "payment", 900.0, "ms", "incident", {"target": "10.0.0.5:5432", "before": 40.0, "during": 940.0}),
    ]
    ctx.rca_evidence = [EvidenceItem("dependency_unreachable", "payment", quote="charge 4111 1111 1111 1111 timeout")]
    OperatorRegistry.create("rca.root_cause.llm", {"provider": "test-mask-capture", "model": "candidates"}).execute(ctx)
    [prompt] = _Capture.prompts
    _assert_clean(prompt)
    assert "10.0.0.5:5432" in prompt and "900" in prompt and "<CARD_" in prompt
    assert any(a.startswith("root_cause.llm：送出前遮罩（") for a in ctx.audit)


def test_narrator_masks_candidate_reasons_and_undetermined_text():
    ctx = _ctx()
    ctx.rca_root_candidates = [RootCandidate("payment", 0.7, "日誌暴增：login failed for alice.wang@corp.example.com (password=hunter2)")]
    ctx.rca_evidence = [EvidenceItem("dependency_unreachable", "payment", quote="Account Name: jsmith locked")]
    out = OperatorRegistry.create("enrichment.rca_narrator", {"provider": "test-mask-capture", "model": "summary"}).execute(ctx)
    [prompt] = _Capture.prompts
    _assert_clean(prompt)
    assert out.llm_summary.startswith("最可能的根因：payment") and "alice.wang" in out.llm_summary  # 模板不送出，留在本地


def test_legacy_summary_and_briefing_mask_their_prompts():
    ctx = _ctx()
    ctx.rca_sections = [ReportSection("log_patterns", "日誌", "login failed for alice.wang@corp.example.com x40 password=hunter2"),
                        ReportSection("root_cause_candidates", "根因", "1. payment（分數 0.7）")]
    ctx.rca_root_candidates = [RootCandidate("payment", 0.7, "x")]
    OperatorRegistry.create("enrichment.llm_summary", {"provider": "test-mask-capture", "model": "summary"}).execute(ctx)
    ctx.rca_root_note = "Account Name: jsmith from 203.0.113.9"
    OperatorRegistry.create("enrichment.llm_briefing", {"provider": "test-mask-capture", "model": "summary"}).execute(ctx)
    assert len(_Capture.prompts) == 2
    for p in _Capture.prompts:
        _assert_clean(p)


# ── 第六條路徑：控制面的調參回饋（control_plane/learning/tuning.py）──

def test_tuning_feedback_text_is_masked_before_it_reaches_the_llm(monkeypatch):
    from aiops.control_plane.learning import tuning
    from aiops.stages.enrichment import registry

    prompts = []

    class _Capture(BaseLLM):
        name = "capture"

        def complete(self, system, prompt):
            prompts.append(prompt)
            return '{"params": {"threshold": 4.0}, "reason": "太敏感"}'

    monkeypatch.setattr(registry, "_ENTRIES", dict(registry._ENTRIES))
    registry.register("capture_tuning", requires_model=False)(lambda model, base_url, api_key: _Capture())
    op = OperatorRegistry.create("admin.feedback_propose", {"provider": "capture_tuning"})
    fb = {"verdict": "false_positive", "text": "又誤報了，請聯絡 alice.wang@corp.example.com，主機 10.20.30.40 password=Hunter2!"}
    tunable = {"threshold": (3.0, 1.0, 20.0)}
    params, _reason, generated_by, fallback = op._extract(fb, "detector.zscore", tunable, {"threshold": 3.0},
                                                         lambda: ({"threshold": 3.6}, "rule", "auto"))
    assert generated_by == "llm" and fallback is None and params == {"threshold": 4.0}
    sent = prompts[0]
    assert "alice.wang@corp.example.com" not in sent and "10.20.30.40" not in sent and "Hunter2!" not in sent
    assert "detector.zscore" in sent and "false_positive" in sent and "threshold" in sent  # 調參需要的東西還在


def test_tuning_falls_back_to_the_rule_without_sending_when_masking_fails(monkeypatch):
    from aiops.control_plane.learning import tuning  # noqa: F401
    from aiops.stages.enrichment import registry

    called = []

    class _Never(BaseLLM):
        name = "never"

        def complete(self, system, prompt):
            called.append(prompt)
            return "{}"

    def boom(self, text):
        raise RuntimeError("regex engine exploded")

    monkeypatch.setattr(registry, "_ENTRIES", dict(registry._ENTRIES))
    registry.register("never_tuning", requires_model=False)(lambda model, base_url, api_key: _Never())
    monkeypatch.setattr(Masker, "text", boom)
    op = OperatorRegistry.create("admin.feedback_propose", {"provider": "never_tuning"})
    out = op._extract({"verdict": "false_positive", "text": "x"}, "detector.zscore", {"threshold": (3.0, 1.0, 20.0)},
                      {"threshold": 3.0}, lambda: ({"threshold": 3.6}, "rule", "auto"))
    assert not called and out[:3] == ({"threshold": 3.6}, "rule", "auto") and out[3].startswith("mask_failed")
