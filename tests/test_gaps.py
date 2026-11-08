"""補測試缺口：LLM 供應商的請求組裝與解析（模擬 HTTP），以及 peer／chronic／shift／p0_boost 的邊界與行為。"""
import json
from datetime import datetime, timedelta

import httpx
import numpy as np
import pandas as pd
import pytest

from aiops.infrastructure.errors import InfrastructureError
from aiops.llm import get_llm
from aiops.llm.base import LLMError, parse_json_reply
from aiops.llm.providers import AnthropicLLM, OpenAICompatibleLLM
from aiops.plugins.base import AnomalyEvent, Incident
from aiops.plugins.registry import DetectorRegistry, ScorerRegistry

T0 = datetime(2026, 11, 8)


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


# ── LLM：OpenAI 相容 ──


def test_openai_builds_request_and_parses_reply():
    seen = {}

    def handler(req):
        seen.update(url=str(req.url), auth=req.headers.get("authorization"), body=json.loads(req.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "答案"}}]})

    llm = OpenAICompatibleLLM("k-1", "gpt-x", "http://gw.internal/", client=_client(handler))
    assert llm.complete("系統", "問題") == "答案"
    assert seen["url"] == "http://gw.internal/v1/chat/completions" and seen["auth"] == "Bearer k-1"
    assert seen["body"]["model"] == "gpt-x" and seen["body"]["temperature"] == 0
    assert seen["body"]["messages"] == [{"role": "system", "content": "系統"}, {"role": "user", "content": "問題"}]


def test_openai_gateway_without_key_sends_no_auth_header():
    seen = {}

    def handler(req):
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    OpenAICompatibleLLM(None, "m", "http://gw", client=_client(handler)).complete("s", "p")
    assert seen["auth"] is None


# ── LLM：Anthropic ──


def test_anthropic_builds_request_and_joins_text_blocks():
    seen = {}

    def handler(req):
        seen.update(url=str(req.url), headers=dict(req.headers), body=json.loads(req.content))
        return httpx.Response(200, json={"content": [{"type": "text", "text": "第一段"}, {"type": "tool_use", "id": "x"},
                                                     {"type": "text", "text": "第二段"}]})

    llm = AnthropicLLM("ak", "claude-sonnet-5-5", client=_client(handler))
    assert llm.complete("系統", "問題") == "第一段第二段"  # 非文字區塊忽略
    assert seen["url"] == "https://api.anthropic.com/v1/messages"
    assert seen["headers"]["x-api-key"] == "ak" and seen["headers"]["anthropic-version"] == "2023-06-01"
    assert seen["body"]["system"] == "系統" and seen["body"]["messages"] == [{"role": "user", "content": "問題"}]
    assert seen["body"]["model"] == "claude-sonnet-5-5" and seen["body"]["max_tokens"] > 0


# ── LLM：失敗一律 LLMError（呼叫端會退回模板）──


def _boom(req):
    raise httpx.ConnectError("refused", request=req)


@pytest.mark.parametrize("handler", [
    lambda r: httpx.Response(500, text="oops"),
    lambda r: httpx.Response(429, json={"error": "rate limit"}),
    lambda r: httpx.Response(200, text="not json"),
    lambda r: httpx.Response(200, json={"unexpected": True}),
    _boom,
])
@pytest.mark.parametrize("make", [
    lambda c: OpenAICompatibleLLM("k", "m", "http://gw", client=c),
    lambda c: AnthropicLLM("k", "m", client=c),
])
def test_provider_failures_become_llm_error(handler, make):
    with pytest.raises(LLMError):
        make(_client(handler)).complete("s", "p")


# ── LLM：設定 ──


@pytest.mark.parametrize("env, expected", [
    ({}, None),
    ({"AIOPS_LLM_PROVIDER": "none"}, None),
    ({"AIOPS_LLM_PROVIDER": "openai", "AIOPS_LLM_MODEL": "m"}, OpenAICompatibleLLM),
    ({"AIOPS_LLM_PROVIDER": "anthropic", "AIOPS_LLM_MODEL": "m", "AIOPS_LLM_API_KEY": "k"}, AnthropicLLM),
])
def test_get_llm_picks_provider(monkeypatch, env, expected):
    for k in ("AIOPS_LLM_PROVIDER", "AIOPS_LLM_MODEL", "AIOPS_LLM_API_KEY", "AIOPS_LLM_BASE_URL"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    llm = get_llm()
    assert llm is None if expected is None else isinstance(llm, expected)


@pytest.mark.parametrize("env", [
    {"AIOPS_LLM_PROVIDER": "openai"},  # 沒有模型
    {"AIOPS_LLM_PROVIDER": "anthropic", "AIOPS_LLM_MODEL": "m"},  # 沒有 key
    {"AIOPS_LLM_PROVIDER": "mystery", "AIOPS_LLM_MODEL": "m"},
])
def test_get_llm_misconfiguration_is_infrastructure_error(monkeypatch, env):
    for k in ("AIOPS_LLM_PROVIDER", "AIOPS_LLM_MODEL", "AIOPS_LLM_API_KEY"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(InfrastructureError):
        get_llm()


def test_parse_json_reply_strips_fences_and_rejects_non_objects():
    assert parse_json_reply('```json\n{"a": 1}\n```') == {"a": 1}
    with pytest.raises(LLMError):
        parse_json_reply("[1, 2]")


# ── 偵測器邊界 ──


def _df(values, **cols):
    return pd.DataFrame({"ts": pd.date_range(T0, periods=len(values), freq="60s"), "value": values, **cols})


def _detect(name, train, test=None, **params):
    return DetectorRegistry.get(name)(params).fit(train).detect(test if test is not None else train)


def test_shift_constant_series_has_no_anomalies():
    r = _detect("shift", _df([5.0] * 30))
    assert not any(r.is_anomaly) and set(r.scores) == {0.0}


def test_shift_flags_only_the_jump_point_of_a_level_change():
    noisy = [50 + (i % 3) * 0.5 for i in range(60)]
    step = noisy[:30] + [v + 20 for v in noisy[30:]]
    flagged = [i for i, f in enumerate(_detect("shift", _df(noisy), _df(step)).is_anomaly) if f]
    assert flagged == [30]  # 跳上去之後是新水位，不再報


def test_chronic_window_longer_than_data_reports_nothing():
    r = _detect("chronic", _df([50.0, 51.0, 49.0, 50.0]), _df([80.0] * 10), window=30)
    assert not any(r.is_anomaly)


def test_chronic_flat_baseline_does_not_crash():
    r = _detect("chronic", _df([50.0] * 40), _df([50.0] * 40), window=5)
    assert not any(r.is_anomaly)


def test_chronic_catches_sustained_drop_too():
    base = [50 + (i % 5 - 2) * 0.5 for i in range(120)]
    drop = base[:60] + [v - 1.0 for v in base[60:]]
    assert any(_detect("chronic", _df(base), _df(drop), window=20).is_anomaly)


def test_zscore_respects_threshold_param():
    df = _df([50.0] * 29 + [60.0] + [50.0] * 30)
    assert any(_detect("zscore", df, threshold=3.0).is_anomaly)
    assert not any(_detect("zscore", df, threshold=10.0).is_anomaly)


def test_peer_tolerates_missing_peer_points():
    peers = [50.0] * 60
    peers[10] = np.nan
    df = _df([50.0] * 60, peer_0=peers, peer_1=[50.5] * 60)
    r = _detect("peer", df)
    assert len(r.scores) == 60 and not any(r.is_anomaly)


def test_peer_median_ignores_a_single_misbehaving_peer():
    train = _df([50.0] * 60, peer_0=[50.0] * 60, peer_1=[51.0] * 60, peer_2=[49.0] * 60)
    one_peer_crazy = _df([50.0] * 60, peer_0=[50.0] * 60, peer_1=[90.0] * 60, peer_2=[49.0] * 60)
    assert not any(_detect("peer", train, one_peer_crazy).is_anomaly)


def test_peer_without_peer_columns_fails_loudly():
    with pytest.raises(ValueError):
        DetectorRegistry.get("peer")({}).fit(_df([50.0] * 10))


# ── p0_boost ──


def _inc(*entities, score=3.0):
    return Incident.combine([Incident([AnomalyEvent(e, "cpu", T0 + timedelta(minutes=i), score)], score=score)
                             for i, e in enumerate(entities)])


def test_p0_boost_only_when_an_entity_is_p0():
    scorer = ScorerRegistry.get("p0_boost")()
    assert scorer.score(_inc("a", "b"), {"p0_entities": ["x"]}) == 3.0
    assert scorer.score(_inc("a", "b"), {}) == 3.0
    assert scorer.score(_inc("a", "b"), {"p0_entities": ["b"]}) == 13.0  # 群裡任一個是 P0 就加


def test_p0_boost_custom_amount_and_no_mutation():
    inc = _inc("a")
    before = inc.to_dict()
    assert ScorerRegistry.get("p0_boost")({"boost": 2.5}).score(inc, {"p0_entities": ["a"]}) == 5.5
    assert inc.to_dict() == before
