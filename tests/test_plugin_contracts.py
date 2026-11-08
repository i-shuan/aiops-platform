"""每個註冊的外掛都必須遵守契約。新增外掛不必寫新測試，這裡會自動涵蓋。"""
from datetime import datetime, timedelta

import pandas as pd
import pytest

from aiops.initialization import init_plugins
from aiops.plugins.base import AnomalyEvent, AnomalyResult, Incident, ReportSection
from aiops.plugins.registry import (
    DetectorRegistry,
    GrouperRegistry,
    ScorerRegistry,
    SectionRegistry,
    SuppressorRegistry,
)

init_plugins()

T0 = datetime(2026, 11, 8)
DF = pd.DataFrame({"ts": pd.date_range(T0, periods=60, freq="60s"), "value": [50.0] * 30 + [90.0] + [50.0] * 29})


def _incidents():
    events = [
        AnomalyEvent("a", "latency", T0, 3.0),
        AnomalyEvent("a", "cpu", T0 + timedelta(minutes=1), 2.0),
        AnomalyEvent("b", "errors", T0 + timedelta(minutes=2), 1.0),
    ]
    return [Incident([e], score=e.score) for e in events]


CTX = {"topology": [["a", "b"]], "p0_entities": ["b"]}


# 需要參數才能偵測的偵測器，契約測試用的範例參數
SAMPLE_PARAMS = {"rules": {"upper": 80}}
# 刻意不對單點尖峰反應的偵測器（chronic 只抓持續偏移，單點會被滾動平均稀釋）
IGNORES_SINGLE_SPIKE = {"chronic"}


def _df_for(name: str) -> pd.DataFrame:
    """需要同儕的偵測器，補兩條平穩的同儕序列。"""
    if DetectorRegistry.get(name).requires_peers:
        return DF.assign(peer_0=50.0, peer_1=51.0)
    return DF


def _detector(name: str):
    return DetectorRegistry.get(name)(SAMPLE_PARAMS.get(name, {}))


@pytest.mark.parametrize("name", DetectorRegistry.names())
def test_detector_contract(name):
    det, df = _detector(name), _df_for(name)
    det.validate_params()
    with pytest.raises(RuntimeError):
        det.detect(df)
    assert det.fit(df) is det
    result = det.detect(df)
    assert isinstance(result, AnomalyResult)
    assert len(result.is_anomaly) == len(result.scores) == len(df)
    assert result.is_anomaly[30] != (name in IGNORES_SINGLE_SPIKE), f"{name} 對第 30 點尖峰的反應不符預期"


@pytest.mark.parametrize("name", DetectorRegistry.names())
def test_detector_state_round_trips_through_json(name):
    """批次訓練的狀態要能存進 DB（JSON）再載回，載回後偵測結果必須一模一樣。"""
    import json

    df = _df_for(name)
    trained = _detector(name).fit(df)
    state = json.loads(json.dumps(trained.get_state()))
    restored = _detector(name).load_state(state)
    assert restored.fitted
    assert restored.detect(df).to_dict() == trained.detect(df).to_dict()


@pytest.mark.parametrize("name", DetectorRegistry.names())
def test_detector_tunable_ranges_are_sane(name):
    """可調參數的預設值要落在範圍內，超出範圍的參數要被 validate_params 擋下。"""
    cls = DetectorRegistry.get(name)
    for key, (default, low, high) in cls.tunable.items():
        assert low <= default <= high, f"{name}.{key}"
        with pytest.raises(ValueError):
            cls({**SAMPLE_PARAMS.get(name, {}), key: high + 1}).validate_params()


@pytest.mark.parametrize("name", GrouperRegistry.names())
def test_grouper_keeps_every_event(name):
    out = GrouperRegistry.get(name)().group(_incidents(), CTX)
    assert all(isinstance(i, Incident) for i in out)
    assert sum(len(i.events) for i in out) == 3


@pytest.mark.parametrize("name", ScorerRegistry.names())
def test_scorer_returns_float_without_mutating(name):
    inc = _incidents()[2]
    before = inc.to_dict()
    assert isinstance(ScorerRegistry.get(name)().score(inc, CTX), float)
    assert inc.to_dict() == before


@pytest.mark.parametrize("name", SuppressorRegistry.names())
def test_suppressor_returns_reason_or_none_without_mutating(name):
    batch = _incidents()
    ctx = {**CTX, "batch": batch, "silences": [], "recent_notifications": []}
    before = [i.to_dict() for i in batch]
    for inc in batch:
        reason = SuppressorRegistry.get(name)().suppress(inc, ctx)
        assert reason is None or (isinstance(reason, str) and reason)
    assert [i.to_dict() for i in batch] == before


@pytest.mark.parametrize("name", SectionRegistry.names())
def test_section_builder_contract(name):
    builder = SectionRegistry.get(name)()
    inc = Incident.combine(_incidents())
    if builder.applies(inc, CTX):
        section = builder.build(inc, CTX)
        assert isinstance(section, ReportSection) and section.key == name and section.body
