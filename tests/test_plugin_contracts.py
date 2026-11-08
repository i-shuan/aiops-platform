"""每個註冊的外掛都必須遵守契約。新增外掛不必寫新測試，這裡會自動涵蓋。"""
from datetime import datetime, timedelta

import pandas as pd
import pytest

from aiops.initialization import init_plugins
from aiops.plugins.base import AnomalyEvent, AnomalyResult, Incident, ReportSection
from aiops.plugins.registry import DetectorRegistry, GrouperRegistry, ScorerRegistry, SectionRegistry

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


@pytest.mark.parametrize("name", DetectorRegistry.names())
def test_detector_contract(name):
    det = DetectorRegistry.get(name)({})
    with pytest.raises(RuntimeError):
        det.detect(DF)
    assert det.fit(DF) is det
    result = det.detect(DF)
    assert isinstance(result, AnomalyResult)
    assert len(result.is_anomaly) == len(result.scores) == len(DF)


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


@pytest.mark.parametrize("name", SectionRegistry.names())
def test_section_builder_contract(name):
    builder = SectionRegistry.get(name)()
    inc = Incident.combine(_incidents())
    if builder.applies(inc, CTX):
        section = builder.build(inc, CTX)
        assert isinstance(section, ReportSection) and section.key == name and section.body
