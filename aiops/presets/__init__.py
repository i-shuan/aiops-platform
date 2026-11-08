"""三大場景、四組 preset：IT（it_otel、it_apm）、工廠（factory）、純指標（metrics_only）。"""
from aiops.commands.base import CommandError
from aiops.presets.base import Preset

_IT_SECTIONS = ("summary", "timeline", "affected_entities", "golden_signals")
_BASIC_SECTIONS = ("summary", "timeline", "affected_entities")

PRESETS: dict[str, Preset] = {
    p.name: p
    for p in (
        Preset(
            name="it_otel",
            description="IT：OpenTelemetry trace + 指標，有服務拓撲與黃金訊號",
            detectors=("zscore", "shift"),
            groupers=("same_entity", "topology_chain"),
            scorers=("golden_signals", "p0_boost"),
            rca_sections=_IT_SECTIONS,
        ),
        Preset(
            name="it_apm",
            description="IT：APM 產品資料，有服務拓撲與黃金訊號",
            detectors=("zscore", "shift"),
            groupers=("same_entity", "topology_chain"),
            scorers=("golden_signals", "p0_boost"),
            rca_sections=_IT_SECTIONS,
        ),
        Preset(
            name="factory",
            description="工廠：機台／Lot 指標，沒有 Pod、HTTP 與黃金訊號",
            detectors=("zscore", "shift"),
            groupers=("same_entity",),
            scorers=("p0_boost",),
            rca_sections=_BASIC_SECTIONS,
        ),
        Preset(
            name="metrics_only",
            description="只有時序指標，沒有 trace 與拓撲",
            detectors=("zscore", "shift"),
            groupers=("same_entity",),
            scorers=(),
            rca_sections=_BASIC_SECTIONS,
        ),
    )
}


def get_preset(name: str) -> Preset:
    if name not in PRESETS:
        raise CommandError(f"未知的 preset: {name}（可用：{', '.join(sorted(PRESETS))}）")
    return PRESETS[name]
