"""三大場景、四組 preset：IT（it_otel、it_apm）、工廠（factory）、純指標（metrics_only）。"""
from aiops.commands.base import CommandError
from aiops.presets.base import Preset

_ALL_DETECTORS = ("zscore", "shift", "peer", "rules", "chronic")

# RCA 報告共 8 段；資料不足的段落會自動略過，所以列了不代表一定出現
_IT_SECTIONS = ("summary", "timeline", "affected_entities", "golden_signals",
                "root_cause_candidates", "propagation_path", "trace_evidence", "suggested_actions")
_FACTORY_SECTIONS = ("summary", "timeline", "affected_entities",
                     "root_cause_candidates", "propagation_path", "suggested_actions")
_BASIC_SECTIONS = ("summary", "timeline", "affected_entities", "suggested_actions")

# 有拓撲的場景才做同根因抑制；靜音／排除時段一定排第一
_IT_SUPPRESSORS = ("silence_window", "same_root_cause", "dedup")
_BASIC_SUPPRESSORS = ("silence_window", "dedup")

PRESETS: dict[str, Preset] = {
    p.name: p
    for p in (
        Preset(
            name="it_otel",
            description="IT：OpenTelemetry trace + 指標，有服務拓撲與黃金訊號",
            detectors=_ALL_DETECTORS,
            groupers=("same_entity", "topology_chain"),
            scorers=("golden_signals", "p0_boost"),
            rca_sections=_IT_SECTIONS,
            suppressors=_IT_SUPPRESSORS,
        ),
        Preset(
            name="it_apm",
            description="IT：APM 產品資料，有服務拓撲與黃金訊號",
            detectors=_ALL_DETECTORS,
            groupers=("same_entity", "topology_chain"),
            scorers=("golden_signals", "p0_boost"),
            rca_sections=_IT_SECTIONS,
            suppressors=_IT_SUPPRESSORS,
        ),
        Preset(
            name="factory",
            description="工廠：機台／Lot 指標，沒有 Pod、HTTP 與黃金訊號；拓撲來自 Lot 履歷（lot_edges）",
            detectors=_ALL_DETECTORS,
            groupers=("same_entity", "lot_path_chain"),
            scorers=("p0_boost",),
            rca_sections=_FACTORY_SECTIONS,
            suppressors=_BASIC_SUPPRESSORS,
        ),
        Preset(
            name="metrics_only",
            description="只有時序指標，沒有 trace 與拓撲",
            detectors=_ALL_DETECTORS,
            groupers=("same_entity",),
            scorers=(),
            rca_sections=_BASIC_SECTIONS,
            suppressors=_BASIC_SUPPRESSORS,
        ),
    )
}


def get_preset(name: str) -> Preset:
    if name not in PRESETS:
        raise CommandError(f"未知的 preset: {name}（可用：{', '.join(sorted(PRESETS))}）")
    return PRESETS[name]
