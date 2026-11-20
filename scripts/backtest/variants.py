"""偵測段（回測與測試共用）：偵測管線樣本 scripts/pipelines/sample_it_otel.yaml 的偵測段，也就是 R+SNR——
hourly_baseline＋residual_shift＋residual_chronic 開 snr、detector.soft_vote、旁路 drift_solo。

以前還有三組對照變體（R、A、A+SNR），用的是 detector.mad／quantile_drift／ensemble，這些算子已移除，變體跟著拿掉；
四組並列的歷史數字在 commit 67a29a1 的 docs/RESIDUAL_SNR_ENSEMBLE.md。
"""
from __future__ import annotations

from typing import Any

import yaml

from aiops.engine.pipeline import NodeSpec
from scripts.sample_pipelines import SAMPLE_DIR

VARIANTS = ("R+SNR",)
VOTERS = ("detector.soft_vote",)


def detection_specs(workflow: str = "sample_it_otel", variant: str = "R+SNR") -> list[tuple[str, str, dict[str, Any]]]:
    """workflow YAML 的偵測段（fetch 之後到裁決節點，含），回傳 (節點名, 算子, params)。"""
    if variant not in VARIANTS:
        raise ValueError(f"variant 只能是 {VARIANTS}")
    raw = yaml.safe_load((SAMPLE_DIR / f"{workflow}.yaml").read_text(encoding="utf-8"))["nodes"]
    stop = next(i for i, n in enumerate(raw) if n["operator"] in VOTERS)
    return [(n["name"], n["operator"], dict(n.get("params") or {})) for n in raw[1:stop + 1]]


def detection_nodes(fetch_operator: str, variant: str = "R+SNR", *, workflow: str = "sample_it_otel",
                    keep_members: bool = False) -> list[NodeSpec]:
    """偵測段的 NodeSpec 清單，fetch 換成 fetch_operator（回放、測試用）。keep_members：裁決保留各票事件。"""
    nodes = [NodeSpec(name="fetch", operator=fetch_operator)]
    for name, operator, params in detection_specs(workflow, variant):
        if operator in VOTERS and keep_members:
            params = {**params, "keep_members": True}
        nodes.append(NodeSpec(name=name, operator=operator, params=params))
    return nodes
