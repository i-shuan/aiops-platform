"""偵測段（回測與測試共用）：偵測管線樣本 scripts/pipelines/sample_it_otel.yaml 的偵測節點——
hourly_baseline＋residual_shift＋residual_chronic 開 snr，各自成立（沒有表決節點，2026-11-20 移除 detector.soft_vote
與旁路 drift_solo）。

以前還有三組對照變體（R、A、A+SNR），用的是 detector.mad／quantile_drift／ensemble，這些算子已移除，變體跟著拿掉；
四組並列的歷史數字在 commit 67a29a1 的 docs/RESIDUAL_SNR_ENSEMBLE.md。
"""
from __future__ import annotations

from typing import Any

import yaml

from aiops.engine.pipeline import NodeSpec
from scripts.sample_pipelines import SAMPLE_DIR

NOT_A_DETECTOR = ("detector.new_only",)  # 名稱是 detector.*，但不學基準線、不判異常（只濾掉舊的異常點）


def detection_specs(workflow: str = "sample_it_otel") -> list[tuple[str, str, dict[str, Any]]]:
    """workflow YAML 的偵測節點（detector.*，不含 new_only），回傳 (節點名, 算子, params)。"""
    raw = yaml.safe_load((SAMPLE_DIR / f"{workflow}.yaml").read_text(encoding="utf-8"))["nodes"]
    return [(n["name"], n["operator"], dict(n.get("params") or {})) for n in raw
            if n["operator"].startswith("detector.") and n["operator"] not in NOT_A_DETECTOR]


def detection_nodes(fetch_operator: str, *, workflow: str = "sample_it_otel") -> list[NodeSpec]:
    """偵測段的 NodeSpec 清單，fetch 換成 fetch_operator（回放、測試用）。"""
    return [NodeSpec(name="fetch", operator=fetch_operator),
            *(NodeSpec(name=name, operator=operator, params=params) for name, operator, params in detection_specs(workflow))]
