"""偵測段的對照變體（回測與測試共用）。現行設定 R+SNR 就是偵測管線樣本 scripts/pipelines/sample_it_otel.yaml 的偵測段
（出貨的 it_otel 現在只吃外部告警、不跑偵測器）；另外三組由它改出來，
對應的管線 YAML 已刪除（四組並列的歷史數字在 commit 67a29a1 的 docs/RESIDUAL_SNR_ENSEMBLE.md）：

  R+SNR  it_otel 原樣：hourly_baseline＋residual_shift＋residual_chronic 開 snr、detector.soft_vote、旁路 hard_limit／drift_solo
  R      同上但不開 snr、裁決換回 detector.ensemble 多數決（67a29a1 的 it_otel_residual）
  A      hourly_baseline＋mad＋quantile_drift、detector.ensemble 多數決、旁路只有 hard_limit（2699bdf／tag 1.1 的 it_otel）
  A+SNR  A 的投票節點開 snr＋detector.soft_vote（67a29a1 的 it_otel_snr）
"""
from __future__ import annotations

from typing import Any

import yaml

from aiops.engine.pipeline import NodeSpec
from scripts.sample_pipelines import SAMPLE_DIR

VARIANTS = ("R+SNR", "R", "A", "A+SNR")
VOTERS = ("detector.ensemble", "detector.soft_vote")
_DEFAULT_SNR = {"p_fa": 0.001, "tz": "Asia/Taipei"}


def detection_specs(workflow: str = "sample_it_otel", variant: str = "R+SNR") -> list[tuple[str, str, dict[str, Any]]]:
    """workflow YAML 的偵測段（fetch 之後到裁決節點，含）改成 variant，回傳 (節點名, 算子, params)。"""
    if variant not in VARIANTS:
        raise ValueError(f"variant 只能是 {VARIANTS}")
    raw = yaml.safe_load((SAMPLE_DIR / f"{workflow}.yaml").read_text(encoding="utf-8"))["nodes"]
    stop = next(i for i, n in enumerate(raw) if n["operator"] in VOTERS)
    specs = [(n["name"], n["operator"], dict(n.get("params") or {})) for n in raw[1:stop + 1]]
    if variant == "R+SNR":
        return specs
    with_snr = variant.endswith("+SNR")
    snr = next((p["snr"] for _, _, p in specs if "snr" in p), _DEFAULT_SNR)
    out: list[tuple[str, str, dict[str, Any]]] = []
    for name, operator, params in specs:
        params = {k: v for k, v in params.items() if k != "snr"}
        if variant.startswith("A"):
            if operator == "detector.residual_shift":
                name, operator, params = "detect_mad", "detector.mad", {}
            elif operator == "detector.residual_chronic" and params.get("min_run", 1) > 1:
                continue  # A 沒有旁路漂移票
            elif operator == "detector.residual_chronic":
                name, operator, params = "detect_qdrift", "detector.quantile_drift", {}
        if operator in VOTERS:
            bypass = [b for b in params.get("bypass", []) if any(n == b for n, _, _ in out)]
            operator, params = ("detector.soft_vote", {"votes": params.get("votes", 2), "bypass": bypass}) if with_snr \
                else ("detector.ensemble", {"strategy": "majority", "bypass": bypass})
        elif with_snr and operator.startswith("detector.") and operator != "detector.hard_limit":
            params["snr"] = snr
        out.append((name, operator, params))
    return out


def detection_nodes(fetch_operator: str, variant: str = "R+SNR", *, workflow: str = "sample_it_otel",
                    keep_members: bool = False) -> list[NodeSpec]:
    """偵測段的 NodeSpec 清單，fetch 換成 fetch_operator（回放、測試用）。keep_members：裁決保留各票事件。"""
    nodes = [NodeSpec(name="fetch", operator=fetch_operator)]
    for name, operator, params in detection_specs(workflow, variant):
        if operator in VOTERS and keep_members:
            params = {**params, "keep_members": True}
        nodes.append(NodeSpec(name=name, operator=operator, params=params))
    return nodes
