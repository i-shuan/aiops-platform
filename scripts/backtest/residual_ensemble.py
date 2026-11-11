"""四組偵測回測，用 ES 的每分鐘請求數序列：
  A      it_otel               hourly_baseline＋mad＋quantile_drift，detector.ensemble 多數決
  R      it_otel_residual      hourly_baseline＋residual_shift＋residual_chronic 多數決＋旁路漂移票
  R+SNR  it_otel_residual_snr  R 的投票節點開 snr（p_fa 換算門檻）＋detector.soft_vote 軟投票
  A+SNR  it_otel_snr           A 的投票節點開 snr＋detector.soft_vote 軟投票

偵測走真的 Pipeline：從各 workflow YAML 取出偵測節點（fetch 之後到裁決節點為止，含 hard_limit 與 bypass），
fetch 換成回放用的 replay.window（每輪給最近 60 分鐘，同 it_otel 的 lookback_minutes），基準線由記憶體版 Hooks 保存——
第一輪沒有基準線時給訓練段 train_df、引擎訓練並交出 new_detector_states，之後每輪載入同一份（等同正式環境基準線還沒到期）。
每 5 分鐘一輪（同 CronJob 週期）；一分鐘只要在任何一輪被 detector.ensemble 判成異常、或被 bypass 節點
（不投票、單獨發告警）判成異常，就算告警分鐘。「各票」也列出 bypass 節點自己的分鐘數。

訓練／測試切分：資料 >= 17 天用 14 天訓練，其餘 7 天。三種注入（測試期中段某個平日 11–13 點）：
  step  瞬間 ×0.3 持續 20 分    ramp  15 分鐘線性滑到 ×0.3 再維持 20 分    mild  ×0.8 持續 60 分

  python scripts/backtest/residual_ensemble.py --secrets <secrets.local.yaml> --cache <快取目錄> --out <結果.json>
"""
from __future__ import annotations

import argparse
import io
import json
import sys
import warnings
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from aiops.engine.operator import BaseOperator  # noqa: E402
from aiops.engine.pipeline import NodeSpec, Pipeline, TargetSpec, Workflow  # noqa: E402
from aiops.engine.registry import OperatorRegistry  # noqa: E402
from aiops.engine.types import ANOMALY  # noqa: E402
from aiops.interfaces.runner import load_all, workflow_path  # noqa: E402
from es_series import load_series  # noqa: E402

warnings.filterwarnings("ignore")
TZ = "Asia/Taipei"
LOOKBACK, STEP = 60, 5
SERVICES = ["mes-api", "wip-worker", "report-svc", "order-svc", "cart-api", "pay-api"]
VARIANTS = {"A": "it_otel", "R": "it_otel_residual", "R+SNR": "it_otel_residual_snr", "A+SNR": "it_otel_snr"}
VOTERS = ("detector.ensemble", "detector.soft_vote")
INJECTIONS = {"step": np.full(20, 0.3), "ramp": np.r_[np.linspace(1.0, 0.3, 15), np.full(20, 0.3)],
              "mild": np.full(60, 0.8)}

load_all()


class Replay:
    """這一輪要給引擎的資料（回放腳本自己的狀態，不在 aiops/ 裡）。"""
    raw: pd.DataFrame | None = None
    train: pd.DataFrame | None = None


@OperatorRegistry.register("replay.window")
class ReplayWindow(BaseOperator):
    def execute(self, ctx):
        ctx.raw_df = Replay.raw.reset_index(drop=True)
        missing = [n for n in ctx.baseline_nodes if n not in ctx.detector_states]
        ctx.train_df = Replay.train.reset_index(drop=True) if missing else None
        return ctx


class MemoryHooks:
    """記憶體版基準線存放：load_target 給現有基準線，persist_target 收新訓練的（同 StorageHooks 的生命週期）。"""

    def __init__(self):
        self.states = {}

    def load_batch(self, ctx): ...

    def load_target(self, ctx, nodes):
        ctx.detector_states = {name: self.states[name] for name, _ in nodes if name in self.states}

    def persist_target(self, ctx):
        self.states.update(ctx.new_detector_states)

    def persist_batch(self, ctx): ...

    def finalize_batch(self, ctx): ...


def detection_nodes(workflow: str) -> list[NodeSpec]:
    """workflow YAML 的偵測段：fetch 之後到 detector.ensemble（含），fetch 換成回放。裁決保留成員事件，才算得出各票。"""
    raw = yaml.safe_load(workflow_path(workflow).read_text(encoding="utf-8"))["nodes"]
    stop = next(i for i, n in enumerate(raw) if n["operator"] in VOTERS)
    nodes = [NodeSpec(name="fetch", operator="replay.window")]
    for n in raw[1:stop + 1]:
        params = dict(n.get("params") or {})
        if n["operator"] in VOTERS:
            params["keep_members"] = True
        nodes.append(NodeSpec(name=n["name"], operator=n["operator"], params=params))
    return nodes


class Runner:
    def __init__(self, workflow: str, service: str, train: pd.DataFrame):
        self.pipe = Pipeline(Workflow(name=f"replay_{workflow}", nodes=detection_nodes(workflow),
                                      targets=[TargetSpec(source_type="mock", query=f"rpm:{service}", entity=service)]),
                             MemoryHooks())
        self.train = train
        vote = next(op for _, op in self.pipe.target_nodes if op.operator_name in VOTERS)
        # bypass 的節點（hard_limit、不投票的旁路漂移票）事件原樣留下，正式環境一樣會發告警：算進告警分鐘
        self.bypass = set(vote.bypass)
        self.voters = [n.name for n, op in self.pipe.target_nodes
                       if getattr(op, "has_baseline", False) and n.name not in self.bypass]

    def flags(self, test: pd.DataFrame, rounds) -> tuple[np.ndarray, dict[str, np.ndarray]]:
        n = len(test)
        pos = {t: k for k, t in enumerate(test["ts"])}
        verdict = np.zeros(n, bool)
        member = {v: np.zeros(n, bool) for v in self.voters}
        Replay.train = self.train
        for i in rounds:
            Replay.raw = test.iloc[i - LOOKBACK + 1:i + 1]
            now = test["ts"].iloc[i].to_pydatetime() + timedelta(seconds=30)
            batch = self.pipe.run(now=now, strict=True)
            for e in batch.anomalies:
                k = pos.get(pd.Timestamp(e.ts))
                if e.level != ANOMALY or k is None:
                    continue
                if e.detector in VOTERS or e.detector_node in self.bypass:
                    verdict[k] = True
                if e.detector_node in self.bypass:
                    member.setdefault(e.detector_node, np.zeros(n, bool))[k] = True
                elif e.detector_node in member:
                    member[e.detector_node][k] = True
        return verdict, member


def clusters(flag: np.ndarray, gap: int = 10) -> int:
    """事件數：告警分鐘之間隔超過 gap 分鐘就算另一件。"""
    idx = np.flatnonzero(flag)
    return int(1 + (np.diff(idx) > gap).sum()) if len(idx) else 0


def inject(test: pd.DataFrame, kind: str) -> tuple[pd.DataFrame, int, int]:
    ts = test["ts"].dt.tz_convert(TZ)
    idx = np.arange(len(test))
    cand = np.flatnonzero(((ts.dt.hour >= 11) & (ts.dt.hour <= 13) & (ts.dt.day_of_week < 5)).to_numpy()
                          & (idx > 200) & (idx < len(test) - 200))
    s = int(cand[len(cand) // 2])
    f = INJECTIONS[kind]
    v = test["value"].to_numpy(float).copy()
    v[s:s + len(f)] *= f
    out = test.copy()
    out["value"] = v
    return out, s, len(f)


def switch_minutes(test: pd.DataFrame, flag: np.ndarray, hours=(9, 19), pad: int = 5) -> int:
    """當地 09:00／19:00 前後 pad 分鐘內的告警分鐘數（每天固定的階梯切換點）。"""
    loc = test["ts"].dt.tz_convert(TZ)
    minute_of_day = (loc.dt.hour * 60 + loc.dt.minute).to_numpy()
    near = np.zeros(len(test), bool)
    for h in hours:
        near |= np.abs(minute_of_day - h * 60) <= pad
    return int((flag & near).sum())


def run_service(service: str, df: pd.DataFrame) -> dict:
    train_days = 14 if len(df) / 1440 >= 17 else 7
    split = df["ts"].iloc[0] + pd.Timedelta(days=train_days)
    train = df[df["ts"] < split].reset_index(drop=True)
    test = df[df["ts"] >= split].reset_index(drop=True)
    days = len(test) / 1440
    per_week = 7 / days
    loc = test["ts"].dt.tz_convert(TZ)
    out = {"資料": f"{df['ts'].iloc[0]:%m-%d %H:%M} → {df['ts'].iloc[-1]:%m-%d %H:%M} UTC",
           "訓練天": train_days, "測試天": round(days, 2)}
    all_rounds = range(LOOKBACK - 1, len(test), STEP)
    for key, workflow in VARIANTS.items():
        runner = Runner(workflow, service, train)
        verdict, member = runner.flags(test, all_rounds)
        r = {"workflow": workflow,
             "各票_分/週": {k: round(int(v.sum()) * per_week) for k, v in member.items()},
             "誤報_分/週": round(int(verdict.sum()) * per_week, 1),
             "誤報_事件/週": round(clusters(verdict) * per_week, 1),
             "切換點±5分_分/週": round(switch_minutes(test, verdict) * per_week, 1)}
        if verdict.any():
            hrs = loc[verdict].dt.hour.value_counts().head(4)
            r["誤報集中的小時(當地)"] = {int(h): int(c) for h, c in hrs.items()}
        r["基準線摘要"] = {k: {"noise": v.state["noise"], "lowo_share": v.state["profile"]["lowo_share"],
                           "slots_ready": v.state["profile"]["slots_ready"]}
                       for k, v in runner.pipe.hooks.states.items() if "noise" in v.state}
        r["SNR"] = {k: {x: v.state["_snr"][x] for x in ("snr_db", "status", "noise_sigma", "signal_var", "threshold",
                                                        "threshold_source")}
                    for k, v in runner.pipe.hooks.states.items() if "_snr" in v.state}
        for kind in INJECTIONS:
            inj, s, length = inject(test, kind)
            # 只重跑看得到注入窗（含其後 60 分拖尾）的輪次；基準線沿用同一份（同一個訓練段）
            rounds = [i for i in all_rounds if s <= i <= s + length + LOOKBACK + 30]
            v2, _ = runner.flags(inj, rounds)
            win = np.zeros(len(test), bool)
            win[s:s + length] = True
            after = np.zeros(len(test), bool)
            after[s + length:s + length + 60] = True
            hit = v2 & win
            r[f"注入_{kind}"] = {"抓到": f"{int(hit.sum())}/{length}",
                               "首次告警_分": int(np.argmax(hit[win])) if hit.any() else None,
                               "窗後60分拖尾_分": int((v2 & after & ~verdict).sum()),
                               "注入起點(當地)": f"{loc.iloc[s]:%m-%d %a %H:%M}"}
        out[key] = r
    return out


def main() -> None:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--secrets", type=Path, help="secrets.local.yaml（快取沒有時才連 ES，唯讀）")
    ap.add_argument("--cache", type=Path, required=True, help="序列 CSV 快取目錄（不要放在 repo 裡）")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--services", default=",".join(SERVICES))
    ap.add_argument("--variants", default=",".join(VARIANTS), help="要跑的組別（逗號分隔）")
    args = ap.parse_args()
    for key in [k for k in VARIANTS if k not in args.variants.split(",")]:
        del VARIANTS[key]
    result = {}
    for service in args.services.split(","):
        result[service] = run_service(service, load_series(service, args.cache, args.secrets))
        print(service, json.dumps(result[service], ensure_ascii=False), flush=True)
        args.out.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
