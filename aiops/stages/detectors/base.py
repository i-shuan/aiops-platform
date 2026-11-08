"""偵測器算子的共同骨架。每個演算法只需要寫 fit／detect／get_state／load_state，execute 由這裡統一處理。

execute 一律用「新建的演算法實例」跑，所以同一個算子可以安全地對多條序列重複使用（不會把 A 序列學到的基準線帶給 B）。
"""
from __future__ import annotations

import io
from abc import abstractmethod
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from aiops.engine.context import AnomalyResult, DetectorState, PipelineContext
from aiops.engine.errors import BadRequest
from aiops.engine.params import deep_merge
from aiops.engine.types import AnomalyEvent, as_utc
from aiops.engine.operator import BaseOperator


class DetectorOperator(BaseOperator):
    scope: ClassVar[str] = "target"
    # 引擎看到這個才會替節點載入基準線與調參（new_only、ensemble 這類沒有基準線的算子不設）
    has_baseline: ClassVar[bool] = True
    algorithm: ClassVar[str] = ""
    # True：df 除了 ts、value 還必須有 peer_* 欄位
    requires_peers: ClassVar[bool] = False
    # 可調參數：名稱 -> (預設值, 最小值, 最大值)。調參（含 LLM 抽規則）只能改這些，且不能超出範圍
    tunable: ClassVar[dict[str, tuple[float, float, float]]] = {}
    # 不開放自動調參、但合法的參數（例如靜態規則的上下限）
    other_params: ClassVar[frozenset[str]] = frozenset()

    def __init__(self, **params: Any):
        unknown = set(params) - set(self.tunable) - set(self.other_params)
        if unknown:
            allowed = sorted({*self.tunable, *self.other_params})
            raise TypeError(f"{self.operator_name or type(self).__name__} 沒有參數 {sorted(unknown)}；可用：{allowed}")
        self.params = params
        self.fitted = False

    # ── 演算法契約 ──

    @abstractmethod
    def fit(self, df: pd.DataFrame) -> DetectorOperator: ...

    @abstractmethod
    def detect(self, df: pd.DataFrame) -> AnomalyResult: ...

    @abstractmethod
    def get_state(self) -> dict[str, Any]:
        """fit 學到的狀態（基準線參數），必須能 JSON 序列化。"""

    @abstractmethod
    def load_state(self, state: dict[str, Any]) -> DetectorOperator:
        """還原 get_state() 的結果，之後視同已 fit。回傳 self。"""

    # ── 共用 ──

    def validate_params(self) -> None:
        """參數不合理就丟 ValueError。預設檢查 tunable 範圍。"""
        for key, (_default, low, high) in self.tunable.items():
            if key in self.params and not (low <= float(self.params[key]) <= high):
                raise ValueError(f"{self.operator_name}.{key}={self.params[key]} 超出範圍 [{low}, {high}]")

    def param(self, key: str) -> float:
        return float(self.params.get(key, self.tunable[key][0]))

    def _require_fitted(self) -> None:
        if not self.fitted:
            raise RuntimeError(f"{self.operator_name}: detect 前必須先 fit")

    def _result(self, df: pd.DataFrame, scores: pd.Series, flagged: pd.Series) -> AnomalyResult:
        return AnomalyResult(
            algorithm=self.algorithm,
            timestamps=[t.isoformat() for t in df["ts"]],
            values=df["value"].tolist(),
            scores=scores.round(4).tolist(),
            is_anomaly=[bool(f) for f in flagged],
        )

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        target = ctx.target
        if target is None or ctx.raw_df is None:
            raise BadRequest(f"{self.operator_name} 前面需要先有 ingestion.fetch（ctx.raw_df 是空的）")
        key = self.state_key
        ctx.detector_runs.append(key)
        if ctx.raw_df.empty:
            return ctx
        if self.requires_peers and not (target.peers or target.peer_group):
            raise BadRequest(f"{self.operator_name} 需要同儕序列（target 的 peers 或 peer_group）")

        algo = type(self)(**self.effective_params(ctx))
        try:
            algo.validate_params()
        except ValueError as e:
            raise BadRequest(str(e)) from e
        missing = [m for m in algo.required_lags() if f"lag_{m}" not in ctx.raw_df.columns]
        if missing:
            raise BadRequest(f"{self.operator_name} 需要 ingestion.fetch 的 lags_minutes 包含 {missing}")

        df = ctx.raw_df
        state = ctx.detector_states.get(key)
        base_version = state.version if state else 0  # 寫回時帶上，資料庫才能偵測並發覆蓋
        if ctx.train_df is not None:  # 批次訓練：基準線過期或還沒有
            algo.fit(ctx.train_df)
            ctx.new_detector_states[key] = self._snapshot(algo, ctx, ctx.train_df, base_version)
        elif state is not None:
            algo.load_state(state.state)
            if state.artifact is not None:
                algo.load_artifact(state.artifact_format, state.artifact)
        else:  # 第一次：用這一輪的資料學基準線並保存
            algo.fit(df)
            ctx.new_detector_states[key] = self._snapshot(algo, ctx, df, base_version)

        # 多個偵測器串在一起時，anomaly_result 只留最後一個（給除錯看）；真正的結果是 anomalies 的聯集，
        # 每一筆都帶著自己是哪個演算法、哪個節點抓到的（要交集／多數決就接 detector.ensemble）
        ctx.anomaly_result = algo.detect(df)
        ctx.anomalies.extend(
            AnomalyEvent(target.entity, target.query, as_utc(ts.to_pydatetime()), float(score),
                         dict(target.attrs), float(value), self.operator_name, key, target.source_type)
            for ts, value, score, flagged in zip(df["ts"], ctx.anomaly_result.values,
                                                 ctx.anomaly_result.scores, ctx.anomaly_result.is_anomaly)
            if flagged
        )
        return ctx

    def _snapshot(self, algo: DetectorOperator, ctx: PipelineContext, df: pd.DataFrame,
                  base_version: int) -> DetectorState:
        artifact = algo.get_artifact()
        fmt, blob = artifact if artifact is not None else ("", None)
        if blob is not None and fmt not in SAFE_ARTIFACT_FORMATS:
            raise BadRequest(f"{self.operator_name} 的模型檔格式 {fmt!r} 不允許（只接受 {sorted(SAFE_ARTIFACT_FORMATS)}）")
        return DetectorState(algo.get_state(), as_utc(ctx.now), df["ts"].min().to_pydatetime(),
                             df["ts"].max().to_pydatetime(), len(df), base_version, self.operator_name, blob, fmt)

    @property
    def state_key(self) -> str:
        """ctx 裡基準線與調參的 key：Workflow 節點名稱（直接用算子、沒經過引擎時退回算子名）。"""
        return self.node_name or self.operator_name

    def effective_params(self, ctx: PipelineContext) -> dict[str, Any]:
        """逐鍵合併：演算法預設值 <- tuning 核准的現行參數 <- Workflow YAML 寫的參數。

        YAML 寫了的鍵視為「鎖定」，不會被後台調參蓋掉；沒寫的鍵才吃調參結果。
        （以前是 YAML 只要寫了任何一個參數，調參結果就整組失效。）
        調參資料裡已經不認得的參數（例如演算法改版拿掉的）直接略過並記錄，不讓整條序列失敗。
        """
        allowed = {*self.tunable, *self.other_params}
        tuned = ctx.detector_params.get(self.state_key, {})
        unknown = sorted(set(tuned) - allowed)
        if unknown:
            ctx.log(f"params:{self.state_key}:ignored-unknown:{','.join(unknown)}")
        tuned = {k: v for k, v in tuned.items() if k in allowed}
        locked = sorted(set(tuned) & set(self.params))
        if locked:
            ctx.log(f"params:{self.state_key}:yaml-locked:{','.join(locked)}")
        return deep_merge(tuned, self.params)

    def warmup_points(self) -> int:
        """detect() 開頭需要幾個「前面的點」才算得出分數（滾動窗）。回測切測試段時往前多帶這麼多點。"""
        return 0

    def required_lags(self) -> list[int]:
        """需要 fetch 準備哪些往前位移的欄位（lag_<分鐘>），例如同比要 10080。回測也照這個抓。"""
        return []

    # ── 非 JSON 的模型本體（選用）──
    # get_state() 必須是 JSON；模型有大量數值（例如樹模型、矩陣）時，另外用 get_artifact() 交出二進位本體。
    # 只允許安全格式：npz（numpy 陣列，載入時禁止 pickle）。任何 pickle／joblib 都不准（tests/test_architecture.py 檢查）。

    def get_artifact(self) -> tuple[str, bytes] | None:
        """回傳 (格式, 內容) 或 None（預設：沒有額外模型檔）。"""
        return None

    def load_artifact(self, fmt: str, data: bytes) -> None:
        """還原 get_artifact() 的內容。在 load_state() 之後呼叫。"""
        raise BadRequest(f"{self.operator_name} 沒有實作 load_artifact，但資料庫裡有它的模型檔（{fmt}）")


SAFE_ARTIFACT_FORMATS = frozenset({"npz"})


def pack_arrays(arrays: dict[str, np.ndarray]) -> tuple[str, bytes]:
    """把數值陣列打包成 npz（給 get_artifact 用）。只接受數值型陣列，物件陣列（會需要 pickle）直接拒絕。"""
    for name, arr in arrays.items():
        if np.asarray(arr).dtype == object:
            raise ValueError(f"陣列 {name} 是物件型別，不能安全序列化")
    buf = io.BytesIO()
    np.savez_compressed(buf, **{k: np.asarray(v) for k, v in arrays.items()})
    return "npz", buf.getvalue()


def unpack_arrays(fmt: str, data: bytes) -> dict[str, np.ndarray]:
    """還原 pack_arrays 的內容。allow_pickle=False：就算檔案被竄改也不會執行任意程式。"""
    if fmt != "npz":
        raise ValueError(f"不支援的模型檔格式：{fmt}")
    with np.load(io.BytesIO(data), allow_pickle=False) as npz:
        return {k: npz[k] for k in npz.files}
