"""偵測器算子的共同骨架。每個演算法只需要寫 fit／detect／get_state／load_state，execute 由這裡統一處理。

execute 一律用「新建的演算法實例」跑，所以同一個算子可以安全地對多條序列重複使用（不會把 A 序列學到的基準線帶給 B）。
"""
from __future__ import annotations

import io
import math
from abc import abstractmethod
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from aiops.engine.context import AnomalyResult, DetectorState, PipelineContext, TargetError
from aiops.engine.errors import BadRequest
from aiops.engine.params import deep_merge
from aiops.engine.types import ANOMALY, WARNING, AnomalyEvent, as_utc
from aiops.engine.operator import BaseOperator


class DetectorOperator(BaseOperator):
    scope: ClassVar[str] = "target"
    # 引擎看到這個才會替節點載入基準線與調參（new_only、ensemble 這類沒有基準線的算子不設）
    has_baseline: ClassVar[bool] = True
    algorithm: ClassVar[str] = ""
    # True：df 除了 ts、value 還必須有 peer_* 欄位
    requires_peers: ClassVar[bool] = False
    # False：不需要歷史資料學基準線（靜態規則），訓練點數再少也照判。True 的演算法訓練點數 < min_train_points 就不學、不判
    requires_training: ClassVar[bool] = True
    # 可調參數：名稱 -> (預設值, 最小值, 最大值)。調參（含 LLM 抽規則）只能改這些，且不能超出範圍
    tunable: ClassVar[dict[str, tuple[float, float, float]]] = {}
    # 不開放自動調參、但合法的參數（例如靜態規則的上下限）
    other_params: ClassVar[frozenset[str]] = frozenset()
    # False：這個節點的事件不交給 detector.ensemble 表決（致命硬上限：單點踩線就成立，不能被統計多數決否決）。
    # 排在 ensemble 前面的非投票節點必須列在 ensemble 的 bypass，載入期檢查；不然它會默默變成分母的一員
    votes: ClassVar[bool] = True
    # False：參數只認 YAML 與 target overrides，後台調參（含 LLM 抽規則、規則升級器）一律不套用、提案直接拒絕
    accepts_tuning: ClassVar[bool] = True

    def __init__(self, **params: Any):
        routing = {k: params.pop(k) for k in ROUTING_PARAMS & set(params)}
        behaviour = {k: params.pop(k) for k in NODE_PARAMS & set(params)}
        unknown = set(params) - set(self.tunable) - set(self.other_params)
        if unknown:
            allowed = sorted({*self.tunable, *self.other_params, *ROUTING_PARAMS, *NODE_PARAMS})
            raise TypeError(f"{self.operator_name or type(self).__name__} 沒有參數 {sorted(unknown)}；可用：{allowed}")
        # 路由參數（這個節點對哪些實體跑）由引擎讀，不進 self.params：不會傳給演算法、也不在調參能改的範圍
        self.include_entities = _entity_set(routing, "include_entities")
        self.exclude_entities = _entity_set(routing, "exclude_entities") or frozenset()
        # 節點行為參數：同樣不進演算法、調參改不到
        self.exclude_incidents = bool(behaviour.get("exclude_incidents", True))
        ratio = behaviour.get("warning_ratio")
        if ratio is not None:
            ratio = float(ratio)
            if not 0 < ratio < 1:
                raise TypeError("warning_ratio 必須在 0 到 1 之間（不含），例如 0.8 = 門檻的八成就先預警")
            if "threshold" not in self.tunable:
                raise TypeError(f"{self.operator_name or type(self).__name__} 沒有 threshold，不能設 warning_ratio")
        self.warning_ratio = ratio
        self.min_train_points = int(behaviour.get("min_train_points", DEFAULT_MIN_TRAIN_POINTS))
        if self.min_train_points < 1:
            raise TypeError("min_train_points 至少 1")
        self.params = params
        # 建立時就檢查寫了的值在不在範圍內：節點 YAML 寫 threshold: -3 在載入 workflow 時就報錯，不是等到第一輪偵測
        self.check_ranges()
        self.fitted = False
        # detect 過程中要讓維運看到、但不是異常的事（例如資料值超出物理可能被當成壞資料）。
        # execute 每次都新建演算法實例，所以這份清單只活一次 execute，不會跨序列、跨輪次累積
        self.notes: list[str] = []

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

    def check_ranges(self) -> None:
        """「寫了的值」合不合理（不管有沒有寫齊）：__init__ 會呼叫，所以節點 YAML、target overrides、調參提案
        都在建立的當下擋掉。預設檢查 tunable 的範圍；有其他參數的演算法覆寫時記得呼叫 super()。"""
        for key, (_default, low, high) in self.tunable.items():
            if key not in self.params:
                continue
            value = self.params[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{self.operator_name}.{key} 必須是有限的數值，不是 {value!r}")
            if not low <= value <= high:
                raise ValueError(f"{self.operator_name}.{key}={value} 超出範圍 [{low}, {high}]")

    def validate_params(self) -> None:
        """各層參數合併之後、真的要用之前的檢查（範圍，加上「有沒有寫齊」）。不合理就丟 ValueError。"""
        self.check_ranges()

    def check_override(self, override: dict[str, Any]) -> None:
        """target overrides 的載入期檢查：參數名稱要合法、合併後要通過範圍檢查；路由參數不准寫在 target 上。"""
        routing = sorted(ROUTING_PARAMS & set(override))
        if routing:
            raise TypeError(f"{routing} 是節點的路由參數，只能寫在節點 params；target 要選偵測器請用 detectors")
        node_only = sorted(NODE_PARAMS & set(override))
        if node_only:
            raise TypeError(f"{node_only} 是節點的行為參數，只能寫在節點 params")
        type(self)(**deep_merge(self.params, override)).validate_params()

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
        if ctx.raw_df.empty:
            return ctx  # 沒資料、沒評估：不算參與投票（棄權），不是投反對票
        if self.requires_peers and not (target.peers or target.peer_group):
            raise BadRequest(f"{self.operator_name} 需要同儕序列（target 的 peers 或 peer_group）")
        if self.requires_peers and not any(str(c).startswith("peer_") for c in ctx.raw_df.columns):
            ctx.log(f"detect:{key}:no-peers:abstain")  # peer_group 只撈到自己：沒得比就棄權，不投反對票
            return ctx

        try:
            algo = type(self)(**self.effective_params(ctx))
            algo.validate_params()
        except ValueError as e:
            raise BadRequest(str(e)) from e
        missing = [m for m in algo.required_lags() if f"lag_{m}" not in ctx.raw_df.columns]
        if missing:
            raise BadRequest(f"{self.operator_name} 需要 ingestion.fetch 的 lags_minutes 包含 {missing}")

        df = ctx.raw_df
        if not algo.can_evaluate(df):
            # 這一輪的資料算不出任何分數（滾動窗比資料長、沒有規則可套）：棄權，不進 detector_runs，不投反對票
            ctx.log(f"detect:{key}:cannot-evaluate:abstain")
            return ctx
        state = ctx.detector_states.get(key)
        base_version = state.version if state else 0  # 寫回時帶上，資料庫才能偵測並發覆蓋
        train: pd.DataFrame | None = None
        if ctx.train_df is not None or state is None:
            # 批次訓練（基準線過期或還沒有）；完全沒有訓練窗（第一次、沒設 train_lookback）就用這一輪的資料
            candidate = self._training_frame(ctx, ctx.train_df if ctx.train_df is not None else df)
            points = algo.training_points(candidate)  # 演算法真正學得到的點（lag／同儕有值的列），不是列數
            if type(self).requires_training and points < self.min_train_points:
                if state is None:
                    # 冷啟動／資料太少：不學、不判、不投票（不進 detector_runs），下一輪資料夠了再學。
                    # 幾十個點學到的基準線不可信；服務一上線就壞的話，還會把壞的狀態學成正常
                    ctx.log(f"train:{key}:too-few-points:{points}<{self.min_train_points}")
                    return ctx
                # 基準線到期要重訓、但這次訓練窗資料不足（資料斷了一天剛恢復）：舊基準線還能用，先用它判
                ctx.log(f"train:{key}:too-few-points:{points}<{self.min_train_points}:keep-old-baseline")
            else:
                train = candidate
        if train is not None:
            algo.fit(train)
            ctx.new_detector_states[key] = self._snapshot(algo, ctx, train, base_version, algo.training_points(train))
        else:
            assert state is not None
            algo.load_state(state.state)
            if state.artifact is not None:
                algo.load_artifact(state.artifact_format, state.artifact)

        ctx.detector_runs.append(key)  # 真的評估過這段資料才算參與（沒抓到異常 = 反對票）
        # 多個偵測器串在一起時，anomaly_result 只留最後一個（給除錯看）；真正的結果是 anomalies 的聯集，
        # 每一筆都帶著自己是哪個演算法、哪個節點抓到的（要交集／多數決就接 detector.ensemble）
        result = ctx.anomaly_result = algo.detect(df)
        for note in algo.notes:  # 壞資料這類「沒判、但要有人知道」的事：進 audit 也進 errors，不默默吞掉
            ctx.log(f"{algo.algorithm}:{key}:{note}")
            ctx.errors.append(TargetError(target.entity, f"{algo.algorithm}:{key}:{note}"))
        warn_at = algo.param("threshold") * self.warning_ratio if self.warning_ratio else None
        for ts, value, score, flagged in zip(df["ts"], result.values, result.scores, result.is_anomaly):
            if flagged:
                level = ANOMALY
            elif warn_at is not None and score > warn_at:
                level = WARNING  # 接近門檻：先預警，不等真的越線
            else:
                continue
            ctx.anomalies.append(AnomalyEvent(target.entity, target.query, as_utc(ts.to_pydatetime()), float(score),
                                              dict(target.attrs), float(value), self.operator_name, key,
                                              target.source_type, (), level))
        return ctx

    def _training_frame(self, ctx: PipelineContext, df: pd.DataFrame) -> pd.DataFrame:
        """訓練前拿掉這個實體的歷史事故與維護窗（ctx.training_exclusions），避免基準線被當機污染。
        拿掉後剩不到原本一半、或不到 MIN_TRAIN_POINTS 點，就退回用全部資料並記錄（資料太少時有偏的基準線好過沒有）。"""
        if not self.exclude_incidents or not ctx.training_exclusions or df.empty:
            return df
        ts = pd.to_datetime(df["ts"], utc=True)
        dirty = pd.Series(False, index=df.index)
        for w in ctx.training_exclusions:
            dirty |= (ts >= pd.Timestamp(w.start)) & (ts <= pd.Timestamp(w.end))
        removed = int(dirty.sum())
        if not removed:
            return df
        kept = df[~dirty].reset_index(drop=True)
        if len(kept) < max(EXCLUSION_MIN_KEEP, len(df) // 2):
            ctx.log(f"train:{self.state_key}:exclusion-skipped:{removed}/{len(df)}pts-would-leave-too-little")
            return df
        ctx.log(f"train:{self.state_key}:excluded:{removed}pts")
        return kept

    def _snapshot(self, algo: DetectorOperator, ctx: PipelineContext, df: pd.DataFrame,
                  base_version: int, points: int) -> DetectorState:
        artifact = algo.get_artifact()
        fmt, blob = artifact if artifact is not None else ("", None)
        if blob is not None and fmt not in SAFE_ARTIFACT_FORMATS:
            raise BadRequest(f"{self.operator_name} 的模型檔格式 {fmt!r} 不允許（只接受 {sorted(SAFE_ARTIFACT_FORMATS)}）")
        return DetectorState(algo.get_state(), as_utc(ctx.now), df["ts"].min().to_pydatetime(),
                             df["ts"].max().to_pydatetime(), points, base_version, self.operator_name, blob, fmt)

    # ── 演算法可以覆寫的「我這一輪能不能判」與「我真正學到幾個點」 ──

    def can_evaluate(self, df: pd.DataFrame) -> bool:
        """這一輪的資料算得出分數嗎？算不出來（滾動窗比資料長、沒有規則）就棄權：不進 detector_runs、不投反對票。"""
        return True

    def training_points(self, df: pd.DataFrame) -> int:
        """訓練資料裡演算法真正用得到的點數（冷啟動門檻與 DetectorState.points 用）。
        預設是列數；需要 lag 或同儕欄位的演算法只算那些欄位有值的列。"""
        return len(df)

    @property
    def state_key(self) -> str:
        """ctx 裡基準線與調參的 key：Workflow 節點名稱（直接用算子、沒經過引擎時退回算子名）。"""
        return self.node_name or self.operator_name

    def effective_params(self, ctx: PipelineContext) -> dict[str, Any]:
        """逐鍵合併：演算法預設值 <- tuning 核准的現行參數 <- Workflow 節點 YAML 的 params <- target 的 overrides。

        YAML 寫了的鍵視為「鎖定」，不會被後台調參蓋掉；沒寫的鍵才吃調參結果。
        target 的 overrides[節點名稱] 最優先：同一個節點套到 50 個服務，個別服務要不同門檻就寫在自己的 target 上。
        調參資料裡已經不認得的參數（例如演算法改版拿掉的）直接略過並記錄，不讓整條序列失敗。
        """
        allowed = {*self.tunable, *self.other_params}
        tuned = ctx.detector_params.get(self.state_key, {})
        if tuned and not self.accepts_tuning:
            ctx.log(f"params:{self.state_key}:tuning-not-accepted:{','.join(sorted(tuned))}")
            tuned = {}
        unknown = sorted(set(tuned) - allowed)
        if unknown:
            ctx.log(f"params:{self.state_key}:ignored-unknown:{','.join(unknown)}")
        locked = sorted((set(tuned) & allowed) & set(self.params))
        if locked:
            ctx.log(f"params:{self.state_key}:yaml-locked:{','.join(locked)}")
        override = ctx.target.overrides.get(self.state_key, {}) if ctx.target else {}
        if override:
            ctx.log(f"params:{self.state_key}:target-override:{','.join(sorted(override))}")
        return layer_params(type(self), tuned, self.params, override)

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

# 每個偵測節點都能寫的路由參數（緊急止血、灰度）：exclude 優先，再看 include（有寫才限制），最後才看 target 的 detectors
ROUTING_PARAMS = frozenset({"include_entities", "exclude_entities"})
# 每個偵測節點都能寫的行為參數：
#   exclude_incidents（預設 true）：訓練基準線時排除這個實體的歷史事故與維護窗
#   warning_ratio（預設不開）：分數超過「門檻 × 這個比例」但還沒越線時記成 warning（預警），例如 0.8
#   min_train_points（預設 60 = 每分鐘一點的 1 小時）：訓練點數不到就不學、不判；requires_training=False 的算子不受影響
NODE_PARAMS = frozenset({"exclude_incidents", "warning_ratio", "min_train_points"})
DEFAULT_MIN_TRAIN_POINTS = 60
# 訓練排除事故窗後至少要剩這麼多點（或原本的一半），不然退回用全部資料
EXCLUSION_MIN_KEEP = 30


def layer_params(cls: type[DetectorOperator], tuned: dict[str, Any], yaml_params: dict[str, Any],
                 override: dict[str, Any]) -> dict[str, Any]:
    """參數優先序的唯一實作：核准調參 <- 節點 YAML params（寫了的鍵鎖定）<- target overrides，逐鍵 deep_merge；
    演算法預設值由算子自己補（param()）。偵測（effective_params）與回測（admin.proposal_backtest）都走這裡，
    兩邊才會算出同一組參數——回測若少疊 YAML 那層，顯示的效果在正式偵測裡根本不會生效。
    調參資料裡演算法已經不認得的鍵（改版拿掉的）直接略過。"""
    allowed = {*cls.tunable, *cls.other_params} if cls.accepts_tuning else set()  # 不收調參的算子：tuned 整層略過
    return deep_merge(deep_merge({k: v for k, v in tuned.items() if k in allowed}, yaml_params), override)


# 分母底噪的兩個 tunable（mad、zscore、hourly_baseline、quantile_drift、compare 都有）：
#   min_scale：絕對下限，單位跟指標一樣（失敗數填 0.5、延遲 µs 填 5000）；0 = 不用
#   min_scale_ratio：相對下限，|中心值| × 這個比例（預設 1%：延遲 1,000,000 µs 的序列分母至少 10,000）
SCALE_FLOOR_TUNABLE: dict[str, tuple[float, float, float]] = {
    "min_scale": (0.0, 0.0, 1e12),
    "min_scale_ratio": (0.01, 0.0, 1.0),
}


def is_integer_series(values: pd.Series | np.ndarray) -> bool:
    """全部是整數值（失敗數、請求數這類計數）。空序列或有 NaN／無限大都不算。"""
    arr = np.asarray(values, dtype=float)
    return bool(arr.size) and bool(np.isfinite(arr).all()) and bool((arr == np.round(arr)).all())


def scale_floor(scale: float, values: pd.Series | np.ndarray | None = None, min_scale: float = 0.0,
                min_scale_ratio: float = 0.01, *, center: float | None = None, integer: bool | None = None) -> float:
    """分母的保底：max(算出來的 scale, 絕對下限 min_scale, |中心| × min_scale_ratio, 整數序列的 1 個單位)。

    整數序列（失敗數、請求數）MAD 常常是 0：1 筆就是最小有意義的變動，分母至少 1——不然 2 筆失敗會變成百萬分。
    中心與「是不是整數」預設從 values（訓練資料）算：中心取中位數；偵測時沒有訓練資料，就由 center／integer 傳進來
    （演算法在 fit 時記在基準線裡）。
    三個下限都是 0（min_scale=0、min_scale_ratio=0、中心為 0 或非整數）時退回 1.0，跟 quantile_drift／compare
    既有的最後一層一致：寧可分數偏鈍，也不要除以趨近 0 的數把雜訊放大成異常。
    底噪只在 scale 小於它時起作用；正常波動的序列 scale 遠大於中心的 1%，分數一分不變。
    """
    if center is None:
        center = float(np.nanmedian(np.asarray(values, dtype=float))) if values is not None and len(values) else 0.0
    if integer is None:
        integer = values is not None and is_integer_series(values)
    floor = max(float(min_scale), abs(float(center)) * float(min_scale_ratio), 1.0 if integer else 0.0)
    out = max(float(scale) if np.isfinite(scale) else 0.0, floor)
    return out if out > 0 else 1.0


def limit_breach(values: pd.Series, upper: float | None, lower: float | None,
                 consecutive: int = 1) -> tuple[pd.Series, pd.Series]:
    """靜態上下限的越線判斷（detector.rules 與 detector.hard_limit 共用；偵測器之間不准互相 import）。
    回傳 (分數, 是否成立)：超過 upper 或低於 lower 算違規，連續違規段長度 >= consecutive 的點才成立；
    成立點的分數 = 1 + 超出比例（相對於那條界線），其餘 0。NaN 不算違規，也會把連續段切斷。"""
    v = values
    excess = pd.Series(0.0, index=v.index)
    if upper is not None:
        excess = excess.where(~(v > upper), (v - upper) / max(abs(float(upper)), 1e-9))
    if lower is not None:
        excess = excess.where(~(v < lower), (lower - v) / max(abs(float(lower)), 1e-9))
    breach = excess > 0
    # 連續違規的長度：遇到沒違規就歸零
    run = breach.groupby((~breach).cumsum()).cumsum()
    flagged = run >= int(consecutive)
    return (excess + breach.astype(float)).where(flagged, 0.0), flagged


def _entity_set(routing: dict[str, Any], key: str) -> frozenset[str] | None:
    if key not in routing:
        return None
    value = routing[key]
    if isinstance(value, str) or not isinstance(value, (list, tuple, set, frozenset)):
        raise TypeError(f"{key} 必須是實體名稱的清單，不是 {value!r}")
    return frozenset(str(v) for v in value)


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
