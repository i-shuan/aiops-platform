"""偵測器算子的共同骨架。每個演算法只需要寫 fit／detect／get_state／load_state，execute 由這裡統一處理。

execute 一律用「新建的演算法實例」跑，所以同一個算子可以安全地對多條序列重複使用（不會把 A 序列學到的基準線帶給 B）。
"""
from __future__ import annotations

import io
import math
from abc import abstractmethod
from statistics import NormalDist
from typing import Any, ClassVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import numpy as np
import pandas as pd

from aiops.engine.context import AnomalyResult, DetectorState, PipelineContext, TargetError
from aiops.engine.errors import BadRequest
from aiops.engine.params import deep_merge
from aiops.engine.types import ANOMALY, WARNING, AnomalyEvent, as_utc
from aiops.engine.operator import BaseOperator


class DetectorOperator(BaseOperator):
    scope: ClassVar[str] = "target"
    # 引擎看到這個才會替節點載入基準線與調參（new_only 這類沒有基準線的算子不設）
    has_baseline: ClassVar[bool] = True
    algorithm: ClassVar[str] = ""
    # True：df 除了 ts、value 還必須有 peer_* 欄位
    requires_peers: ClassVar[bool] = False
    # 可調參數：名稱 -> (預設值, 最小值, 最大值)。調參（含 LLM 抽規則）只能改這些，且不能超出範圍
    tunable: ClassVar[dict[str, tuple[float, float, float]]] = {}
    # 不開放自動調參、但合法的參數（例如時區、時段最少訓練點數）
    other_params: ClassVar[frozenset[str]] = frozenset()
    # False：參數只認 YAML 與 target overrides，後台調參（含 LLM 抽規則）一律不套用、提案直接拒絕
    accepts_tuning: ClassVar[bool] = True
    # True：分數是對「時段（週幾×小時）基準」算的，季節訊號已經扣掉，分數單位就是雜訊尺度（hourly_baseline、residual_*）。
    # 開 snr 時決定門檻怎麼從 p_fa 換算（見 snr_threshold）；不開 snr 時沒有任何作用
    whitened: ClassVar[bool] = False

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
        self.snr = _snr_config(behaviour.get("snr"), self.tunable, self.operator_name or type(self).__name__)
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
            return ctx  # 沒資料、沒評估：不進 detector_runs
        if self.requires_peers and not (target.peers or target.peer_group):
            raise BadRequest(f"{self.operator_name} 需要同儕序列（target 的 peers 或 peer_group）")
        if self.requires_peers and not any(str(c).startswith("peer_") for c in ctx.raw_df.columns):
            ctx.log(f"detect:{key}:no-peers:abstain")  # peer_group 只撈到自己：沒得比就棄權
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
            # 這一輪的資料算不出任何分數（滾動窗比資料長）：棄權，不進 detector_runs
            ctx.log(f"detect:{key}:cannot-evaluate:abstain")
            return ctx
        state = ctx.detector_states.get(key)
        base_version = state.version if state else 0  # 寫回時帶上，資料庫才能偵測並發覆蓋
        train: pd.DataFrame | None = None
        if ctx.train_df is not None or state is None:
            # 批次訓練（基準線過期或還沒有）；完全沒有訓練窗（第一次、沒設 train_lookback）就用這一輪的資料。
            # 沒有最少點數的門檻：拿到幾個點就學幾個點（冷啟動棄權 min_train_points 已於 2026-11-20 移除）
            train = self._training_frame(ctx, ctx.train_df if ctx.train_df is not None else df)
        snr_stats: dict[str, Any] | None = None
        if train is not None:
            algo.fit(train)
            snapshot = self._snapshot(algo, ctx, train, base_version, algo.training_points(train))
            if self.snr is not None:
                snr_stats = fit_snr(algo, train, self.snr)
                snapshot.state = {**snapshot.state, SNR_STATE_KEY: snr_stats}  # 跟基準線一起存、一起到期
            ctx.new_detector_states[key] = snapshot
        else:
            assert state is not None
            stored = dict(state.state)
            snr_stats = stored.pop(SNR_STATE_KEY, None)  # 演算法自己的 load_state 看不到這一層
            algo.load_state(stored)
            if state.artifact is not None:
                algo.load_artifact(state.artifact_format, state.artifact)
        if self.snr is not None:
            threshold, source = (float(snr_stats["threshold"]), str(snr_stats["threshold_source"])) if snr_stats \
                else (algo.param("threshold"), "fallback")  # 舊基準線沒有 SNR 統計：照原本的 threshold，下次重訓才有
            algo.params = {**algo.params, "threshold": threshold}
            db = snr_stats.get("snr_db") if snr_stats else None
            ctx.log(f"snr:{key}:db={'n/a' if db is None else round(db, 2)}:threshold={round(threshold, 3)}:{source}")

        ctx.detector_runs.append(key)  # 真的評估過這段資料才記
        # 多個偵測器串在一起時，anomaly_result 只留最後一個（給除錯看）；真正的結果是 anomalies 的聯集，
        # 每一筆都帶著自己是哪個演算法、哪個節點抓到的。沒有表決：任何一個節點越線，事件就原樣往下游走
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
        拿掉後剩不到原本一半、或不到 EXCLUSION_MIN_KEEP 點，就退回用全部資料並記錄（資料太少時有偏的基準線好過沒有）。"""
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
        """這一輪的資料算得出分數嗎？算不出來（滾動窗比資料長）就棄權：不進 detector_runs。"""
        return True

    def training_points(self, df: pd.DataFrame) -> int:
        """訓練資料裡演算法真正用得到的點數（記在 DetectorState.points）。
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
        allowed = tuning_keys(type(self))
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
#   snr（預設不開）：{p_fa: 目標逐點虛警率, tz: 時段時區, min_slot_points: 時段最少點數}。開了就算序列 SNR（記在基準線裡，
#     只當診斷）、節點門檻改由 p_fa 換算（取代 threshold）。它只是「門檻怎麼訂」的另一種寫法，不壓制任何事件
NODE_PARAMS = frozenset({"exclude_incidents", "warning_ratio", "snr"})
SNR_STATE_KEY = "_snr"


def tuning_keys(cls: type[DetectorOperator]) -> set[str]:
    """調參（ctx.detector_params／核准的提案）可以帶哪些鍵：tunable 與 other_params。
    資料庫裡舊調參帶的其他鍵（2026-11-20 移除的 threshold_scale、min_scale、fpr、max_tail_factor）
    不在這裡面：effective_params／layer_params 直接略過並記 log，不會讓序列失敗。"""
    return {*cls.tunable, *cls.other_params}


# 訓練排除事故窗後至少要剩這麼多點（或原本的一半），不然退回用全部資料
EXCLUSION_MIN_KEEP = 30


def layer_params(cls: type[DetectorOperator], tuned: dict[str, Any], yaml_params: dict[str, Any],
                 override: dict[str, Any]) -> dict[str, Any]:
    """參數優先序的唯一實作：核准調參 <- 節點 YAML params（寫了的鍵鎖定）<- target overrides，逐鍵 deep_merge；
    演算法預設值由算子自己補（param()）。偵測（effective_params）與回測（admin.proposal_backtest）都走這裡，
    兩邊才會算出同一組參數——回測若少疊 YAML 那層，顯示的效果在正式偵測裡根本不會生效。
    調參資料裡演算法已經不認得的鍵（改版拿掉的）直接略過。"""
    allowed = tuning_keys(cls) if cls.accepts_tuning else set()  # 不收調參的算子：tuned 整層略過
    return deep_merge(deep_merge({k: v for k, v in tuned.items() if k in allowed}, yaml_params), override)


def is_integer_series(values: pd.Series | np.ndarray) -> bool:
    """全部是整數值（失敗數、請求數這類計數）。空序列或有 NaN／無限大都不算。"""
    arr = np.asarray(values, dtype=float)
    return bool(arr.size) and bool(np.isfinite(arr).all()) and bool((arr == np.round(arr)).all())


def guard_scale(*candidates: float | None) -> float:
    """分母唯一的保底，只防除以 0：依序取第一個「有限而且 > 0」的尺度，都沒有就 1.0。
    量到的尺度多小都照用，不墊高。演算法實際用的是 floored_scale：在這之上只多一個相對底噪（水位的 1%）。
    其他分母下限（min_scale、整數序列至少 1、時段 std 至少全段的一成）已於 2026-11-20 移除，沒有放回來。"""
    for c in candidates:
        if c is not None and math.isfinite(c) and c > 0:
            return float(c)
    return 1.0


# 相對底噪：分母至少是「序列水位 × min_scale_ratio」，預設 1%。
# 來源：這是 2026-11-20 之前就有的預設值（當時跟絕對下限、整數下限、尾巴校正一起被移除），同一天依實測結果只放回這一個。
# 驗證的資料（演練叢集，真實服務指標經 Grafana 取得）：沒有它的時候，故障移除後 20 分鐘，rca-mes-api 的 p95 延遲
#   0.0988 秒（正常值）在 residual_chronic 得到 11,247 分、模擬產線的佇列深度 35（正常值）得到 182 分，每一輪都有新異常，
#   事件群不會結案——很穩定的序列量到的雜訊尺度趨近 0，任何一點偏差除下去都是幾千分。
# 留出驗證：沒有。1% 沒有在其他序列上比較過別的值（0.5%、2%），只確認了放回去之後上面那兩條序列在故障後回到門檻以下。
# 代價與已知的邊界（tests/test_hardening_noise_floor.py 釘著）：
#   - 很穩定的序列上，看不到的範圍是「門檻 × 1%」：偏離水位 3%（hourly、chronic）或 4%（shift）以內不會報，不只是 1%。
#   - 訓練期完全不動的序列（σ = 0）分母從以前的 1.0 變成水位的 1%：水位小於 100 的反而比沒放回來之前更敏感
#     （固定是 5 的序列，5→6 現在是 20 分）。
#   - 水位接近 0 的序列（稀疏計數、平常是 0 的值）底噪也接近 0，等於沒有。
#   - 兩個殘差偵測器用整段訓練的中位數當水位，不分時段；hourly 用該時段的平均。日夜差很大的序列，白天的實際底噪不到 1%。
#   - 沒記水位的舊基準線（放回之前存的）沒有底噪，要等重訓。
# 要更靈敏就在節點 YAML 把 min_scale_ratio 調小，0 = 關掉。
MIN_SCALE_RATIO_TUNABLE: dict[str, tuple[float, float, float]] = {"min_scale_ratio": (0.01, 0.0, 1.0)}


def floored_scale(level: float | None, ratio: float, *candidates: float | None) -> float:
    """分母 = max(量到的尺度, |水位| × ratio)。量到的尺度依序取第一個有限而且 > 0 的（同 guard_scale）；
    兩者都是 0（沒量到尺度、水位也是 0 或不知道）才退回 1.0，只防除以 0。
    level 是 None（基準線是放回相對底噪之前存的，沒記水位）時沒有相對底噪，等下次重訓。"""
    measured = next((float(c) for c in candidates if c is not None and math.isfinite(c) and c > 0), 0.0)
    floor = abs(float(level)) * float(ratio) if level is not None and math.isfinite(level) else 0.0
    out = max(measured, floor)
    return out if out > 0 else 1.0


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


# ── 時序白化（殘差偵測器共用）──
# 「原值 y(t) − 該 週幾×小時 時段的基準」= 殘差 e(t)。每天固定的 09:00／19:00 階梯、週末低谷都被扣掉，
# 剩下的才交給「看相鄰差」或「看持續偏離」的演算法。偵測器之間不准互相 import，所以白化寫成這裡的純函式，
# 每個殘差算子自己學、自己存（基準線以節點名稱持久化），不去讀 hourly_baseline 的狀態。
#
# 時段資料不足時的退回順序（每個點各自退）：週幾×小時（168 格）→ 一天中的小時（24 格，平日週末混算）→ 全段平均。
# 「夠不夠」= 那一格的訓練點數 >= min_slot_points。退到哪一層記在 state 的 slots_ready／hours_ready。

SLOTS_PER_WEEK = 168
_WEEK = pd.Timedelta(days=7)


def parse_tz(name: Any, operator: str) -> ZoneInfo:
    try:
        return ZoneInfo(str(name))
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise TypeError(f"{operator} 的 tz 不是合法時區：{name!r}") from e


def _local_slots(ts: pd.Series, tz: ZoneInfo) -> tuple[np.ndarray, np.ndarray]:
    t = pd.to_datetime(ts)
    t = t.dt.tz_localize("UTC") if t.dt.tz is None else t
    local = t.dt.tz_convert(tz)
    return (local.dt.day_of_week * 24 + local.dt.hour).to_numpy(), local.dt.hour.to_numpy()


def fit_seasonal_profile(df: pd.DataFrame, tz: ZoneInfo, min_slot_points: int) -> tuple[dict[str, Any], np.ndarray]:
    """學 週幾×小時 的平均，回傳 (profile, 訓練期殘差)。

    訓練期殘差用「留一週」算：某一格有 2 週以上的資料時，每個點扣的是「同一格、別週」的平均，
    不是含自己在內的平均——否則訓練殘差會小得不真實（7 天裡每格只看過一次時，殘差只剩那一小時內的雜訊），
    接下來用它學的雜訊尺度會偏小、測試期一換週就誤報。只有 1 週的格子沒得留，用含自己的平均（尺度偏樂觀，
    state 的 lowo_share 記有多少比例的點是留一週算的）。用平均不用中位數，是為了留一週能用加總直接扣掉。"""
    slot, hour = _local_slots(df["ts"], tz)
    v = df["value"].astype(float).to_numpy()
    ts = pd.to_datetime(df["ts"])
    week = ((ts - ts.min()) // _WEEK).to_numpy().astype(int)
    frame = pd.DataFrame({"s": slot, "h": hour, "w": week, "v": v})
    by_slot = frame.groupby("s")["v"].agg(["sum", "count"])
    by_slot_week = frame.groupby(["s", "w"])["v"].agg(["sum", "count"])
    by_hour = frame.groupby("h")["v"].agg(["mean", "count"])
    profile = {
        "tz": str(tz), "min_slot_points": int(min_slot_points),
        "slots": {str(int(s)): [float(r["sum"] / r["count"]), int(r["count"])] for s, r in by_slot.iterrows()},
        "hours": {str(int(h)): [float(r["mean"]), int(r["count"])] for h, r in by_hour.iterrows()},
        "global": float(v.mean()) if len(v) else 0.0,
    }
    profile["slots_ready"] = sum(1 for _m, n in profile["slots"].values() if n >= min_slot_points)
    profile["hours_ready"] = sum(1 for _m, n in profile["hours"].values() if n >= min_slot_points)

    base = seasonal_baseline(df["ts"], profile)
    s_sum = by_slot["sum"].reindex(slot).to_numpy()
    s_cnt = by_slot["count"].reindex(slot).to_numpy()
    sw = by_slot_week.reindex(pd.MultiIndex.from_arrays([slot, week]))
    other_cnt = s_cnt - sw["count"].to_numpy()
    lowo = (other_cnt >= min_slot_points) & (s_cnt >= min_slot_points)
    with np.errstate(invalid="ignore", divide="ignore"):
        other_mean = (s_sum - sw["sum"].to_numpy()) / other_cnt
    base = np.where(lowo, other_mean, base)
    profile["lowo_share"] = round(float(lowo.mean()) if len(lowo) else 0.0, 3)
    return profile, v - base


def seasonal_baseline(ts: pd.Series, profile: dict[str, Any]) -> np.ndarray:
    """每個時間點該扣的基準：週幾×小時 → 小時 → 全段（點數不到 min_slot_points 就往下一層退）。"""
    tz = parse_tz(profile["tz"], "seasonal_profile")
    need = int(profile["min_slot_points"])
    slot, hour = _local_slots(ts, tz)
    slots, hours, fallback = profile["slots"], profile["hours"], float(profile["global"])
    out = np.empty(len(slot))
    for i, (s, h) in enumerate(zip(slot, hour)):
        m, n = slots.get(str(int(s)), (0.0, 0))
        if n < need:
            m, n = hours.get(str(int(h)), (0.0, 0))
            if n < need:
                m = fallback
        out[i] = m
    return out


def seasonal_residual(df: pd.DataFrame, profile: dict[str, Any]) -> np.ndarray:
    return df["value"].astype(float).to_numpy() - seasonal_baseline(df["ts"], profile)


# ── 雜訊尺度（殘差偵測器共用）──
# 分數 = |統計量| / σ，分數 > threshold 就舉牌。σ = 1.4826 × MAD(訓練期統計量)：常態下等於標準差，不會被訓練期的尖峰拉大。
# 統計量的分布直接在訓練期殘差上量（滾動平均就量滾動平均），所以殘差的自相關、時段基準的週間誤差都已經算進去，
# 不用 std/√window 那種「點與點獨立」的假設。
# 沒有尾巴校正、沒有絕對底噪（fpr／max_tail_factor／min_scale 已於 2026-11-20 移除；相對底噪 min_scale_ratio 同日放回，見 floored_scale）：尾巴比常態肥的序列會比常態假設吵。
# σ 為 0（訓練期幾乎不動）時分母是水位的 min_scale_ratio；水位也是 0 才退到 1.0（floored_scale）。

_MAD_TO_STD_RES = 1.4826


def noise_stats(stat: np.ndarray) -> dict[str, float]:
    """fit 時量訓練期統計量的 σ（1.4826 × MAD），存進基準線。
    MAD 為 0 時不退標準差：標準差會被訓練期裡那幾個跳點自己撐大，等於把異常學成雜訊。"""
    raw = np.asarray(stat, dtype=float)
    raw = raw[np.isfinite(raw)]
    if not len(raw):
        return {"noise": 0.0, "points": 0}
    return {"noise": float(np.median(np.abs(raw - np.median(raw))) * _MAD_TO_STD_RES), "points": int(len(raw))}


# ── 序列 SNR 與「由 p_fa 換算門檻」（節點參數 snr 開了才用）──
#
# 序列層 SNR（fit 時算、跟基準線一起存在 state 的 "_snr"）：
#   signal = 訓練期每個點對應的 168 格（週幾×小時）季節基準的變異數（點數加權；時段不足的點照 seasonal_baseline 的退回）
#   noise  = (1.4826 × MAD(殘差))²，殘差 = 原值 − 季節基準（留一週算，見 fit_seasonal_profile）
#   SNR_dB = 10·log10(signal / noise)
#   noise 為 0（殘差幾乎不動）：σ 用 guard_scale 的 1.0，noise_floored = True，不除以 0。
#   資料不足（168 格一格都不夠 min_slot_points）：status = insufficient，snr_db = None；signal 為 0（時段基準全一樣）：
#   status = flat，snr_db = None。兩種情況門檻照常算（門檻不依賴序列 SNR，見下）。
#
# 門檻（取代寫死的 threshold）：
#   whitened 節點（分數已經是「偏離 / 雜訊尺度」）：threshold = z_{1−p_fa/2}，常態雙尾分位數（p_fa = 0.001 -> 3.29）。
#     Neyman–Pearson：固定虛警率時門檻只由雜訊分布決定；SNR 決定的是抓不抓得到（檢出率），不是門檻該放哪。
#     序列 SNR 在這裡不進門檻，照實不硬塞。
#   沒白化的節點（whitened = False 的演算法）：理論上它們「看到的雜訊」= 雜訊 + 沒扣掉的季節訊號，
#     帶寬應該是 z·σ_n·√(1+SNR)；但它們的分數單位不是 σ_n（mad 的分母是原值的 MAD），這條式子換不到它們的單位。
#     替代：threshold = max(z_{1−p_fa/2}, 訓練期該節點分數的 1−p_fa 經驗分位數)——同一個 p_fa、直接量季節訊號撐大了多少，
#     效果就是 √(1+SNR) 那一項的實測版。


def _snr_config(raw: Any, tunable: dict[str, Any], name: str) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise TypeError(f"{name} 的 snr 必須是 {{p_fa: ..., tz: ...}}，不是 {raw!r}")
    unknown = sorted(set(raw) - {"p_fa", "tz", "min_slot_points"})
    if unknown:
        raise TypeError(f"{name} 的 snr 沒有 {unknown}；可用：p_fa、tz、min_slot_points")
    if "threshold" not in tunable:
        raise TypeError(f"{name} 沒有 threshold，不能用 snr 換算門檻")
    p_fa = raw.get("p_fa")
    if isinstance(p_fa, bool) or not isinstance(p_fa, (int, float)) or not 0 < float(p_fa) < 0.5:
        raise ValueError(f"{name} 的 snr.p_fa 必須在 0 到 0.5 之間（不含），例如 0.001；現在是 {p_fa!r}")
    tz = parse_tz(raw.get("tz", "UTC"), f"{name} 的 snr")
    slots = int(raw.get("min_slot_points", 30))
    if slots < 2:
        raise TypeError(f"{name} 的 snr.min_slot_points 至少要 2")
    return {"p_fa": float(p_fa), "tz": tz, "min_slot_points": slots}


def gaussian_threshold(p_fa: float) -> float:
    """雙尾常態分位數 z_{1−p_fa/2}：雜訊是常態、分數 = |偏離| / σ 時，逐點虛警率剛好是 p_fa。"""
    return float(NormalDist().inv_cdf(1.0 - p_fa / 2.0))


def series_snr(df: pd.DataFrame, tz: ZoneInfo, min_slot_points: int) -> dict[str, Any]:
    """序列層 SNR（定義見上面的註解）。回傳可 JSON 序列化的 dict。"""
    profile, resid = fit_seasonal_profile(df, tz, min_slot_points)
    baseline = seasonal_baseline(df["ts"], profile)
    signal = float(np.var(baseline)) if len(baseline) else 0.0
    r = resid[np.isfinite(resid)]
    sigma = float(np.median(np.abs(r - np.median(r))) * _MAD_TO_STD_RES) if len(r) else 0.0
    floored = sigma <= 0
    sigma = guard_scale(sigma)
    out = {"signal_var": signal, "noise_sigma": sigma, "noise_floored": floored,
           "slots_ready": int(profile["slots_ready"]), "lowo_share": profile["lowo_share"], "points": int(len(df))}
    if profile["slots_ready"] == 0:
        return {**out, "status": "insufficient", "snr_lin": None, "snr_db": None}
    if signal <= 0:
        return {**out, "status": "flat", "snr_lin": 0.0, "snr_db": None}
    snr_lin = signal / sigma ** 2
    return {**out, "status": "ok", "snr_lin": snr_lin, "snr_db": float(10 * math.log10(snr_lin))}


def snr_threshold(algo: DetectorOperator, train: pd.DataFrame, p_fa: float) -> tuple[float, str]:
    """由 p_fa 換算門檻（跟節點分數同單位）。whitened 節點用常態分位數；沒白化的取常態分位數與訓練期分數經驗分位數的大者。"""
    z = gaussian_threshold(p_fa)
    if type(algo).whitened:
        return z, "gaussian"
    algo.params = {**algo.params, "threshold": z}
    scores = np.asarray(algo.detect(train).scores, dtype=float)
    scores = scores[np.isfinite(scores)]
    empirical = float(np.quantile(scores, 1.0 - p_fa)) if len(scores) else 0.0
    return max(z, empirical), "empirical" if empirical > z else "gaussian"


def fit_snr(algo: DetectorOperator, train: pd.DataFrame, cfg: dict[str, Any]) -> dict[str, Any]:
    stats = series_snr(train, cfg["tz"], cfg["min_slot_points"])
    threshold, source = snr_threshold(algo, train, cfg["p_fa"])
    return {**stats, "p_fa": cfg["p_fa"], "threshold": threshold, "threshold_source": source}
