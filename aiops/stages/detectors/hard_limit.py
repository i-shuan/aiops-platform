import numpy as np
import pandas as pd

from aiops.engine.context import AnomalyResult
from aiops.engine.registry import OperatorRegistry
from aiops.stages.detectors.base import DetectorOperator, limit_breach

_LIMITS = ("fatal_ceiling", "fatal_lower", "max_rise", "max_drop", "max_deviation_ratio")
_MIN_PRIOR_POINTS = 5  # 偏離倍數要跟「前面幾個點的中位數」比：前面不到 5 個有效點就不判


@OperatorRegistry.register("detector.hard_limit")
class HardLimitDetector(DetectorOperator):
    """致命硬上限（雙軌制的軌道一）：物理或業務上「踩到就是出事」的界線，單點成立、不交給統計多數決表決。

    統計偵測器（軌道二）要 3 個裡 2 個同意才算異常，單點的微電弧、瞬斷正好會被否決；這個算子不投票
    （votes = False，必須列在 detector.ensemble 的 bypass），它的事件原樣留下，再由 alert.fatal_floor 直接拉到 P0。

    params（都是人訂的，通常寫在 target 的 overrides[節點名稱]；一個都沒給的序列就棄權）：
      fatal_ceiling        絕對上限：值 > 它就成立（電流、溫度、錯誤率）
      fatal_lower          絕對下限：值 < 它就成立（電壓、流量、心跳數）
      max_rise／max_drop   瞬間微分：跟前一個點比，一步上升／下降超過這個量就成立（單位跟指標一樣）
      max_deviation_ratio  極端單點偏離：|值 − 前 deviation_window 點的中位數| > 倍數 × |中位數| 就成立
      deviation_window     上面那個中位數看前面幾個點（預設 30；前面有效點不到 5 個、或中位數是 0 時不判）
      consecutive          fatal_ceiling／fatal_lower 要連續幾點才成立（預設 1 = 單點就成立；重啟歸零會誤踩下限的
                           指標設 2，代價是晚一個點）。微分與偏離倍數本來就是單點事件，不受它影響
      plausible_max／plausible_min  超出物理可能的值（比例 > 1、溫度 9999）當成壞資料：不判致命、不拿來當微分與
                           偏離的比較基準，並記進 audit 與 errors（hard_limit:<節點>:implausible:…）

    不學習（requires_training = False）、不開放調參（accepts_tuning = False）：界線只認 YAML，
    後台調參、LLM 抽規則、規則升級器都改不到它。分數 = 1 + 超出比例。
    """

    algorithm = "hard_limit"
    other_params = frozenset({*_LIMITS, "deviation_window", "consecutive", "plausible_max", "plausible_min"})
    requires_training = False
    votes = False
    accepts_tuning = False

    def _num(self, key: str) -> float | None:
        value = self.params.get(key)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not np.isfinite(value):
            raise ValueError(f"{key} 必須是有限的數值，不是 {value!r}")
        return float(value)

    @property
    def has_limit(self) -> bool:
        return any(self.params.get(k) is not None for k in _LIMITS)

    def can_evaluate(self, df: pd.DataFrame) -> bool:
        return self.has_limit  # 這條序列沒訂任何界線：棄權（不進 detector_runs），不是「看過了沒事」

    def check_ranges(self) -> None:
        ceiling, lower = self._num("fatal_ceiling"), self._num("fatal_lower")
        top, bottom = self._num("plausible_max"), self._num("plausible_min")
        for key in ("max_rise", "max_drop", "max_deviation_ratio"):
            value = self._num(key)
            if value is not None and value <= 0:
                raise ValueError(f"{key} 必須 > 0，不是 {value:g}")
        for key, minimum in (("consecutive", 1), ("deviation_window", _MIN_PRIOR_POINTS)):
            value = self.params.get(key)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < minimum):
                raise ValueError(f"{key} 必須是 >= {minimum} 的整數，不是 {value!r}")
        if ceiling is not None and lower is not None and lower >= ceiling:
            raise ValueError(f"fatal_lower（{lower:g}）必須小於 fatal_ceiling（{ceiling:g}）")
        if top is not None and bottom is not None and bottom >= top:
            raise ValueError(f"plausible_min（{bottom:g}）必須小於 plausible_max（{top:g}）")
        if ceiling is not None and top is not None and ceiling >= top:
            raise ValueError(f"fatal_ceiling（{ceiling:g}）必須小於 plausible_max（{top:g}），不然踩線的值全被當成壞資料")
        if lower is not None and bottom is not None and lower <= bottom:
            raise ValueError(f"fatal_lower（{lower:g}）必須大於 plausible_min（{bottom:g}），不然踩線的值全被當成壞資料")

    def fit(self, df: pd.DataFrame) -> "HardLimitDetector":
        self.fitted = True
        return self

    def get_state(self) -> dict:
        return {}

    def load_state(self, state: dict) -> "HardLimitDetector":
        self.fitted = True
        return self

    def detect(self, df: pd.DataFrame) -> AnomalyResult:
        self._require_fitted()
        self.validate_params()
        raw = df["value"].astype(float)
        top, bottom = self._num("plausible_max"), self._num("plausible_min")
        bad = pd.Series(False, index=raw.index)
        if top is not None:
            bad |= raw > top
        if bottom is not None:
            bad |= raw < bottom
        if bad.any():
            shown = ",".join(f"{x:g}" for x in raw[bad].head(5)) + ("…" if int(bad.sum()) > 5 else "")
            self.notes.append(f"implausible:{int(bad.sum())}pts:{shown}")
        v = raw.mask(bad)  # 壞資料變 NaN：不踩線，也不當下一個點的比較基準

        scores, flagged = limit_breach(v, self._num("fatal_ceiling"), self._num("fatal_lower"),
                                       int(self.params.get("consecutive", 1)))
        step = v.diff()  # 第一個點、或前一個點是壞資料／缺值時是 NaN：不判
        for key, moved in (("max_rise", step), ("max_drop", -step)):
            limit = self._num(key)
            if limit is not None:
                over = (moved - limit) / limit
                scores = scores.where(~(over > 0) | (scores >= 1 + over), 1 + over)
        ratio = self._num("max_deviation_ratio")
        if ratio is not None:
            window = int(self.params.get("deviation_window", 30))
            centre = v.shift(1).rolling(window, min_periods=_MIN_PRIOR_POINTS).median()
            scale = centre.abs().where(centre.abs() > 0)  # 中位數是 0：倍數沒有意義，不判（用 fatal_ceiling 或 max_rise）
            over = ((v - centre).abs() / scale - ratio) / ratio
            scores = scores.where(~(over > 0) | (scores >= 1 + over), 1 + over)
        scores = scores.fillna(0.0).clip(upper=1e6)  # 資料源吐 inf 時分數也要是能存的數
        return self._result(df, scores, scores > 0)
