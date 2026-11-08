from dataclasses import dataclass

# 分數 >= 門檻就是該等級，由高到低比對
DEFAULT_SEVERITY = (("P0", 10.0), ("P1", 6.0), ("P2", 3.0), ("P3", 0.0))


@dataclass(frozen=True)
class Preset:
    """場景組態：這個場景啟用哪些外掛（只寫名字）與場景參數（嚴重度門檻）。不 import 任何實作。

    同一條 Command 流程，換 preset 就換一組零件；不同場景不另寫一套流程。
    """

    name: str
    description: str
    detectors: tuple[str, ...]
    groupers: tuple[str, ...]
    scorers: tuple[str, ...]
    rca_sections: tuple[str, ...]
    suppressors: tuple[str, ...] = ()
    severity_thresholds: tuple[tuple[str, float], ...] = DEFAULT_SEVERITY
