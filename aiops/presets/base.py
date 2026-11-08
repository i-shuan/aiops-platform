from dataclasses import dataclass


@dataclass(frozen=True)
class Preset:
    """場景組態：只列「這個場景啟用哪些外掛」，全是名字，不 import 任何實作。

    同一條 Command 流程，換 preset 就換一組零件；不同場景不另寫一套流程。
    """

    name: str
    description: str
    detectors: tuple[str, ...]
    groupers: tuple[str, ...]
    scorers: tuple[str, ...]
    rca_sections: tuple[str, ...]
