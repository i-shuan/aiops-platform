"""所有可插拔算子的基底。算子只讀寫 PipelineContext。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from aiops.core.context import PipelineContext


class BaseOperator(ABC):
    # target：對每條序列各跑一次（抓資料、偵測）；batch：所有序列合併後只跑一次（收斂、抑制、通知）
    scope: ClassVar[str] = "target"
    operator_name: ClassVar[str] = ""
    # Workflow 節點名稱（引擎建立管線時填）。同一個算子可以掛兩次（例如 zscore 短窗、長窗），靠它區分
    node_name: str = ""

    @abstractmethod
    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """接收 Context，計算後修改並回傳 Context。"""
