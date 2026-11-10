"""所有可插拔算子的基底。算子只讀寫 PipelineContext。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar

from aiops.engine.context import PipelineContext


class BaseOperator(ABC):
    # target：對每條序列各跑一次（抓資料、偵測）；batch：所有序列合併後只跑一次（收斂、抑制、通知）
    scope: ClassVar[str] = "target"
    operator_name: ClassVar[str] = ""
    # Workflow 節點名稱（引擎建立管線時填）。同一個算子可以掛兩次（例如 zscore 短窗、長窗），靠它區分
    node_name: str = ""

    @abstractmethod
    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """接收 Context，計算後修改並回傳 Context。"""


@dataclass(frozen=True)
class NodeInfo:
    """Workflow 裡的一個節點（載入期檢查用）。算子若實作 check_workflow(nodes: list[NodeInfo])，引擎在建立管線時
    把整條 workflow 的節點依序交給它：跨節點的設定錯誤（指到不存在的節點、排錯順序）在啟動時就丟
    TypeError／ValueError（引擎包成 WorkflowError），不延到執行期。引擎自己不認得任何算子名稱。"""

    name: str
    operator: str
    params: dict[str, Any]
    op: BaseOperator
