from typing import Any, Literal

from pydantic import BaseModel, Field

from aiops.schemas import MetricTarget, TimeRange


class ProposalCreateRequest(BaseModel):
    # auto：自動調優；llm：從回饋文字抽出；manual：人工調整
    source: Literal["auto", "llm", "manual"]
    detector_name: str
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = ""
    proposer: str


class BacktestRequest(BaseModel):
    metric: MetricTarget
    time_range: TimeRange
    # peer 偵測器回測需要同儕序列
    peers: list[MetricTarget] = Field(default_factory=list)


class ApproveRequest(BaseModel):
    approver: str


class FeedbackSubmitRequest(BaseModel):
    # 通知裡的 incident_uid
    incident_uid: str
    # false_positive：誤報；true_positive：確認是真的；missed：漏報（該報沒報）
    verdict: Literal["false_positive", "true_positive", "missed"]
    # 哪個偵測器的問題；不知道可以不填，轉提案時再指定
    detector_name: str | None = None
    text: str = Field(min_length=1, max_length=4000)
    author: str


class ProposeFromFeedbackRequest(BaseModel):
    proposer: str
    # 回饋沒填偵測器時要在這裡指定
    detector_name: str | None = None
