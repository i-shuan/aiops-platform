"""調參唯一收口：自動 tune、LLM 從回饋抽規則、人工調整，三條路都只能產出提案。

提案 -> 回測 -> 核准，才會寫入現行參數。核准時檢查樂觀鎖：
提案建立後若現行參數已被別的提案改過（例如人工剛調好），舊提案一律作廢重提，
避免「人工剛調好，半夜被自動 tune 蓋掉」。
"""
from aiops.commands.base import BaseCommand, CommandConflict, CommandError, CommandNotFound
from aiops.datasource_specs import fetch_frame
from aiops.domains.tuning.dao import ActiveConfigDAO, ProposalDAO
from aiops.domains.tuning.schemas import BacktestRequest, ProposalCreateRequest
from aiops.plugins.registry import DetectorRegistry


def _load(proposal_id: int) -> dict:
    row = ProposalDAO.get(proposal_id)
    if row is None:
        raise CommandNotFound(f"找不到調優提案 {proposal_id}")
    return row


class CreateProposalCommand(BaseCommand):
    def __init__(self, req: ProposalCreateRequest):
        self.req = req

    def validate(self) -> None:
        if not DetectorRegistry.has(self.req.detector_name):
            raise CommandError(f"未知的檢測外掛: {self.req.detector_name}")
        try:
            DetectorRegistry.get(self.req.detector_name)(self.req.params).validate_params()
        except ValueError as e:
            raise CommandError(f"提案參數不合法：{e}") from e

    def run(self) -> dict:
        self.validate()
        return ProposalDAO.create(
            {
                **self.req.model_dump(),
                "status": "pending",
                "base_version": ActiveConfigDAO.version(self.req.detector_name),
                "backtest": None,
                "approved_by": None,
            }
        )


class GetProposalCommand(BaseCommand):
    def __init__(self, proposal_id: int):
        self.proposal_id = proposal_id

    def validate(self) -> None:
        self.row = _load(self.proposal_id)

    def run(self) -> dict:
        self.validate()
        return self.row


class BacktestProposalCommand(BaseCommand):
    """同一段歷史資料，分別用現行參數與提案參數跑一次，記下差異。"""

    def __init__(self, proposal_id: int, req: BacktestRequest):
        self.proposal_id = proposal_id
        self.req = req

    def validate(self) -> None:
        self.row = _load(self.proposal_id)
        if self.row["status"] not in ("pending", "backtested"):
            raise CommandConflict(f"提案狀態為 {self.row['status']}，不能再回測")
        if not DetectorRegistry.has(self.row["detector_name"]):
            raise CommandError(f"偵測器 {self.row['detector_name']} 已不存在")
        if DetectorRegistry.get(self.row["detector_name"]).requires_peers and not self.req.peers:
            raise CommandError(f"{self.row['detector_name']} 偵測器回測需要 peers")

    def run(self) -> dict:
        self.validate()
        name = self.row["detector_name"]
        m, tr = self.req.metric, self.req.time_range
        df = fetch_frame(m.source_type, m.query, tr.start, tr.end, tr.step_seconds,
                         [(p.source_type, p.query) for p in self.req.peers])
        current_params = (ActiveConfigDAO.get(name) or {}).get("params", {})
        detector_cls = DetectorRegistry.get(name)
        current = detector_cls(current_params).fit(df).detect(df)
        proposed = detector_cls(self.row["params"]).fit(df).detect(df)
        result = {
            "metric": m.model_dump(),
            "points": len(df),
            "current_params": current_params,
            "current_anomalies": int(sum(current.is_anomaly)),
            "proposed_anomalies": int(sum(proposed.is_anomaly)),
        }
        return ProposalDAO.update(self.proposal_id, status="backtested", backtest=result)


class ApproveTuningCommand(BaseCommand):
    def __init__(self, proposal_id: int, approver: str):
        self.proposal_id = proposal_id
        self.approver = approver

    def validate(self) -> None:
        self.row = _load(self.proposal_id)
        if self.row["status"] != "backtested":
            raise CommandConflict(f"提案狀態為 {self.row['status']}，必須先回測才能核准")
        current = ActiveConfigDAO.version(self.row["detector_name"])
        if current != self.row["base_version"]:
            ProposalDAO.update(self.proposal_id, status="stale")
            raise CommandConflict(
                f"現行參數已更新（目前版本 {current}，提案基準版本 {self.row['base_version']}），此提案已作廢，請重新提案"
            )

    def run(self) -> dict:
        self.validate()
        config = ActiveConfigDAO.set(
            self.row["detector_name"], self.row["params"], self.row["source"], self.proposal_id
        )
        proposal = ProposalDAO.update(self.proposal_id, status="approved", approved_by=self.approver)
        return {"proposal": proposal, "active_config": config}
