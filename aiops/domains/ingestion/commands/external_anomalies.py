from aiops.commands.base import BaseCommand, CommandError
from aiops.domains.ingestion.dao import ExternalAnomalyDAO
from aiops.domains.ingestion.schemas import IngestAnomaliesRequest


class IngestAnomaliesCommand(BaseCommand):
    """接收外部系統推來的異常（例如工廠 FDC），先存起來，由排程跟偵測到的異常一起進告警管線。

    冪等：同一 source 的 external_id 只收一次，重送只會算成 duplicates。
    不直接觸發告警，也不 import alerts；alerts 用 ExternalAnomalyReadDAO 讀。
    """

    def __init__(self, req: IngestAnomaliesRequest):
        self.req = req

    def validate(self) -> None:
        ids = [a.external_id for a in self.req.anomalies]
        if len(ids) != len(set(ids)):
            raise CommandError("同一批裡 external_id 重複")

    def run(self) -> dict:
        self.validate()
        req = self.req
        seen = ExternalAnomalyDAO.existing_ids(req.source, [a.external_id for a in req.anomalies])
        rows = [
            {"pipeline": req.pipeline, "source": req.source, **a.model_dump()}
            for a in req.anomalies
            if a.external_id not in seen
        ]
        accepted = ExternalAnomalyDAO.create_many(rows)
        return {"pipeline": req.pipeline, "source": req.source, "accepted": accepted, "duplicates": len(seen)}
