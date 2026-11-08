"""欄位對照的審核工作流：提交 -> 核准（同來源舊版自動作廢）-> 查詢現行版。

datasource_specs 只讀「已核准」的對照，不管審核狀態。
"""
from aiops.commands.base import BaseCommand, CommandConflict, CommandNotFound
from aiops.domains.ingestion.dao import MappingDAO
from aiops.domains.ingestion.schemas import MappingSubmitRequest


class SubmitMappingCommand(BaseCommand):
    def __init__(self, req: MappingSubmitRequest):
        self.req = req

    def validate(self) -> None:
        pass  # 欄位形狀已由 schema 驗證

    def run(self) -> dict:
        self.validate()
        return MappingDAO.create({**self.req.model_dump(), "status": "pending", "approved_by": None})


class ApproveMappingCommand(BaseCommand):
    def __init__(self, mapping_id: int, approver: str):
        self.mapping_id = mapping_id
        self.approver = approver

    def validate(self) -> None:
        self.row = MappingDAO.get(self.mapping_id)
        if self.row is None:
            raise CommandNotFound(f"找不到欄位對照 {self.mapping_id}")
        if self.row["status"] != "pending":
            raise CommandConflict(f"欄位對照 {self.mapping_id} 狀態為 {self.row['status']}，只有 pending 能核准")

    def run(self) -> dict:
        self.validate()
        for old in MappingDAO.list_by(self.row["source_type"], "approved"):
            MappingDAO.update(old["id"], status="superseded")
        return MappingDAO.update(self.mapping_id, status="approved", approved_by=self.approver)


class GetApprovedMappingCommand(BaseCommand):
    def __init__(self, source_type: str):
        self.source_type = source_type

    def validate(self) -> None:
        pass

    def run(self) -> dict:
        rows = MappingDAO.list_by(self.source_type, "approved")
        if not rows:
            raise CommandNotFound(f"{self.source_type} 沒有已核准的欄位對照")
        return rows[0]
