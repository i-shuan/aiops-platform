from fastapi import APIRouter

from aiops.domains.rca.commands.generate import GenerateRCACommand
from aiops.domains.rca.schemas import GenerateRCARequest

router = APIRouter(prefix="/api/v1/rca", tags=["rca"])


@router.post("/generate")
def generate_rca(req: GenerateRCARequest):
    return {"result": GenerateRCACommand(req).run()}
