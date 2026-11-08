from aiops.core.context import PipelineContext
from aiops.core.registry import OperatorRegistry
from aiops.datasource_specs import check_sources
from aiops.operators.base import BaseOperator


@OperatorRegistry.register("admin.sources_check")
class SourcesCheck(BaseOperator):
    """資料源連線自我檢查：每個資料源有沒有設定、連不連得上、延遲多少。不會丟例外，結果全部列出來。"""

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        results = check_sources()
        failing = [r["source_type"] for r in results if r["configured"] and not r["ok"]]
        ctx.result = {"ok": not failing, "failing": failing, "sources": results}
        return ctx
