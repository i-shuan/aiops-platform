from aiops.commands.base import BaseCommand
from aiops.datasource_specs import check_sources


class CheckSourcesCommand(BaseCommand):
    """資料源連線自我檢查：每個資料源有沒有設定、連不連得上、延遲多少。不會丟例外，結果全部列出來。"""

    def validate(self) -> None:
        pass

    def run(self) -> dict:
        results = check_sources()
        failing = [r["source_type"] for r in results if r["configured"] and not r["ok"]]
        return {"ok": not failing, "failing": failing, "sources": results}
