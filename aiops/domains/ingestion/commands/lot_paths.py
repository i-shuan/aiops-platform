from collections import defaultdict

from aiops.commands.base import BaseCommand
from aiops.domains.ingestion.dao import LotPathDAO
from aiops.domains.ingestion.schemas import LotPathsMergeRequest, LotStepIn
from aiops.plugins.base import lot_edges_from_paths


def _minutes(step: LotStepIn) -> float:
    return round((step.end - step.start).total_seconds() / 60, 2)


class MergeLotPathsCommand(BaseCommand):
    """工廠 Lot 履歷合成：把每個 Lot 的站點紀錄依時間排成一條路徑，並統計機台之間的流向（lot_edges）。

    存起來之後，alerts 的 lot_path_chain 收斂與 rca 的根因推論在沒給 lot_edges 時會自動從已存的路徑推導。
    注意：流向的傳播方向是「上游 -> 下游」，跟 IT 的 topology（[caller, callee]，傳播是 callee -> caller）相反，
    所以一律放在 lot_edges，不要塞進 topology。
    """

    def __init__(self, req: LotPathsMergeRequest):
        self.req = req

    def validate(self) -> None:
        pass  # 每筆站點紀錄的時間順序已由 schema 驗證

    def run(self) -> dict:
        self.validate()
        by_lot: dict[str, list[LotStepIn]] = defaultdict(list)
        for step in self.req.steps:
            by_lot[step.lot_id].append(step)

        lots = []
        for lot_id in sorted(by_lot):
            steps = sorted(by_lot[lot_id], key=lambda s: (s.start, s.end))
            end = max(s.end for s in steps)
            lots.append({
                "lot_id": lot_id,
                "path": [s.equipment for s in steps],
                "stations": [s.station for s in steps],
                "start": steps[0].start,
                "end": end,
                "total_minutes": round((end - steps[0].start).total_seconds() / 60, 2),
                # 同一 Lot 前一站還沒結束、下一站就開始：資料品質問題，標出來但不丟掉
                "overlapping_steps": sum(1 for a, b in zip(steps, steps[1:]) if b.start < a.end),
                "steps": [{"station": s.station, "equipment": s.equipment, "recipe": s.recipe,
                           "start": s.start.isoformat(), "end": s.end.isoformat(), "minutes": _minutes(s)}
                          for s in steps],
            })

        if self.req.persist:
            LotPathDAO.upsert_many(lots)
        return {
            "lots": [{**lot, "start": lot["start"].isoformat(), "end": lot["end"].isoformat()} for lot in lots],
            "lot_edges": lot_edges_from_paths([lot["path"] for lot in lots]),
            "persisted": self.req.persist,
        }
