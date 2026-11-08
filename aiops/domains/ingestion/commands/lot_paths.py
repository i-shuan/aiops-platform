from collections import Counter, defaultdict

from aiops.commands.base import BaseCommand
from aiops.domains.ingestion.schemas import LotPathsMergeRequest, LotStepIn


def _minutes(step: LotStepIn) -> float:
    return round((step.end - step.start).total_seconds() / 60, 2)


class MergeLotPathsCommand(BaseCommand):
    """工廠 Lot 履歷合成：把每個 Lot 的站點紀錄依時間排成一條路徑，並統計機台之間的流向。

    輸出的 edges（機台 A -> 機台 B，經過的 Lot 數）就是工廠情境的「拓撲」，
    之後交給 lot_path_chain 這類 Grouper 當 context 用（跟 IT 的服務拓撲同一個用法）。
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

        lots, edges = [], Counter()
        for lot_id in sorted(by_lot):
            steps = sorted(by_lot[lot_id], key=lambda s: (s.start, s.end))
            # 同一 Lot 前一站還沒結束、下一站就開始：資料品質問題，標出來但不丟掉
            overlaps = sum(1 for a, b in zip(steps, steps[1:]) if b.start < a.end)
            path = [s.equipment for s in steps]
            edges.update((a, b) for a, b in zip(path, path[1:]) if a != b)
            lots.append({
                "lot_id": lot_id,
                "path": path,
                "stations": [s.station for s in steps],
                "start": steps[0].start.isoformat(),
                "end": max(s.end for s in steps).isoformat(),
                "total_minutes": round((max(s.end for s in steps) - steps[0].start).total_seconds() / 60, 2),
                "overlapping_steps": overlaps,
                "steps": [{"station": s.station, "equipment": s.equipment, "recipe": s.recipe,
                           "start": s.start.isoformat(), "end": s.end.isoformat(), "minutes": _minutes(s)}
                          for s in steps],
            })

        return {
            "lots": lots,
            "edges": [{"from": a, "to": b, "lots": n} for (a, b), n in sorted(edges.items(), key=lambda kv: -kv[1])],
            # 直接給 alerts 的 context.topology 用：[[上游機台, 下游機台], ...]
            "topology": [[a, b] for (a, b) in sorted(edges)],
        }
