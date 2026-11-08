"""建議處置：依指標種類給規則式、可預期的建議；有根因候選時先指出要優先處理的源頭。"""
from __future__ import annotations

from aiops.core.registry import OperatorRegistry
from aiops.operators.rca.base import RcaSection

# (指標名稱關鍵字, 建議)。依指標名稱比對，命中幾條就列幾條；都沒命中就給通用建議
_RULES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("latency", "duration", "p95", "p99", "slow"), "檢查下游依賴與慢查詢，並比對同時間的部署或設定變更"),
    (("error", "5xx", "fail", "exception"), "查看錯誤日誌與最近一次部署，必要時回滾"),
    (("cpu", "saturation", "load", "memory", "mem", "oom"), "確認資源用量與限制（limits、HPA），必要時擴容"),
    (("disk", "io", "iops", "inode"), "檢查磁碟容量與 I/O 等待"),
    (("traffic", "qps", "rps", "request"), "確認流量來源是否異常（活動、爬蟲、重試風暴）"),
    (("rf_", "power", "pressure", "temp", "vacuum", "flow"), "檢查機台保養（PM）紀錄與近期配方（recipe）變更"),
)
_GENERIC = "比對事件時間前後的變更紀錄（部署、設定、配方）"


@OperatorRegistry.register("rca.section.suggested_actions")
class SuggestedActions(RcaSection):
    key, title = "suggested_actions", "建議處置"

    def build(self, incident, ctx):
        actions: list[str] = []
        if ctx.rca_root_candidates:
            actions.append(f"優先處理可能的源頭：{', '.join(c.entity for c in ctx.rca_root_candidates[:3])}")
        names = " ".join(f"{e.metric} {e.attrs.get('signal', '')}" for e in incident.events).lower()
        hits = [action for keywords, action in _RULES if any(k in names for k in keywords)]
        actions += hits or [_GENERIC]
        return "\n".join(f"{i}. {a}" for i, a in enumerate(actions, 1))
