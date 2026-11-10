"""證據融合根因（rca.root_cause.evidence）：用這一輪隨需撈回來的 span（ctx.rca_spans）判斷「誰自己變慢、誰自己出錯」。

拓撲排序與 PageRank 假設異常一律從被呼叫端往呼叫端傳，碰到「上游把流量放大、壓垮下游」（load_amplification）就排反；
時序互相關在大家同一分鐘一起變慢時排不出先後。這個算子改看自身耗時（span 耗時扣掉等下游的時間，見 stages/rca/spans.py）：
被拖累的服務總耗時會漲，但自身耗時不太漲；真正出問題的服務，自身耗時（或自身錯誤）漲最多。

只看自身耗時還不夠：上游把流量放大（重試風暴、呼叫次數暴增）時，下游會因為排隊，自身耗時也真的漲。
所以先做「流量放大歸因」：
  1. 每個服務的流入流量倍數 load = 事件期間每分鐘入口請求 ÷ 事件前每分鐘入口請求（從 span 的父子關係數，見 spans.inbound_callers）
  2. load >= min_load_ratio 的服務是「被灌流量」的；往上游找（沿主要呼叫者），找到第一個自己流量沒漲的呼叫者 = 放大者
  3. 被灌流量的服務，自身耗時增量與自身錯誤率增量中 (1 − 1/load) 的比例視為流量造成，轉記到放大者身上
     （例：流量 2 倍 -> 一半算在放大者）。這是近似：排隊延遲對負載不是線性的，理由文字會寫明轉了多少，讓人自己判斷
  沒有流量變化時（被呼叫端自己變慢、共同外部因素）這一步什麼都不做。

扇出（指名「流量放大者」的判準，attribution["amplifier"]，Runbook RB-08 靠它）：
  扇出 = 呼叫者→被呼叫者的呼叫數 ÷ 呼叫者自己的入口請求數（每處理一個請求打下游幾次）。事件期間 ÷ 事件前 >= min_fanout_ratio，
  而且受害者自己的自身耗時或自身錯誤率增量過了下面的顯著門檻，才在放大者的候選上標 attribution["amplifier"]。
  1. 上面第 2 步往上游走的時候，呼叫者自己流量也漲了、但它對這個下游的扇出也漲過門檻：它就是放大者，不再往上走
     （外部流量漲 + 它又重試放大；這時只轉 (1 − 1/扇出倍數) 的增量，其餘是外部流量的份）
  2. 放大者不在事件裡（它自己沒有症狀，沒有告警）：扇出判準成立才替它補一列候選並照轉分數；
     不成立就跟以前一樣只寫在受害者的理由裡、不轉分數（它的入口請求不足 min_requests 也補不了）
  3. 流量是外部入口打進來的（往上走到頭是沒有父 span 的請求）：沒有內部放大者，不轉分數，
     在被灌流量的候選上標 attribution["ingress_surge"]（Runbook RB-04 靠它）
  死區（都留有測試，tests/test_rca_amplifiers.py）：
    a. 透過訊息佇列放大（生產者把訊息量放大）：非同步邊的呼叫者是 mq:<生產者>，不算流量倍數，不指名
    b. 事件前這條呼叫邊不到 min_requests 筆（新上線的呼叫路徑）：算不出扇出倍數，不標 amplifier（流量轉記仍照舊規則做）

訊息佇列（非同步邊，ctx.rca_mq；ingestion.incident_traces 開了 follow_messaging 才有，沒有就整段不做、輸出一字不差）：
  事件實體消費的訊息是更早之前發的，生產者有沒有問題要回到「這批訊息的發布時窗」看（MqEdge.pub_start~pub_end），
  對照組是發布時窗之前等長的一段——不是事件時窗、也不是事件前。
  1. 生產者在發布時窗的自身耗時或自身錯誤率增量過了跟下面一樣的顯著門檻，才替它補一列候選（它通常不在事件裡，
     所以這是唯一會出現「事件實體以外」候選的地方）；沒過門檻不補列，只在消費者的理由裡寫「看過了、沒異常」
  2. 訊息夾帶失敗轉記：生產者過了門檻，而且消費者事件期間「自己失敗」的入口有 >= MIN_MESSAGE_SHARE 是在消費這條邊的訊息，
     消費者的自身錯誤率增量 × 那個佔比 × async_weight 轉記給生產者。只轉錯誤、不轉耗時：消費者變慢是它自己或它的依賴的事
     （佇列積壓看延遲 P95，寫在理由與 attribution["async"]，不進分數）
  這是推論不是證明：trace 只能證明「訊息是它發的、它當時有異常」，不能證明訊息內容有問題，理由文字照實寫。
  死區（都留有測試）：
    a. 毒訊息但生產者自己沒有任何症狀（耗時、錯誤都正常）-> 不補列、不轉記，第一名仍是消費者
    b. 生產者在對照段（發布時窗之前）就已經壞了 -> 看不出增量，不補列
    c. 消費者自己的 bug 剛好碰上生產者真的有異常 -> 會被轉記（async_weight 設 0 可以關掉轉記做對照）

分數（每個特徵先除以候選中的最大值，歸一到 0~1；用的是歸因後的增量）：
  自身耗時增量（事件期間 − 事件前，ms／請求）       權重 self_weight（預設 1，主分；只為消融對照保留，不要改）
  自身錯誤率增量（不含下游連帶的失敗）             權重 error_weight（預設 0.5）
  日誌暴增倍率（rca.logs.service_burst）          權重 log_weight（預設 0：下游被拖垮時噴的錯最多，加權會把受害者推上來；只當佐證寫在理由裡）
同分時依首次異常時間。拓撲不參與計分：「被呼叫端拖垮呼叫端」與「呼叫端放大流量壓垮被呼叫端」方向相反，固定方向的拓撲規則必定在其中一種排錯。

沒有 span 時分兩種：
  - trace 來源有設定、但這一輪連不上或讀失敗（ctx.rca_traces_failed，例如 ES 重啟）：證據被吃掉了，證據不足、不指認，
    不拿拓撲硬猜（大家一起變慢時拓撲一定會挑一個下游出來頂罪）
  - 這個場景本來就沒有 trace（工廠、沒設定 trace 來源）：退回 fallback 算子（預設 rca.root_cause.topological，依 Lot 流向或
    呼叫拓撲推測），並在 ctx.rca_root_note 寫明「沒有 trace 證據、以下是拓撲推測」。fallback 設成空字串就一律證據不足。

證據不足時不指認（ctx.rca_root_note 寫明原因，報告的「可能的源頭」段落會照出）：
  1. span 沒涵蓋事件前、或事件前／事件期間的入口請求太少（< min_requests）
  2. 歸因後沒有任何候選的自身耗時增量 >= max(min_self_increase_ms, 基準 × min_self_increase_ratio)，
     也沒有自身錯誤率增量 >= min_error_increase
  3. 第一名分數 < 第二名 × min_dominance：幾個服務一起變慢、程度相近（共同外部因素的典型樣子，例如資料源或網路抖動）
"""
from __future__ import annotations

from datetime import timedelta

from aiops.engine.context import RootCandidate
from aiops.engine.registry import OperatorRegistry
from aiops.engine.types import as_utc
from aiops.stages.rca.base import RootCauseOperator, first_seen
from aiops.stages.rca.logs import service_burst
from aiops.stages.rca.mq import async_attribution, lag_text, producer_windows, window_text
from aiops.stages.rca.spans import EXTERNAL_CALLER, UNKNOWN_CALLER, inbound_callers, resource_kinds, window_stats

MAX_HOPS = 10
# 訊息夾帶失敗轉記的門檻：消費者事件期間自己失敗的入口，至少這個比例是在消費同一條邊的訊息才轉。
# 不到一半代表它大部分的失敗跟這批訊息無關（同步請求也在失敗），不能把錯記到生產者頭上
MIN_MESSAGE_SHARE = 0.5
# attribution["others"]：主導資源以外，等待增量佔自身耗時增量這個比例以上的資源也列出來（1 成以下當雜訊）
OTHER_RESOURCE_SHARE = 0.1


@OperatorRegistry.register("rca.root_cause.evidence")
class EvidenceRootCause(RootCauseOperator):
    """params: self_weight（1.0）、error_weight（0.5）、log_weight（0.0）、min_requests（20）、min_self_increase_ms（5）、
    min_self_increase_ratio（0.2）、min_error_increase（0.05）、min_dominance（1.1）、min_load_ratio（1.5）、
    min_dependency_share（0.6：候選的自身耗時增量裡，單一下游資源的等待增量佔比達到就填 dependency 下鑽；
    或入口失敗裡該資源失敗的佔比達到也填）、
    fallback（"rca.root_cause.topological"；沒有 trace 的場景改用它，空字串 = 不退回）、
    async_weight（1.0；訊息夾帶失敗轉記的比例，0 = 不轉記，只為消融對照）、
    min_lag_seconds（60；訊息延遲 P95 到這個秒數、而且比事件前多一倍才標成積壓，見 stages/rca/mq.is_backlog）、
    min_fanout_ratio（2.0；呼叫者對下游的扇出，事件期間 ÷ 事件前達到這個倍數才指名為流量放大者，見模組說明）。

    dependency 不改分數、不改排名：候選仍是服務（拓撲、傳播路徑、標準答案都不變），只多告訴人「該查的是它等的東西」。"""

    def __init__(self, self_weight: float = 1.0, error_weight: float = 0.5, log_weight: float = 0.0,
                 min_requests: int = 20, min_self_increase_ms: float = 5.0, min_self_increase_ratio: float = 0.2,
                 min_error_increase: float = 0.05, min_dominance: float = 1.1, min_load_ratio: float = 1.5,
                 min_dependency_share: float = 0.6, fallback: str = "rca.root_cause.topological",
                 async_weight: float = 1.0, min_lag_seconds: float = 60.0, min_fanout_ratio: float = 2.0):
        if min(self_weight, error_weight, log_weight, min_self_increase_ms, min_self_increase_ratio, min_error_increase) < 0:
            raise ValueError("權重與門檻不能是負的")
        if min_requests < 1 or min_dominance < 1 or min_load_ratio <= 1:
            raise ValueError("min_requests 至少 1；min_dominance 至少 1（1 = 不做拉開檢查）；min_load_ratio 必須大於 1")
        if not 0 < min_dependency_share <= 1:
            raise ValueError("min_dependency_share 必須在 (0, 1]")
        self.min_dependency_share = float(min_dependency_share)
        if not 0 <= async_weight <= 1 or min_lag_seconds < 0:
            raise ValueError("async_weight 必須在 [0, 1]（0 = 不做訊息夾帶失敗轉記）；min_lag_seconds 不能是負的")
        self.async_weight, self.min_lag_seconds = float(async_weight), float(min_lag_seconds)
        if min_fanout_ratio <= 1:
            raise ValueError("min_fanout_ratio 必須大於 1（扇出沒變大就不是放大）")
        self.min_fanout_ratio = float(min_fanout_ratio)
        self.self_weight, self.error_weight, self.log_weight = float(self_weight), float(error_weight), float(log_weight)
        self.min_requests = int(min_requests)
        self.min_self_ms, self.min_self_ratio = float(min_self_increase_ms), float(min_self_increase_ratio)
        self.min_error_increase = float(min_error_increase)
        self.min_dominance = float(min_dominance)
        self.min_load_ratio = float(min_load_ratio)
        if fallback and (not fallback.startswith("rca.root_cause.") or fallback == "rca.root_cause.evidence"):
            raise ValueError("fallback 必須是另一個 rca.root_cause.* 算子（或空字串）")
        if fallback:
            OperatorRegistry.get(fallback)  # 載入期就檢查拼字，不要跑到沒有 trace 的那一次才發現
        self.fallback = fallback

    def _insufficient(self, ctx, why: str) -> list[RootCandidate]:
        ctx.rca_root_note = f"證據不足，不指認根因：{why}"
        ctx.log(f"root_cause:evidence:insufficient:{why[:60]}")
        return []

    def _fanout(self, before, during, caller: str, callee: str) -> dict | None:
        """caller 每處理一個入口請求呼叫 callee 幾次：事件前、事件期間與兩者的倍數。任一邊樣本不足（含事件前這條邊太少）回 None。"""
        nb, nd = sum(before.get(caller, {}).values()), sum(during.get(caller, {}).values())
        cb, cd = before.get(callee, {}).get(caller, 0), during.get(callee, {}).get(caller, 0)
        if min(nb, nd, cb) < self.min_requests:
            return None
        fb, fd = cb / nb, cd / nd
        return {"via": callee, "before": fb, "during": fd, "ratio": fd / fb}

    def _amplifiers(self, spans, first_ts, start, end, base_min, dur_min):
        """回 (load, amplifier, fan, ingress)：
        load      每個服務的流入流量倍數
        amplifier 被灌流量的服務 -> 放大者（往上游走到第一個自己流量沒漲、或自己對下游扇出暴增的呼叫者）
        fan       被灌流量的服務 -> 放大者那一跳的扇出（_fanout；算不出來就沒有這個鍵）；flooded = 放大者自己流量也漲了
        ingress   被灌流量的服務 -> 外部入口服務（流量是外面打進來的，沒有內部放大者）"""
        before, during = inbound_callers(spans, first_ts, start), inbound_callers(spans, start, end)
        load: dict[str, float] = {}
        for svc in set(before) | set(during):
            nb, nd = sum(before.get(svc, {}).values()), sum(during.get(svc, {}).values())
            if nb >= self.min_requests and nd >= self.min_requests:
                load[svc] = (nd / dur_min) / (nb / base_min)
        amplifier: dict[str, str] = {}
        fan: dict[str, dict] = {}
        ingress: dict[str, str] = {}
        for svc, ratio in load.items():
            if ratio < self.min_load_ratio:
                continue
            current, hops = svc, 0
            while hops < MAX_HOPS:
                callers = {c: n for c, n in during.get(current, {}).items() if c not in (EXTERNAL_CALLER, UNKNOWN_CALLER)}
                if not callers:
                    # 流量來自外部入口（或呼叫者不在樣本裡，不知道是誰）：不能指認哪個服務放大的。
                    # 確定是外部打進來的（沒有父 span 的請求不少於孤兒）才記成入口突波
                    external = during.get(current, {}).get(EXTERNAL_CALLER, 0)
                    if external and external >= during.get(current, {}).get(UNKNOWN_CALLER, 0):
                        ingress[svc] = current
                    break
                caller = max(callers, key=callers.get)
                fo = self._fanout(before, during, caller, current)
                flooded = load.get(caller, 0.0) >= self.min_load_ratio
                if flooded and not (fo and fo["ratio"] >= self.min_fanout_ratio):
                    current, hops = caller, hops + 1  # 呼叫者自己也被灌流量、扇出沒變：它只是轉手，再往上找
                    continue
                if caller in load:  # 呼叫者流量沒漲（或漲了但扇出也暴增），卻讓下游流量漲了：放大者
                    amplifier[svc] = caller
                    if fo:
                        fan[svc] = {**fo, "flooded": flooded}
                break
        return load, amplifier, fan, ingress

    def _significant(self, r) -> bool:
        """這一列自己（轉記前）的自身耗時或自身錯誤率增量過了顯著門檻。"""
        return (r["self_raw"] >= max(self.min_self_ms, r["b"].self_ms_per_request * self.min_self_ratio)
                or r["err_raw"] >= self.min_error_increase)

    def _amplified(self, r, fo: dict | None) -> bool:
        """指名流量放大者的判準：扇出倍數過門檻，而且受害者自己真的變慢或出錯。"""
        return bool(fo) and fo["ratio"] >= self.min_fanout_ratio and self._significant(r)

    def _amplifier(self, r, attribution: dict, load: dict[str, float], ingress: dict[str, str]) -> dict:
        """attribution 補上 "amplifier"（這個候選把下游流量放大了）或 "ingress_surge"（它的流量是外部入口打進來的）。都沒有就原樣回傳。"""
        hits = r.get("amp_of") or []
        if hits:
            victim, ms, err, fo = max(hits, key=lambda h: (h[1], h[2]))
            attribution = {**attribution, "amplifier": {
                "type": "traffic_amplifier", "victim": victim, "via": fo["via"], "load_ratio": round(load[victim], 2),
                "fanout": {"before": round(fo["before"], 2), "during": round(fo["during"], 2)},
                "fanout_ratio": round(fo["ratio"], 2), "moved_self_ms": round(ms, 1), "moved_error": round(err, 3),
                "victims": sorted(h[0] for h in hits), "in_incident": not r.get("amp_added")}}
        entry = ingress.get(r["entity"])
        if entry is not None:
            attribution = {**attribution, "ingress_surge": {"entry": entry, "load_ratio": round(load[r["entity"]], 2),
                                                           "entry_load_ratio": round(load[entry], 2)}}
        return attribution

    def _messaging(self, ctx, spans, rows: dict[str, dict]) -> None:
        """訊息佇列歸因（見模組說明）。原地改 rows：消費者列掛上它的邊與說明；生產者在發布時窗真的有異常時補一列，
        並把消費者自身錯誤率增量中「訊息夾帶」的部分轉記給它。"""
        err_base: dict[str, float] = {}  # 消費者 -> 轉記前的自身錯誤率增量（多條邊都拿同一個基數算，不會越轉越少）
        for edge in ctx.rca_mq:
            r = rows.get(edge.consumer)
            if r is None or "pub" in r:
                continue
            r.setdefault("mq", []).append(edge)
            queue = edge.queue or "（佇列名稱未知）"
            if not edge.producer:
                r.setdefault("mq_notes", []).append(f"佇列 {queue} 的訊息只有 message.age、不知道是誰發的，沒有回頭看生產者")
                continue
            if edge.producer == edge.consumer:
                continue
            window = window_text(edge.pub_start, edge.pub_end)
            pb, pd = producer_windows(spans, edge)
            if pb is None or pd is None or pb.requests < self.min_requests or pd.requests < self.min_requests:
                r.setdefault("mq_notes", []).append(
                    f"訊息生產者 {edge.producer} 在發布時窗 {window}（或它之前等長的對照段）的入口請求不足 {self.min_requests} 筆，"
                    "無法比較它當時有沒有異常")
                continue
            self_inc = pd.self_ms_per_request - pb.self_ms_per_request
            err_inc = pd.own_error_rate - pb.own_error_rate
            numbers = (f"自身耗時 {pb.self_ms_per_request:.1f}→{pd.self_ms_per_request:.1f} ms、"
                       f"自身錯誤率 {pb.own_error_rate:.0%}→{pd.own_error_rate:.0%}")
            if not (self_inc >= max(self.min_self_ms, pb.self_ms_per_request * self.min_self_ratio)
                    or err_inc >= self.min_error_increase):
                note = f"訊息生產者 {edge.producer}（佇列 {queue}）在發布時窗 {window} {numbers}，未見異常，不轉記"
                if edge.failed and r["err_raw"] >= self.min_error_increase:
                    # 生產者沒有症狀 ≠ 它發出去的內容沒問題：trace 只有耗時與錯誤，看不到 payload。照實講，交給人查。
                    note += (f"；但 {edge.consumer} 處理這批訊息有 {edge.failed} 則失敗，trace 看不到訊息內容，"
                             f"無法排除是 {edge.producer} 發出的內容有問題，請人工抽查失敗訊息與 dead-letter queue")
                    r["mq_suspect"] = True
                r.setdefault("mq_notes", []).append(note)
                continue
            target = rows.get(edge.producer)
            if target is None:
                target = rows[edge.producer] = {"entity": edge.producer, "b": pb, "d": pd, "self_raw": self_inc,
                                                "err_raw": err_inc, "moved_in": [], "moved_out": None,
                                                "self_eff": self_inc, "err_eff": err_inc, "pub": edge}
            elif "pub" not in target:  # 生產者本來就在事件裡：主列看的是事件時窗，發布時窗的數字補在說明
                target.setdefault("mq_notes", []).append(
                    f"它在訊息發布時窗 {window}（{edge.consumer} 事後消費的那批，佇列 {queue}）{numbers}")
            d = r["d"]
            own_failures = round(d.own_error_rate * d.requests)
            share = min(edge.failed / own_failures, 1.0) if own_failures else 0.0
            base = err_base.setdefault(edge.consumer, max(r["err_eff"], 0.0))
            if share < MIN_MESSAGE_SHARE or not self.async_weight:
                continue
            moved = min(base * share * self.async_weight, max(r["err_eff"], 0.0))
            if moved > 0:
                r["err_eff"] -= moved
                target["err_eff"] += moved
                r.setdefault("mq_out", []).append((edge, moved, share))
                target.setdefault("mq_in", []).append((edge, moved))

    def _async(self, r, attribution: dict) -> dict:
        """attribution 補上 "async"：這個候選是訊息生產者（補列或承接了轉記）還是消費者。沒有非同步邊就原樣回傳。"""
        mq_in, mq_out = r.get("mq_in") or [], r.get("mq_out") or []
        if "pub" in r or mq_in:
            edge = r.get("pub") or mq_in[0][0]
            return {**attribution, "async": async_attribution(edge, "producer", self.min_lag_seconds, sum(x[1] for x in mq_in))}
        if r.get("mq"):
            return {**attribution, "async": async_attribution(r["mq"][0], "consumer", self.min_lag_seconds,
                                                             sum(x[1] for x in mq_out), bool(r.get("mq_suspect")))}
        return attribution

    def rank(self, incident, ctx):
        spans = ctx.rca_spans
        if not spans:
            if ctx.rca_traces_failed:
                return self._insufficient(ctx, "trace 資料源這一輪連不上或讀取失敗（見 errors），沒有自身耗時可以比較；"
                                               "不改用拓撲推測，避免在資料源出問題時把下游服務當成根因")
            if not self.fallback:
                return self._insufficient(ctx, "這一輪沒有撈到 trace（span），無法比較各服務的自身耗時")
            ranked = OperatorRegistry.create(self.fallback, {}).rank(incident, ctx)
            name = self.fallback.removeprefix("rca.root_cause.")
            ctx.rca_root_note = (f"這一輪沒有 trace 證據，以上改用 {name} 依拓撲／Lot 流向推測，不是自身耗時判斷"
                                 "（拓撲假設異常從被呼叫端往呼叫端傳，碰到上游流量放大會排反）" if ranked
                                 else f"證據不足，不指認根因：這一輪沒有 trace，{name} 也排不出（沒有拓撲或 Lot 流向）")
            return ranked
        start, end = as_utc(incident.start), as_utc(incident.end) + timedelta(minutes=1)
        # 補撈過生產者發布時段時，手上最早的 span 是幾十分鐘前的；事件前基準仍從主撈取窗算起
        first_ts = as_utc(ctx.rca_main_start) if ctx.rca_main_start else min(as_utc(s.ts) for s in spans)
        if first_ts >= start:
            return self._insufficient(ctx, "撈到的 span 沒有涵蓋事件開始之前，沒有基準可以比較（把 incident_traces 的 before_minutes 加大）")
        before, during = window_stats(spans, first_ts, start), window_stats(spans, start, end)
        base_min = (start - first_ts).total_seconds() / 60
        dur_min = (end - start).total_seconds() / 60

        rows: dict[str, dict] = {}
        for entity in sorted(set(incident.entities)):
            b, d = before.get(entity), during.get(entity)
            if not b or not d or b.requests < self.min_requests or d.requests < self.min_requests:
                continue
            rows[entity] = {"entity": entity, "b": b, "d": d,
                            "self_raw": d.self_ms_per_request - b.self_ms_per_request,
                            "err_raw": d.own_error_rate - b.own_error_rate, "moved_in": [], "moved_out": None}
        if not rows:
            return self._insufficient(ctx, f"事件裡的服務在事件前（{base_min:.0f} 分鐘）或事件期間的入口請求都少於 "
                                           f"{self.min_requests} 筆，trace 不足以比較")

        # 流量放大歸因：被灌流量的服務，增量中 (1 - 1/load) 轉給放大者
        load, amplifier, fan, ingress = self._amplifiers(spans, first_ts, start, end, base_min, dur_min)
        for entity in list(rows):  # 放大者不在事件裡：扇出判準成立、它的入口請求也夠，才替它補一列
            amp = amplifier.get(entity)
            if amp is None or amp in rows or not self._amplified(rows[entity], fan.get(entity)):
                continue
            b, d = before.get(amp), during.get(amp)
            if b and d and b.requests >= self.min_requests and d.requests >= self.min_requests:
                rows[amp] = {"entity": amp, "b": b, "d": d, "self_raw": d.self_ms_per_request - b.self_ms_per_request,
                             "err_raw": d.own_error_rate - b.own_error_rate, "moved_in": [], "moved_out": None,
                             "amp_added": True}
        for r in rows.values():
            r["self_eff"], r["err_eff"] = r["self_raw"], r["err_raw"]
        for entity, r in rows.items():
            amp = amplifier.get(entity)
            if amp is None or amp not in rows:
                if amp is not None:
                    r["moved_out"] = (amp, 0.0, 0.0, load[entity])  # 放大者不在事件裡、也補不了列：只寫在理由裡，不轉分數
                continue
            fo = fan.get(entity)
            # 放大者自己流量也漲了（靠扇出才指名的）：下游的流量倍數裡有外部流量的份，只轉扇出造成的那一份
            ratio = min(load[entity], fo["ratio"]) if fo and fo["flooded"] else load[entity]
            share = 1 - 1 / ratio
            moved_self, moved_err = max(r["self_raw"], 0.0) * share, max(r["err_raw"], 0.0) * share
            r["self_eff"] -= moved_self
            r["err_eff"] -= moved_err
            r["moved_out"] = (amp, moved_self, moved_err, load[entity])
            rows[amp]["self_eff"] += moved_self
            rows[amp]["err_eff"] += moved_err
            rows[amp]["moved_in"].append((entity, moved_self, load[entity]))
            if fo:
                r["fan_out"] = fo
            if self._amplified(r, fo):
                rows[amp].setdefault("amp_of", []).append((entity, moved_self, moved_err, fo))
        for entity, entry in ingress.items():
            if entity in rows:
                rows[entity]["ingress"] = (entry, load[entity])

        if ctx.rca_mq:
            self._messaging(ctx, spans, rows)

        for r in rows.values():
            b = r["b"]
            r["hit"] = (r["self_eff"] >= max(self.min_self_ms, b.self_ms_per_request * self.min_self_ratio)
                        or r["err_eff"] >= self.min_error_increase)
        ordered_rows = list(rows.values())
        if not any(r["hit"] for r in ordered_rows):
            summary = "、".join(f"{r['entity']} {r['self_raw']:+.1f} ms" for r in ordered_rows)
            return self._insufficient(ctx, f"沒有任何服務的自身耗時或自身錯誤率明顯上升（{summary}）。"
                                           "變慢的時間不在這些服務自己身上，可能是共同的外部因素（資料源、網路、未被追蹤的依賴）")

        bursts = service_burst(ctx.rca_logs, ctx.rca_logs_baseline, ctx.rca_log_window_minutes) if ctx.rca_logs else {}
        max_self = max((r["self_eff"] for r in ordered_rows if r["self_eff"] > 0), default=0.0)
        max_err = max((r["err_eff"] for r in ordered_rows if r["err_eff"] > 0), default=0.0)
        max_log = max((bursts[r["entity"]]["ratio"] for r in ordered_rows if r["entity"] in bursts), default=0.0)
        seen = first_seen(incident)
        for r in ordered_rows:
            r["s_self"] = max(r["self_eff"], 0.0) / max_self if max_self else 0.0
            r["s_err"] = max(r["err_eff"], 0.0) / max_err if max_err else 0.0
            r["s_log"] = bursts[r["entity"]]["ratio"] / max_log if max_log and r["entity"] in bursts else 0.0
            r["score"] = self.self_weight * r["s_self"] + self.error_weight * r["s_err"] + self.log_weight * r["s_log"]
        ordered_rows.sort(key=lambda r: (-r["score"], seen.get(r["entity"], (incident.start, 0))[0], r["entity"]))

        top = ordered_rows[0]
        if len(ordered_rows) > 1 and ordered_rows[1]["score"] > 0 and top["score"] < ordered_rows[1]["score"] * self.min_dominance:
            close = "、".join(f"{r['entity']}（分數 {r['score']:.2f}，自身耗時 {r['self_eff']:+.1f} ms）"
                             for r in ordered_rows if r["score"] >= top["score"] / self.min_dominance)
            return self._insufficient(ctx, f"前幾名拉不開（第一名 < 第二名 × {self.min_dominance:g}）：{close}。"
                                           "幾個服務一起變慢且程度相近，常見於共同的外部因素，不指認單一服務")
        kinds = resource_kinds(spans)
        out = []
        for r in ordered_rows:
            dependency, attribution = self._dependency(r, kinds)
            out.append(RootCandidate(r["entity"], r["score"], self._reason(r, bursts, base_min, load, kinds),
                                     dependency, self._amplifier(r, self._async(r, attribution), load, ingress)))
        return out

    def _dependency(self, r, kinds: dict[str, str]) -> tuple[str, dict]:
        """單一下游資源佔了這個服務自身耗時增量（或入口失敗）的 min_dependency_share 以上，就是下鑽歸因。
        等待增量用 (事件期間 − 事件前) 的每請求 ms；失敗用事件期間「該資源失敗筆數 ÷ 入口失敗筆數」。兩個資源各佔一半就不填。
        回 (報告用的字串, 結構化 attribution)：attribution 是等待增量最大那個資源的數字，不夠主導時字串為空、by 為空。
        - 沒有任何資源「等待變長或失敗變多」（例如事件前有呼叫 redis、事件期間程式在入口就崩潰，一次都沒呼叫到）：
          回空的 attribution，報告不該出現一個跟這次事故無關的資源。
        - 其他也有份的資源（等待增量佔自身耗時增量 OTHER_RESOURCE_SHARE 以上，或失敗變多）列在 attribution["others"]，
          依佔比由大到小；沒有就不帶這個鍵。兩個資源各佔一半時，結構體兩個都看得到。"""
        b, d = r["b"], r["d"]
        resources = set(b.wait_ms_per_request) | set(d.wait_ms_per_request)
        if not resources:
            return "", {}
        inc = {res: d.wait_ms_per_request.get(res, 0.0) - b.wait_ms_per_request.get(res, 0.0) for res in resources}
        more_failures = {res: int(d.wait_failures.get(res, 0)) - int(b.wait_failures.get(res, 0)) for res in resources}
        top_res, top_ms = max(sorted(inc.items()), key=lambda kv: kv[1])
        if top_ms <= 0:  # 沒有資源變慢：改看誰的失敗變多；都沒有就是「資源跟這次事故無關」
            top_res, grew = max(sorted(more_failures.items()), key=lambda kv: kv[1])
            if grew <= 0:
                return "", {}
            top_ms = inc[top_res]
        by = ""
        if r["self_raw"] > 0 and top_ms > 0 and top_ms / r["self_raw"] >= self.min_dependency_share:
            by = "latency"
        else:
            entry_failures = round(d.error_rate * d.requests)
            if entry_failures and d.error_rate - b.error_rate >= self.min_error_increase:
                res, n = max(d.wait_failures.items(), key=lambda kv: kv[1], default=(None, 0))
                if res is not None and n / entry_failures >= self.min_dependency_share:
                    top_res, by = res, "errors"

        def _kind(res: str, k: int) -> str:
            parts = (kinds.get(res) or "").split("/")
            return parts[k] if len(parts) > k else ""

        def _share(res: str) -> float:
            return round(min(max(inc.get(res, 0.0) / r["self_raw"], 0.0), 1.0), 3) if r["self_raw"] > 0 else 0.0

        def _numbers(res: str) -> dict:
            return {
                "wait_ms": {"before": round(b.wait_ms_per_request.get(res, 0.0), 1),
                            "during": round(d.wait_ms_per_request.get(res, 0.0), 1)},
                "failures": {"before": int(b.wait_failures.get(res, 0)), "during": int(d.wait_failures.get(res, 0))},
                "share": _share(res),
            }

        attribution = {
            "resource": top_res, "kind": _kind(top_res, 0), "subtype": _kind(top_res, 1), "by": by,
            "self_ms": {"before": round(b.app_ms_per_request, 1), "during": round(d.app_ms_per_request, 1)},
            **_numbers(top_res),
        }
        others = [res for res in sorted(resources - {top_res})
                  if (inc[res] > 0 and _share(res) >= OTHER_RESOURCE_SHARE) or more_failures[res] > 0]
        if others:
            attribution["others"] = [
                {"resource": res, "kind": _kind(res, 0), "subtype": _kind(res, 1), **_numbers(res)}
                for res in sorted(others, key=lambda res: (-_share(res), res))]
        if not by:
            return "", attribution
        label = f"{top_res}（{kinds[top_res]}）" if kinds.get(top_res) else top_res
        return label, attribution

    @staticmethod
    def _breakdown(r, kinds: dict[str, str]) -> str:
        """理由裡的拆解句：程式自身 vs 等每個資源各增加多少、資源失敗幾筆。沒有資源葉節點就回空字串（mock 資料一字不多）。"""
        b, d = r["b"], r["d"]
        resources = sorted(set(b.wait_ms_per_request) | set(d.wait_ms_per_request),
                           key=lambda res: -(d.wait_ms_per_request.get(res, 0.0) - b.wait_ms_per_request.get(res, 0.0)))
        if not resources:
            return ""
        parts = [f"程式自身 {b.app_ms_per_request:.1f}→{d.app_ms_per_request:.1f} ms（{d.app_ms_per_request - b.app_ms_per_request:+.1f}）"]
        for res in resources:
            bw, dw = b.wait_ms_per_request.get(res, 0.0), d.wait_ms_per_request.get(res, 0.0)
            kind = f"，{kinds[res]}" if kinds.get(res) else ""
            parts.append(f"等待 {res}{kind} {bw:.1f}→{dw:.1f} ms（{dw - bw:+.1f}，資源失敗 "
                         f"{b.wait_failures.get(res, 0)}→{d.wait_failures.get(res, 0)} 筆）")
        return "；其中 " + "、".join(parts)

    def _reason(self, r, bursts, base_min, load, kinds: dict[str, str] | None = None) -> str:
        b, d = r["b"], r["d"]
        pub = r.get("pub")  # 訊息生產者補的列：數字是發布時窗的，不是事件時窗的
        if pub:
            width = (pub.pub_end - pub.pub_start).total_seconds() / 60
            scope = f"（訊息發布時窗 {window_text(pub.pub_start, pub.pub_end)} vs 它之前的 {width:.0f} 分鐘；不是事件時窗）"
        else:
            scope = f"（事件前 {base_min:.0f} 分鐘）"
        why = (f"自身耗時 {b.self_ms_per_request:.1f}→{d.self_ms_per_request:.1f} ms／請求（{r['self_raw']:+.1f}）；"
               f"自身錯誤率 {b.own_error_rate:.0%}→{d.own_error_rate:.0%}；入口請求 {b.requests}→{d.requests}"
               + scope)
        why += self._breakdown(r, kinds or {})
        if pub:
            why += (f"；這一列看的是訊息發布時窗：{pub.consumer} 在事件期間消費的訊息是它在這段時間發到佇列 "
                    f"{pub.queue or '（佇列名稱未知）'} 的（延遲 P95 {lag_text(pub.lag_ms_p95)}），所以回頭看它當時的狀況")
        elif r["entity"] in load:
            why += f"；流入流量 {load[r['entity']]:.2f} 倍"
        if r["moved_out"]:
            amp, ms, err, ratio = r["moved_out"]
            why += (f"；流量被 {amp} 放大到 {ratio:.2f} 倍，自身耗時增量中 {ms:.1f} ms、自身錯誤率中 {err:.0%} 推估為流量造成，改記在 {amp}"
                    if ms or err else f"；流量被 {amp} 放大到 {ratio:.2f} 倍（{amp} 不在事件裡，未轉分數）")
            fo = r.get("fan_out")
            if fo:
                why += f"（{amp} 每處理一個請求呼叫 {fo['via']} {fo['before']:.2f}→{fo['during']:.2f} 次，扇出 {fo['ratio']:.2f} 倍）"
        for victim, ms, ratio in r["moved_in"]:
            why += f"；把 {victim} 的流量放大到 {ratio:.2f} 倍，承接 {victim} 自身耗時增量中的 {ms:.1f} ms"
        for victim, _ms, err, fo in r.get("amp_of", ()):
            why += (f"；流量放大者：每處理一個請求呼叫 {fo['via']} {fo['before']:.2f}→{fo['during']:.2f} 次（扇出 {fo['ratio']:.2f} 倍），"
                    f"{victim} 的自身錯誤率增量中 {err:.0%} 也改記在這裡")
        if r.get("amp_added"):
            why += "；它不在事件裡（自己沒有告警），因為把下游的流量放大才補進候選"
        if r.get("ingress"):
            entry, ratio = r["ingress"]
            why += (f"；流量是外部入口打進來的（往上游追到 {entry} 的外部請求，流入 {ratio:.2f} 倍），"
                    "沒有內部服務把流量放大，不轉記")
        for edge in r.get("mq", ()):
            who = f"生產者 {edge.producer}" if edge.producer else "生產者未知"
            why += (f"；消費訊息佇列 {edge.queue or '（佇列名稱未知）'}（{who}）：延遲 P95 "
                    f"{lag_text(edge.lag_ms_p95_before)}→{lag_text(edge.lag_ms_p95)}，"
                    f"這批訊息發布於 {window_text(edge.pub_start, edge.pub_end)}")
        for edge, moved, share in r.get("mq_out", ()):
            window = window_text(edge.pub_start, edge.pub_end)
            why += (f"；自身錯誤率增量中 {moved:.0%} 推估為訊息夾帶造成（事件期間自己失敗的入口有 {share:.0%} 是在消費 "
                    f"{edge.producer} 於 {window} 發的訊息，而它當時有異常），改記在 {edge.producer}"
                    "（推論：trace 只能證明訊息是它發的、它當時有異常，不能證明訊息內容有問題）")
        for edge, moved in r.get("mq_in", ()):
            why += (f"；承接 {edge.consumer} 自身錯誤率增量中的 {moved:.0%}（它消費失敗的訊息是這裡在 "
                    f"{window_text(edge.pub_start, edge.pub_end)} 發的，佇列 {edge.queue or '（佇列名稱未知）'}，"
                    f"延遲 P95 {lag_text(edge.lag_ms_p95)}）")
        for note in r.get("mq_notes", ()):
            why += f"；{note}"
        if r["entity"] in bursts:
            p = bursts[r["entity"]]
            why += f"；日誌暴增：{p['template'][:60]} 約 {p['ratio']:.0f} 倍（權重 {self.log_weight:g}）"
        why += (f"；分數 {r['score']:.2f} = 自身耗時 {self.self_weight:g}×{r['s_self']:.2f}"
                f" + 錯誤 {self.error_weight:g}×{r['s_err']:.2f}")
        if self.log_weight:
            why += f" + 日誌 {self.log_weight:g}×{r['s_log']:.2f}"
        if not r["hit"]:
            why += "（未達顯著門檻，僅供參考）"
        return why
