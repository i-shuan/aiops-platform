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
from aiops.stages.rca.spans import EXTERNAL_CALLER, UNKNOWN_CALLER, inbound_callers, resource_kinds, window_stats

MAX_HOPS = 10


@OperatorRegistry.register("rca.root_cause.evidence")
class EvidenceRootCause(RootCauseOperator):
    """params: self_weight（1.0）、error_weight（0.5）、log_weight（0.0）、min_requests（20）、min_self_increase_ms（5）、
    min_self_increase_ratio（0.2）、min_error_increase（0.05）、min_dominance（1.1）、min_load_ratio（1.5）、
    min_dependency_share（0.6：候選的自身耗時增量裡，單一下游資源的等待增量佔比達到就填 dependency 下鑽；
    或入口失敗裡該資源失敗的佔比達到也填）、
    fallback（"rca.root_cause.topological"；沒有 trace 的場景改用它，空字串 = 不退回）。

    dependency 不改分數、不改排名：候選仍是服務（拓撲、傳播路徑、標準答案都不變），只多告訴人「該查的是它等的東西」。"""

    def __init__(self, self_weight: float = 1.0, error_weight: float = 0.5, log_weight: float = 0.0,
                 min_requests: int = 20, min_self_increase_ms: float = 5.0, min_self_increase_ratio: float = 0.2,
                 min_error_increase: float = 0.05, min_dominance: float = 1.1, min_load_ratio: float = 1.5,
                 min_dependency_share: float = 0.6, fallback: str = "rca.root_cause.topological"):
        if min(self_weight, error_weight, log_weight, min_self_increase_ms, min_self_increase_ratio, min_error_increase) < 0:
            raise ValueError("權重與門檻不能是負的")
        if min_requests < 1 or min_dominance < 1 or min_load_ratio <= 1:
            raise ValueError("min_requests 至少 1；min_dominance 至少 1（1 = 不做拉開檢查）；min_load_ratio 必須大於 1")
        if not 0 < min_dependency_share <= 1:
            raise ValueError("min_dependency_share 必須在 (0, 1]")
        self.min_dependency_share = float(min_dependency_share)
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

    def _amplifiers(self, spans, first_ts, start, end, base_min, dur_min) -> tuple[dict[str, float], dict[str, str]]:
        """每個服務的流入流量倍數，以及被灌流量的服務的放大者（往上游走到第一個自己流量沒漲的呼叫者）。"""
        before, during = inbound_callers(spans, first_ts, start), inbound_callers(spans, start, end)
        load: dict[str, float] = {}
        for svc in set(before) | set(during):
            nb, nd = sum(before.get(svc, {}).values()), sum(during.get(svc, {}).values())
            if nb >= self.min_requests and nd >= self.min_requests:
                load[svc] = (nd / dur_min) / (nb / base_min)
        amplifier: dict[str, str] = {}
        for svc, ratio in load.items():
            if ratio < self.min_load_ratio:
                continue
            current, hops = svc, 0
            while hops < MAX_HOPS:
                callers = {c: n for c, n in during.get(current, {}).items() if c not in (EXTERNAL_CALLER, UNKNOWN_CALLER)}
                if not callers:
                    break  # 流量來自外部入口（或呼叫者不在樣本裡，不知道是誰）：不能指認哪個服務放大的
                caller = max(callers, key=callers.get)
                if load.get(caller, 0.0) >= self.min_load_ratio:
                    current, hops = caller, hops + 1  # 呼叫者自己也被灌流量，再往上找
                    continue
                if caller in load:  # 呼叫者流量沒漲，卻讓下游流量漲了：放大者
                    amplifier[svc] = caller
                break
        return load, amplifier

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
        first_ts = min(as_utc(s.ts) for s in spans)
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
        load, amplifier = self._amplifiers(spans, first_ts, start, end, base_min, dur_min)
        for r in rows.values():
            r["self_eff"], r["err_eff"] = r["self_raw"], r["err_raw"]
        for entity, r in rows.items():
            amp = amplifier.get(entity)
            if amp is None or amp not in rows:
                if amp is not None:
                    r["moved_out"] = (amp, 0.0, 0.0, load[entity])  # 放大者不在事件裡：只寫在理由裡，不轉分數
                continue
            share = 1 - 1 / load[entity]
            moved_self, moved_err = max(r["self_raw"], 0.0) * share, max(r["err_raw"], 0.0) * share
            r["self_eff"] -= moved_self
            r["err_eff"] -= moved_err
            r["moved_out"] = (amp, moved_self, moved_err, load[entity])
            rows[amp]["self_eff"] += moved_self
            rows[amp]["err_eff"] += moved_err
            rows[amp]["moved_in"].append((entity, moved_self, load[entity]))

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
        return [RootCandidate(r["entity"], r["score"], self._reason(r, bursts, base_min, load, kinds),
                              self._dependency(r, kinds)) for r in ordered_rows]

    def _dependency(self, r, kinds: dict[str, str]) -> str:
        """單一下游資源佔了這個服務自身耗時增量（或入口失敗）的 min_dependency_share 以上，就是下鑽歸因。
        等待增量用 (事件期間 − 事件前) 的每請求 ms；失敗用事件期間「該資源失敗筆數 ÷ 入口失敗筆數」。兩個資源各佔一半就不填。"""
        b, d = r["b"], r["d"]
        if r["self_raw"] > 0:
            inc = {res: ms - b.wait_ms_per_request.get(res, 0.0) for res, ms in d.wait_ms_per_request.items()}
            res, ms = max(inc.items(), key=lambda kv: kv[1], default=(None, 0.0))
            if res is not None and ms > 0 and ms / r["self_raw"] >= self.min_dependency_share:
                return f"{res}（{kinds.get(res, '')}）" if kinds.get(res) else res
        entry_failures = round(d.error_rate * d.requests)
        if entry_failures and d.error_rate - b.error_rate >= self.min_error_increase:
            res, n = max(d.wait_failures.items(), key=lambda kv: kv[1], default=(None, 0))
            if res is not None and n / entry_failures >= self.min_dependency_share:
                return f"{res}（{kinds.get(res, '')}）" if kinds.get(res) else res
        return ""

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
        why = (f"自身耗時 {b.self_ms_per_request:.1f}→{d.self_ms_per_request:.1f} ms／請求（{r['self_raw']:+.1f}）；"
               f"自身錯誤率 {b.own_error_rate:.0%}→{d.own_error_rate:.0%}；入口請求 {b.requests}→{d.requests}"
               f"（事件前 {base_min:.0f} 分鐘）")
        why += self._breakdown(r, kinds or {})
        if r["entity"] in load:
            why += f"；流入流量 {load[r['entity']]:.2f} 倍"
        if r["moved_out"]:
            amp, ms, err, ratio = r["moved_out"]
            why += (f"；流量被 {amp} 放大到 {ratio:.2f} 倍，自身耗時增量中 {ms:.1f} ms、自身錯誤率中 {err:.0%} 推估為流量造成，改記在 {amp}"
                    if ms or err else f"；流量被 {amp} 放大到 {ratio:.2f} 倍（{amp} 不在事件裡，未轉分數）")
        for victim, ms, ratio in r["moved_in"]:
            why += f"；把 {victim} 的流量放大到 {ratio:.2f} 倍，承接 {victim} 自身耗時增量中的 {ms:.1f} ms"
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
