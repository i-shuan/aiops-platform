"""調參唯一收口：自動 tune、LLM 從回饋抽規則、人工調整，三條路都只能產出提案。

提案 -> 回測 -> 核准，才會寫入現行參數。核准時檢查樂觀鎖：提案建立後若現行參數已被別的提案改過
（例如人工剛調好），舊提案一律作廢重提，避免「人工剛調好，半夜被自動 tune 蓋掉」。
"""
from __future__ import annotations

from aiops.core.context import PipelineContext
from aiops.core.errors import BadRequest, Conflict, NotFound
from aiops.core.params import ANY, pick_most_specific
from aiops.core.registry import OperatorRegistry
from aiops.core.requests import (
    FeedbackPropose,
    FeedbackSubmit,
    IdRef,
    ProposalApprove,
    ProposalBacktest,
    ProposalCreate,
)
from aiops.operators.admin import tuning_rules
from aiops.operators.base import BaseOperator
from aiops.operators.detectors.base import DetectorOperator
from aiops.operators.fetch import fetch_target_frame
from aiops.operators.llm.base import LLMError, parse_json_reply
from aiops.operators.llm import registry
from aiops.operators.llm.providers import build_llm
from aiops.storage.repository import ActiveConfigs, Feedbacks, Incidents, Proposals


def _detector_cls(name: str) -> type[DetectorOperator]:
    cls = OperatorRegistry.get(name)
    if not (isinstance(cls, type) and issubclass(cls, DetectorOperator)):
        raise BadRequest(f"{name} 不是偵測器")
    return cls


def _check_params(name: str, params: dict) -> None:
    cls = _detector_cls(name)
    try:
        cls(**params).validate_params()
    except (TypeError, ValueError) as e:
        raise BadRequest(f"提案參數不合法：{e}") from e


def _need(request, kind: type):
    if not isinstance(request, kind):
        raise BadRequest(f"需要 {kind.__name__} 請求")
    return request


def _load(proposal_id: int) -> dict:
    row = Proposals.get(proposal_id)
    if row is None:
        raise NotFound(f"找不到調優提案 {proposal_id}")
    return row


def _create_proposal(source: str, detector: str, params: dict, reason: str, proposer: str,
                     pipeline: str = ANY, entity: str = ANY) -> dict:
    return Proposals.create({
        "source": source, "detector_name": detector, "params": params, "reason": reason, "proposer": proposer,
        "pipeline": pipeline, "entity": entity,
        "status": "pending", "base_version": ActiveConfigs.version(detector, pipeline, entity), "backtest": None,
        "approved_by": None,
    })


def _effective_row(detector: str, pipeline: str, entity: str) -> dict | None:
    """這一層實際生效的現行設定（往上找最具體的一筆）。單次查詢。"""
    return pick_most_specific(ActiveConfigs.candidates(detector, pipeline, entity), pipeline, entity)


@OperatorRegistry.register("admin.proposal_create")
class ProposalCreateOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = _need(ctx.request, ProposalCreate)
        _check_params(req.detector, req.params)
        ctx.result = {"proposal": _create_proposal(req.source, req.detector, req.params, req.reason, req.proposer,
                                                   req.pipeline, req.entity)}
        return ctx


@OperatorRegistry.register("admin.proposal_get")
class ProposalGet(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        ctx.result = {"proposal": _load(_need(ctx.request, IdRef).id)}
        return ctx


@OperatorRegistry.register("admin.proposal_backtest")
class ProposalBacktestOp(BaseOperator):
    """同一段歷史資料，分別用現行參數與提案參數跑一次，記下差異。資料由 fetch_target_frame 取得
    （跟實際偵測同一條路徑，欄位對照與 trace 時序由引擎載入）。

    時間切段避免資料洩漏：前 train_ratio 的點拿來訓練（fit），後面的點才拿來測試並計分。
    偵測器若需要滾動窗（warmup_points），測試段往前多帶那麼多點讓窗口湊滿，但只統計測試段內的異常。
    """

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = _need(ctx.request, ProposalBacktest)
        row = _load(req.proposal_id)
        if row["status"] not in ("pending", "backtested"):
            raise Conflict(f"提案狀態為 {row['status']}，不能再回測")
        cls = _detector_cls(row["detector_name"])
        if cls.requires_peers and not req.peers:
            raise BadRequest(f"{row['detector_name']} 回測需要 peers")
        tr = req.time_range
        df = fetch_target_frame(ctx, tr.start, tr.end, tr.step_seconds).reset_index(drop=True)
        split = int(len(df) * req.train_ratio)
        if split < 1 or split >= len(df):
            raise BadRequest(f"回測資料只有 {len(df)} 點，切不出訓練段與測試段，請拉長時間範圍")
        train = df.iloc[:split]
        current_row = _effective_row(row["detector_name"], row["pipeline"], row["entity"])
        current_params = (current_row or {}).get("params", {})
        current_algo, proposed_algo = cls(**current_params), cls(**row["params"])
        warmup = max(current_algo.warmup_points(), proposed_algo.warmup_points())
        offset = min(warmup, split)  # 測試段前面多帶的點數（不計分）
        test = df.iloc[split - offset:]

        def anomalies_in_test(algo) -> int:
            flags = algo.fit(train).detect(test).is_anomaly
            return int(sum(flags[offset:]))

        current_count, proposed_count = anomalies_in_test(current_algo), anomalies_in_test(proposed_algo)
        backtest = {"metric": req.metric.model_dump(), "points": len(df), "train_points": split,
                    "test_points": len(df) - split, "warmup_points": offset,
                    "split_at": df["ts"].iloc[split].isoformat(), "current_params": current_params,
                    "current_anomalies": current_count, "proposed_anomalies": proposed_count}
        ctx.result = {"proposal": Proposals.update(req.proposal_id, status="backtested", backtest=backtest)}
        return ctx


@OperatorRegistry.register("admin.proposal_approve")
class ProposalApproveOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = _need(ctx.request, ProposalApprove)
        row = _load(req.proposal_id)
        if row["status"] != "backtested":
            raise Conflict(f"提案狀態為 {row['status']}，必須先回測才能核准")
        # 條件式寫入：只有現行版本仍等於提案的基準版本才會成功，檢查與寫入是同一個動作，並發核准不會互相覆蓋
        config = ActiveConfigs.set(row["detector_name"], row["params"], row["source"], req.proposal_id,
                                   expected_version=row["base_version"], pipeline=row["pipeline"], entity=row["entity"])
        if config is None:
            current = ActiveConfigs.version(row["detector_name"], row["pipeline"], row["entity"])
            Proposals.update(req.proposal_id, status="stale")
            raise Conflict(f"現行參數已更新（目前版本 {current}，提案基準版本 {row['base_version']}），"
                           "此提案已作廢，請重新提案")
        ctx.result = {"proposal": Proposals.update(req.proposal_id, status="approved", approved_by=req.approver),
                      "active_config": config}
        return ctx


@OperatorRegistry.register("admin.feedback_submit")
class FeedbackSubmitOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = _need(ctx.request, FeedbackSubmit)
        if Incidents.get_by_uid(req.incident_uid) is None:
            raise NotFound(f"找不到事件群 {req.incident_uid}")
        if req.detector:
            _detector_cls(req.detector)
        ctx.result = {"feedback": Feedbacks.create({
            "incident_uid": req.incident_uid, "verdict": req.verdict, "detector_name": req.detector,
            "text": req.text, "author": req.author, "proposal_id": None})}
        return ctx


@OperatorRegistry.register("admin.feedback_propose")
class FeedbackProposeOp(BaseOperator):
    """回饋 -> 調參提案。provider 不是 noop 時讓 LLM 抽規則並驗證；沒設定、失敗、驗證不過都退回固定規則。
    結果只是提案，仍要回測與核准才生效。"""

    scope = "batch"

    def __init__(self, provider: str = "noop", model: str | None = None, base_url: str | None = None):
        if provider not in registry.names():
            raise TypeError(f"provider 只能是 {', '.join(registry.names())}，不是 {provider!r}")
        self.provider, self.model, self.base_url = provider, model, base_url

    def _extract(self, fb: dict, detector: str, tunable: dict, current: dict[str, float]):
        """回傳 (要改的參數, 理由, generated_by, 退回原因)。"""
        fallback = (*tuning_rules.template(fb["verdict"], tunable, current), "template")
        if self.provider == "noop":
            return (*fallback, None)
        try:
            llm = build_llm(self.provider, self.model, self.base_url)
            assert llm is not None
            prompt = tuning_rules.build_prompt(detector, fb["verdict"], fb["text"], tunable, current)
            reply = parse_json_reply(llm.complete(tuning_rules.SYSTEM, prompt))
        except LLMError as e:
            return (*fallback, str(e))
        reason = tuning_rules.validate(reply, fb["verdict"], tunable, current)
        if reason:
            return (*fallback, reason)
        return reply["params"], reply["reason"], "llm", None

    @staticmethod
    def _scope(req: FeedbackPropose, fb: dict) -> tuple[str, str]:
        """提案要改哪一層。管線與實體都取自這筆回饋所屬的事件群，不讓呼叫端亂填。"""
        if req.scope == "global":
            return ANY, ANY
        incident = Incidents.get_by_uid(fb["incident_uid"])
        if incident is None:
            raise NotFound(f"找不到事件群 {fb['incident_uid']}")
        pipeline = incident["pipeline"] or ANY
        if req.scope == "pipeline":
            return pipeline, ANY
        entities = list(incident["incident"].get("entities", []))
        entity = req.entity
        if entity is None:
            if len(entities) != 1:
                raise BadRequest(f"事件群涉及 {len(entities)} 個實體，請用 entity 指定要調哪一個：{entities}")
            entity = entities[0]
        elif entity not in entities:
            raise BadRequest(f"{entity} 不在這個事件群的實體裡：{entities}")
        return pipeline, entity

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = _need(ctx.request, FeedbackPropose)
        fb = Feedbacks.get(req.feedback_id)
        if fb is None:
            raise NotFound(f"找不到回饋 {req.feedback_id}")
        if fb["proposal_id"] is not None:
            raise Conflict(f"回饋 {req.feedback_id} 已轉成提案 {fb['proposal_id']}")
        if fb["verdict"] == "true_positive":
            raise Conflict("這筆回饋確認是真異常，不需要調參")
        detector = req.detector or fb["detector_name"]
        if not detector:
            raise BadRequest("請指定偵測器")
        tunable = _detector_cls(detector).tunable
        if not tunable:
            raise BadRequest(f"{detector} 沒有可自動調整的參數，請走人工提案")

        pipeline, entity = self._scope(req, fb)
        active = (_effective_row(detector, pipeline, entity) or {}).get("params", {})
        current = {k: float(active.get(k, default)) for k, (default, _lo, _hi) in tunable.items()}
        changes, reason, generated_by, rejected = self._extract(fb, detector, tunable, current)
        proposal = _create_proposal("llm" if generated_by == "llm" else "auto", detector, {**active, **changes},
                                    f"來自回饋 #{fb['id']}（{fb['verdict']}）：{reason}", req.proposer,
                                    pipeline, entity)
        Feedbacks.link_proposal(fb["id"], proposal["id"])
        ctx.result = {"proposal": proposal, "generated_by": generated_by, "rejected": rejected}
        return ctx
