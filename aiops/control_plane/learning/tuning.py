"""調參唯一收口：自動 tune、LLM 從回饋抽規則、人工調整，三條路都只能產出提案。

提案 -> 回測 -> 核准，才會寫入現行參數。核准時檢查樂觀鎖：提案建立後若現行參數已被別的提案改過
（例如人工剛調好），舊提案一律作廢重提，避免「人工剛調好，半夜被自動 tune 蓋掉」。
"""
from __future__ import annotations

from aiops.engine.context import PipelineContext, TargetError
from aiops.engine.errors import BadRequest, Conflict, NotFound
from aiops.engine.params import ANY, pick_most_specific
from aiops.engine.registry import OperatorRegistry
from aiops.engine.signature import incident_signature
from aiops.engine.types import Incident, as_utc
from aiops.engine.requests import (
    FeedbackPropose,
    FeedbackSubmit,
    IdRef,
    ProposalApprove,
    ProposalBacktest,
    ProposalCreate,
)
from aiops.control_plane.learning import golden, snr_feedback, threshold, tuning_rules
from aiops.control_plane.learning.segment import snr_nodes
from aiops.control_plane.learning.replay import incident_series
from aiops.control_plane.learning.replay import workflow_layers as _workflow_layers
from aiops.engine.operator import BaseOperator
from aiops.stages.detectors.base import ROUTING_PARAMS, DetectorOperator, layer_params
from aiops.stages.ingestion.fetch import fetch_target_frame
from aiops.stages.enrichment.base import LLMError, parse_json_reply
from aiops.stages.enrichment import masking, registry
from aiops.stages.enrichment.providers import build_llm
from aiops.storage.repository import ActiveConfigs, Feedbacks, Incidents, Knowledge, Proposals


def _detector_cls(name: str) -> type[DetectorOperator]:
    cls = OperatorRegistry.get(name)
    if not (isinstance(cls, type) and issubclass(cls, DetectorOperator)):
        raise BadRequest(f"{name} 不是偵測器")
    return cls


def _require_tunable(name: str) -> None:
    """不收調參的偵測器（致命硬上限）：界線只認 workflow YAML，任何來源的提案（人工、自動、LLM、規則升級器）都拒絕。"""
    if not _detector_cls(name).accepts_tuning:
        raise BadRequest(f"{name} 不接受調參提案：它的界線只能寫在 workflow YAML（節點 params 或 target overrides）")


def _check_params(name: str, params: dict) -> None:
    cls = _detector_cls(name)
    _require_tunable(name)
    routing = sorted(ROUTING_PARAMS & set(params))
    if routing:
        raise BadRequest(f"{routing} 是節點的路由參數（對哪些實體跑），只能寫在 workflow YAML，不能透過調參修改")
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
    _require_tunable(detector)
    return Proposals.create({
        "source": source, "detector_name": detector, "params": params, "reason": reason, "proposer": proposer,
        "pipeline": pipeline, "entity": entity,
        "status": "pending", "base_version": ActiveConfigs.version(detector, pipeline, entity), "backtest": None,
        "approved_by": None,
    })


def _effective_row(detector: str, pipeline: str, entity: str) -> dict | None:
    """這一層實際生效的現行設定（往上找最具體的一筆）。單次查詢。"""
    return pick_most_specific(ActiveConfigs.candidates(detector, pipeline, entity), pipeline, entity)


def _flags_in(algo: DetectorOperator, df, begin: int, stop: int, warmup: int) -> int:
    """用 [0, begin) 訓練，數 [begin, stop) 裡被標成異常的點。前面多帶 warmup 個點讓滾動窗湊滿，但不計分。"""
    offset = min(warmup, begin)
    flags = algo.fit(df.iloc[:begin]).detect(df.iloc[begin - offset:stop]).is_anomaly
    return int(sum(flags[offset:]))


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

    硬性安全卡口（黃金事故防護罩，control_plane/learning/golden.py）：提案影響得到的每一場確認過的真事故
    （經驗庫 verdict = true_positive）都另外現抓重跑；提案參數讓任何一場從「抓得到」變成「一點都抓不到」，
    backtest.safety.passed = false、提案標成 rejected（不能再回測、不能核准，要重新提案）。
    golden_limit：一次最多驗最近的幾場（每場一次資料源查詢）；超過的照實記在 safety.skipped_oldest。
    """

    def __init__(self, golden_limit: int = 50):
        if golden_limit < 1:
            raise TypeError("golden_limit 至少 1")
        self.golden_limit = int(golden_limit)

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = _need(ctx.request, ProposalBacktest)
        row = _load(req.proposal_id)
        if row["status"] not in ("pending", "backtested"):
            raise Conflict(f"提案狀態為 {row['status']}，不能再回測")
        cls = _detector_cls(row["detector_name"])
        if snr_nodes(row["pipeline"], row["detector_name"]):
            # R+SNR 節點（開了 snr、軟投票）：整段偵測重跑，單一偵測器的回測量不準（control_plane/learning/snr_feedback.py）
            backtest = snr_feedback.backtest(ctx, row, req)
            status = "backtested" if backtest["safety"]["passed"] else "rejected"
            ctx.result = {"proposal": Proposals.update(req.proposal_id, status=status, backtest=backtest)}
            return ctx
        if cls.requires_peers and not (req.peers or req.peer_group):
            raise BadRequest(f"{row['detector_name']} 回測需要 peers 或 peer_group")
        current_row = _effective_row(row["detector_name"], row["pipeline"], row["entity"])
        # 跟正式偵測同一套優先序（layer_params）：核准調參 <- 節點 YAML <- target overrides。提案也要疊 YAML：
        # YAML 鎖住的鍵提案改了也不會生效，回測就該照實顯示「沒差」，並把被鎖的鍵列出來
        yaml_params, overrides, layers = _workflow_layers(ctx, row["detector_name"], row["pipeline"], row["entity"])
        current_params = layer_params(cls, (current_row or {}).get("params", {}), yaml_params, overrides)
        proposed_params = layer_params(cls, row["params"], yaml_params, overrides)
        layers["locked"] = sorted(k for k in row["params"] if k in yaml_params or k in overrides)
        current_algo, proposed_algo = cls(**current_params), cls(**proposed_params)
        lags = sorted({*current_algo.required_lags(), *proposed_algo.required_lags()})
        tr = req.time_range
        df = fetch_target_frame(ctx, tr.start, tr.end, tr.step_seconds, lags).reset_index(drop=True)
        split = int(len(df) * req.train_ratio)
        if split < 1 or split >= len(df):
            raise BadRequest(f"回測資料只有 {len(df)} 點，切不出訓練段與測試段，請拉長時間範圍")
        warmup = max(current_algo.warmup_points(), proposed_algo.warmup_points())
        current_count = _flags_in(current_algo, df, split, len(df), warmup)
        proposed_count = _flags_in(proposed_algo, df, split, len(df), warmup)
        backtest = {"metric": req.metric.model_dump(), "points": len(df), "train_points": split,
                    "test_points": len(df) - split, "warmup_points": min(warmup, split),
                    "split_at": df["ts"].iloc[split].isoformat(), "current_params": current_params,
                    "proposed_params": proposed_params, "workflow": layers,
                    "current_anomalies": current_count, "proposed_anomalies": proposed_count,
                    "missed_windows": self._missed_windows(req.proposal_id, df, current_algo, proposed_algo, warmup)}
        backtest["safety"] = safety = golden.check(ctx, row, cls, self.golden_limit)
        status = "backtested" if safety["passed"] else "rejected"
        ctx.result = {"proposal": Proposals.update(req.proposal_id, status=status, backtest=backtest)}
        return ctx

    @staticmethod
    def _missed_windows(proposal_id: int, df, current_algo, proposed_algo, warmup: int) -> list[dict]:
        """這個提案來自「漏報」回饋時：檢查回報的那段時間，現行參數與提案參數各抓不抓得到。
        訓練只用那段時間之前的資料（不偷看答案）。那段時間不在回測範圍內就標示出來。"""
        out = []
        ts = df["ts"]
        for fb in Feedbacks.for_proposal(proposal_id):
            if fb["verdict"] != "missed" or fb["time_start"] is None:
                continue
            start, end = as_utc(fb["time_start"]), as_utc(fb["time_end"])
            item = {"feedback_id": fb["id"], "start": start.isoformat(), "end": end.isoformat()}
            begin = int((ts < start).sum())
            stop = int((ts <= end).sum())
            if begin < 2 or stop <= begin:
                out.append({**item, "note": "這段時間不在回測範圍內（或前面沒有足夠的訓練資料），請把 time_range 拉到涵蓋它"})
                continue
            out.append({**item, "current_caught": _flags_in(current_algo, df, begin, stop, warmup) > 0,
                        "proposed_caught": _flags_in(proposed_algo, df, begin, stop, warmup) > 0})
        return out


@OperatorRegistry.register("admin.proposal_approve")
class ProposalApproveOp(BaseOperator):
    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = _need(ctx.request, ProposalApprove)
        row = _load(req.proposal_id)
        if row["status"] == "advisory":
            raise Conflict("這是 advisory 建議（例如序列體檢建議把某個偵測器從 target 排除）：每條 target 跑哪些偵測節點"
                           "由 workflow YAML 決定（target.detectors、節點 exclude_entities），資料庫核准不會生效，"
                           f"請照建議改 YAML。建議內容：{row['reason']}")
        if row["status"] == "rejected":
            missed = [m["incident_uid"] for m in ((row["backtest"] or {}).get("safety") or {}).get("missed_golden", [])]
            raise Conflict(f"提案回測沒通過黃金事故防護罩（會漏抓 {missed}），不能核准；請重新提案")
        if row["status"] != "backtested":
            raise Conflict(f"提案狀態為 {row['status']}，必須先回測才能核准")
        safety = (row["backtest"] or {}).get("safety")
        if not safety or not safety.get("passed"):
            raise Conflict("提案的回測結果沒有通過黃金事故防護罩（或是舊版回測、沒有驗過），請重新回測")
        # 條件式寫入：只有現行版本仍等於提案的基準版本才會成功，檢查與寫入是同一個動作，並發核准不會互相覆蓋
        config = ActiveConfigs.set(row["detector_name"], row["params"], row["source"], req.proposal_id,
                                   expected_version=row["base_version"], pipeline=row["pipeline"], entity=row["entity"])
        if config is None:
            current = ActiveConfigs.version(row["detector_name"], row["pipeline"], row["entity"])
            Proposals.update(req.proposal_id, status="stale")
            raise Conflict(f"現行參數已更新（目前版本 {current}，提案基準版本 {row['base_version']}），"
                           "此提案已作廢，請重新提案")
        ctx.result = {"proposal": Proposals.update(req.proposal_id, status="approved", approved_by=req.approver,
                                                   approved_at=ctx.now),
                      "active_config": config}
        return ctx


@OperatorRegistry.register("admin.feedback_submit")
class FeedbackSubmitOp(BaseOperator):
    """收回饋。漏報可以不指定事件群（平台根本沒告警），改記管線、實體與發生時段。

    確認是真異常（true_positive）的事件群一律寫進經驗庫（同一個事件群再寫一次就更新），處置可以之後再補：
    它就是黃金事故，之後每個調參提案回測都要證明還抓得到它（admin.proposal_backtest 的 safety）。
    寫入時一併存事件群時窗、嚴重度、這場事故的序列（實體／資料源／查詢，存在 context.series，回測照這個重抓）。
    有寫處置的才會出現在 RCA「歷史相似案例」。
    同一個事件群後來被改判 false_positive：經驗庫那筆降級成 false_positive（不刪，留紀錄），不再當黃金事故。"""

    scope = "batch"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        req = _need(ctx.request, FeedbackSubmit)
        incident = None
        if req.incident_uid is not None:
            incident = Incidents.get_by_uid(req.incident_uid)
            if incident is None:
                raise NotFound(f"找不到事件群 {req.incident_uid}")
        if req.detector:
            _detector_cls(req.detector)
        tr = req.time_range
        feedback = Feedbacks.create({
            "incident_uid": req.incident_uid, "verdict": req.verdict, "detector_name": req.detector,
            "text": req.text, "author": req.author, "proposal_id": None,
            "pipeline": req.pipeline or (incident["pipeline"] if incident else None),
            "entity": req.entity, "time_start": tr.start if tr else None, "time_end": tr.end if tr else None})
        ctx.result = {"feedback": feedback}
        if incident is not None and req.verdict == "true_positive":
            ctx.result["knowledge"] = self._golden(req, incident)
        elif incident is not None and req.verdict == "false_positive" and Knowledge.get(incident["uid"]):
            ctx.result["knowledge"] = Knowledge.upsert(incident["uid"], verdict="false_positive")
        return ctx

    @staticmethod
    def _golden(req: FeedbackSubmit, incident: dict) -> dict:
        inc = Incident.from_dict(incident["incident"], uid=incident["uid"])
        entities, metrics, signals = incident_signature(inc)
        fields = {"pipeline": incident["pipeline"] or "", "entities": entities, "metrics": metrics, "signals": signals,
                  "author": req.author, "verdict": "true_positive", "severity": incident["severity"] or "",
                  "window_start": as_utc(inc.start), "window_end": as_utc(inc.end)}
        if req.resolution:  # 沒寫就不蓋掉之前補過的處置
            fields["resolution"] = req.resolution
            fields["root_cause"] = req.root_cause or ""
        # 兩步都是資料庫端原子操作：ON CONFLICT 建列／更新欄位，再把 context 的鍵「追加」上去。
        # 以前是 Python 讀出整個 context、合併、整包寫回：兩個人同時確認同一個事故，後寫的會蓋掉先寫的標籤
        Knowledge.upsert(incident["uid"], **fields)
        return Knowledge.merge_context(incident["uid"], {**req.context,
                                                        "series": [s.to_dict() for s in incident_series(incident["incident"])]})


@OperatorRegistry.register("admin.feedback_propose")
class FeedbackProposeOp(BaseOperator):
    """回饋 -> 調參提案。provider 不是 noop 時讓 LLM 抽規則並驗證；沒設定、失敗、驗證不過都退回規則式提案。
    結果只是提案，仍要回測與核准才生效。

    規則式提案：
    - 誤報（false_positive）、提案層級是單一實體（scope=entity）、偵測器有 threshold：分位數反推
      （control_plane/learning/threshold.py）：新門檻 = max(S_max × 1.05, 過去 30 天分數的 99.9% 分位數)，
      資料源與查詢取自事件群裡的異常事件，不用呼叫端填。
      * 只在 scope=entity 做：S_max 與分位數是「這一條序列」的分數分佈，拿它去定整條管線或全平台的門檻，
        等於用一個服務的分佈去調其他所有服務；管線／全域層級維持固定規則。
      * 單次變動上限 50%（tuning_rules 的規則）：目標超過就這次先調到 +50%，reason 寫明目標值與還差多少。
        不切成多個提案：同一層的提案共用同一個基準版本，第一個核准後其他的一定變 stale，多步提案核准不了。
      * S_max 不高於現行門檻（誤報不是這個偵測器的分數造成的，例如是多數決裡別的節點）、資料撈不到：
        退回固定規則，原因寫在 reason 與結果的 quantile 欄位。
    - 其他情況（漏報、管線／全域層級）：固定規則，誤報 ×1.2、漏報 ×0.8。"""

    scope = "batch"

    def __init__(self, provider: str = "noop", model: str | None = None, base_url: str | None = None):
        if provider not in registry.names():
            raise TypeError(f"provider 只能是 {', '.join(registry.names())}，不是 {provider!r}")
        self.provider, self.model, self.base_url = provider, model, base_url

    def _extract(self, fb: dict, detector: str, tunable: dict, current: dict[str, float], rule):
        """回傳 (要改的參數, 理由, generated_by, 退回原因)。rule() 是規則式提案 (參數, 理由, generated_by)，需要時才算。"""
        if self.provider == "noop":
            return (*rule(), None)
        fallback = rule
        try:
            llm = build_llm(self.provider, self.model, self.base_url)
            assert llm is not None
            prompt = tuning_rules.build_prompt(detector, fb["verdict"], fb["text"], tunable, current)
            # 回饋是人寫的自由文字，可能帶帳號、信箱、IP、憑證：跟 RCA 一樣只從 masking.send 這個出口送
            reply = parse_json_reply(masking.send(llm, tuning_rules.SYSTEM, prompt, masking.Masker(keep={detector})))
        except masking.MaskError as e:
            return (*fallback(), f"mask_failed：{e}")  # 遮罩失敗就不送，退回規則式提案
        except LLMError as e:
            return (*fallback(), str(e))
        reason = tuning_rules.validate(reply, fb["verdict"], tunable, current)
        if reason:
            return (*fallback(), reason)
        return reply["params"], reply["reason"], "llm", None

    @staticmethod
    def _scope(req: FeedbackPropose, fb: dict) -> tuple[str, str]:
        """提案要改哪一層。管線與實體都取自這筆回饋所屬的事件群，不讓呼叫端亂填。"""
        if req.scope == "global":
            return ANY, ANY
        if fb["incident_uid"] is None:  # 漏報補錄：管線與實體記在回饋本身
            if req.entity is not None and req.entity != fb["entity"]:
                raise BadRequest(f"這筆漏報是 {fb['entity']} 的，不能拿來調 {req.entity}")
            return (fb["pipeline"], ANY) if req.scope == "pipeline" else (fb["pipeline"], fb["entity"])
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
        pipeline, entity = self._scope(req, fb)
        if pipeline != ANY and (snr_nodes(pipeline, detector) if detector else snr_feedback.has_snr(pipeline)):
            return self._propose_snr(ctx, req, fb, detector, pipeline, entity)
        if not detector:
            raise BadRequest("請指定偵測器")
        tunable = _detector_cls(detector).tunable
        if not tunable:
            raise BadRequest(f"{detector} 沒有可自動調整的參數，請走人工提案")

        active = (_effective_row(detector, pipeline, entity) or {}).get("params", {})
        current = {k: float(active.get(k, default)) for k, (default, _lo, _hi) in tunable.items()}
        details: dict = {}

        def rule():
            return self._rule(ctx, fb, detector, tunable, current, active, pipeline, entity, details)

        changes, reason, generated_by, rejected = self._extract(fb, detector, tunable, current, rule)
        proposal = _create_proposal("llm" if generated_by == "llm" else "auto", detector, {**active, **changes},
                                    f"來自回饋 #{fb['id']}（{fb['verdict']}）：{reason}", req.proposer,
                                    pipeline, entity)
        Feedbacks.link_proposal(fb["id"], proposal["id"])
        ctx.result = {"proposal": proposal, "generated_by": generated_by, "rejected": rejected}
        if details:
            ctx.result["quantile"] = details
        return ctx

    @staticmethod
    def _propose_snr(ctx: PipelineContext, req: FeedbackPropose, fb: dict, detector: str | None, pipeline: str,
                     entity: str) -> PipelineContext:
        """R+SNR 管線：誤報 -> 門檻倍率提案（整段重跑二分搜，黃金事故當上限）；漏報不在這裡（倍率下限 1，見 snr_feedback）。"""
        if fb["verdict"] != "false_positive":
            raise BadRequest(f"{pipeline} 是 R+SNR 管線：回饋閉環只處理誤報（門檻倍率只能往少誤報調）；"
                             "漏報請改 YAML 的 p_fa／min_run，或對調過倍率的偵測器人工提案調回")
        det, params, reason, details = snr_feedback.propose_scale(ctx, fb, detector, pipeline, entity)
        _check_params(det, params)
        proposal = _create_proposal("auto", det, params, f"來自回饋 #{fb['id']}（{fb['verdict']}）：{reason}",
                                    req.proposer, pipeline, entity)
        Feedbacks.link_proposal(fb["id"], proposal["id"])
        ctx.result = {"proposal": proposal, "generated_by": "segment_bisect", "rejected": None, "snr": details}
        return ctx

    @staticmethod
    def _rule(ctx: PipelineContext, fb: dict, detector: str, tunable: dict, current: dict[str, float],
              active: dict, pipeline: str, entity: str, details: dict) -> tuple[dict, str, str]:
        """規則式提案：能做分位數反推就做（條件見類別說明），否則固定規則。details 填入反推的中間數字給呼叫端看。"""
        template = tuning_rules.template(fb["verdict"], tunable, current)
        if not (fb["verdict"] == "false_positive" and fb["incident_uid"] and entity != ANY and "threshold" in tunable):
            return (*template, "template")
        incident = Incidents.get_by_uid(fb["incident_uid"])
        series = incident_series(incident["incident"], entity) if incident else []
        fell_back = None
        if not series:
            fell_back = "事件群裡沒有這個實體的資料源與查詢"
        else:
            cls = _detector_cls(detector)
            yaml_params, overrides, _info = _workflow_layers(ctx, detector, pipeline, entity)
            params = layer_params(cls, active, yaml_params, overrides)  # 算分數用正式偵測實際生效的參數
            inc = Incident.from_dict(incident["incident"], uid=incident["uid"])
            got = threshold.infer(ctx, cls, params, series, pipeline, entity, as_utc(inc.start), as_utc(inc.end))
            if isinstance(got, str):
                fell_back = f"撈不到資料（{got}）"
            else:
                details.update(got)
                proposal, note = tuning_rules.quantile_step(got["target"], tunable, current)
                if proposal is None:
                    fell_back = note
                else:
                    details["proposed"] = proposal["threshold"]
                    reason = (f"分位數反推：誤報時窗最高分 S_max={got['s_max']}，過去 {got['lookback_days']} 天"
                              f"（實際有分數 {got['q_days']} 天、{got['q_points']} 點，排除 {got['excluded_windows']} 個事故／維護／黃金時窗）"
                              f"99.9% 分位數 Q={got['q']}；目標 max(S_max×{got['margin']}, Q)={got['target']}。{note}")
                    if got.get("note"):
                        reason += f"（{got['note']}）"
                    return proposal, reason, "quantile"
        details["fallback"] = fell_back
        return template[0], f"{template[1]}（沒用分位數反推：{fell_back}）", "template"
