"""RCA：Python 不推理，只收集證據交給 LLM，再檢查它有沒有瞎編。

  1. 這個事故收斂的告警（系統、規則、開始時間、嚴重度、觸發當下的數值、是否已恢復）
  2. 系統上下游關係邊（設定檔）
  3. 事故時間窗內的錯誤日誌樣本（有就附，沒有就空）
  4. eqp_id／lot_id／carrier_id 在錯誤日誌裡的出現次數
  5. 關聯時間線：出現最多次的批號／載具，在各系統的紀錄照時間排好。工廠沒有 trace id，用它代替
  6. 呼叫鏈：失敗請求實際的呼叫路徑。有埋點的 IT 服務才有
  7. 指標走勢（VictoriaMetrics，Prometheus 相容）：設定的查詢在事故前後的數值變化。有設定才有

錯誤日誌交給 LLM 之前先處理過（_profile_logs）：同類去重、各系統最早的錯誤不被擠掉、事故前後各出現幾筆、警告。
另外由程式產生「證據概況」（overview）：手上有哪些、缺哪些與原因，寫在註記上，也交給 LLM。
這些處理都只是歸類與計數：什麼算異常仍然由 Grafana 規則決定，這裡沒有任何門檻。

LLM 連不上、逾時、格式不對、或點名了證據裡沒有的系統／機台／批號：退回「[未經分析] 原始異常摘要」，排程照常結束。

已知限制（照實寫）：
  - 送給 LLM 的日誌樣本**沒有遮罩**（沒有遮罩出口）。日誌裡的帳號、IP、業務機密
    要由產生日誌的系統先去識別化，或把 llm.url 指到公司內部的模型。
  - 幻覺檢查只擋「名稱不存在」：LLM 把存在的系統講錯因果，程式看不出來，所以第 2 行固定標 [LLM 推論]。
  - 工廠沒有 Trace 時，「集中在哪台機台／哪個批號」只是錯誤日誌的計數，不是因果證據。
  - 關聯時間線靠各系統日誌的時間戳排序：系統之間時鐘差幾秒，先後順序就可能是錯的。
  - 呼叫鏈只看得到有埋點、而且沒被取樣掉的請求。
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import httpx

from src import report
from src.evidence import GRADES, evidence_grade
from src.grouping import Incident

log = logging.getLogger("aiops_lite.rca")

# 夾數字的識別碼樣式（EQP-CVD-07、LOT24A0031）：幻覺檢查用它在摘要文字裡找「像機台／批號的字」（llm.id_pattern 的預設值）
DEFAULT_ID_PATTERN = r"[A-Za-z][A-Za-z0-9_.\-]*\d[A-Za-z0-9_.\-]*"

SYSTEM_PROMPT = """你是負責工廠系統與 IT 服務的 SRE。依照使用者給的證據，說明這個事故為什麼會發生（哪裡出了什麼事、怎麼造成現在的現象）、影響到哪裡。
根因不一定是「哪個系統是源頭」：也可能是資源用完、設定或憑證有問題、相依的元件連不上、流量變了。有多少證據就分析多少，不要因為少了某一種證據就不分析。
規則：
- 只能引用證據裡出現的系統名稱、機台、批號、載具等識別碼，不准自己編。
- 不要寫任何筆數、次數、時間：那些由程式另外顯示。
- 「關聯時間線」只代表同一個批號或載具在各系統出現的先後，不代表因果；各系統的時鐘可能有幾秒誤差。
- 「呼叫鏈」是實際發生的呼叫：最下游、而且底下沒有其他失敗呼叫的那一段，比上游更接近源頭。
- 「請求的日誌」是用呼叫鏈的編號找到的、同一個請求在各系統留下的日誌（不限等級）：它一定跟這個請求有關，比照時間撈到的日誌更直接。
- 「指標走勢」是數值在事故前後的變化（baseline 是事故前的中位數）：它說明哪個數值先變、變了多少，本身不代表因果。
- 「關係邊」只說明誰可能影響誰。某個系統在上游，不能當成它就是源頭的依據；要有指向它的證據：它自己的錯誤日誌或警告、它的指標變化、
  呼叫鏈停在它，或別的系統的錯誤訊息明確寫連不上它、等不到它（連線被拒、逾時並指名它）。
- 「錯誤種類與事故前後的次數」：事故前就以差不多的頻率出現的錯誤是平常就有的，不能當成這次事故的原因；
  事故前沒出現過（new=true）或明顯變多的才是線索。沒有次數的錯誤不能推論它是新的還是平常就有的。
- 「警告」常常寫出後來出錯的原因（資源快用完、重試變多）：錯誤訊息本身沒寫原因時，看同一個系統在出錯前的警告。
- 一定要列出 1 到 3 個可能的原因（candidates），由最可能排到最不可能。每個寫：
  source＝系統名稱；
  reason＝一到兩句白話，要具體：出了什麼事、機制是什麼（怎麼從這個原因變成現在看到的現象）、依據是哪一種證據裡的什麼（直接引用內容）。
        有別的可能被證據排除時，補一句為什麼不是它。不要只寫「它是源頭」「它有錯誤」這種沒有內容的話；
  kind＝evidence 或 inference。evidence：證據直接寫出了這個原因（錯誤訊息自己講了原因、呼叫鏈停在它、它的指標先變壞）。
        inference：證據沒有直接寫，是你從現象、名稱、先後或常識推測的。可以推測，但要說是從哪一項證據推的。
- 不要給分數或把握程度。每個原因的依據等級由程式照證據判定，不是你判定：那個系統自己有事故前沒出現過的錯誤、呼叫鏈的失敗點停在它、
  別的系統的錯誤訊息指名它＝直接證據；只有警告、平常就有的錯誤、指標變化＝間接證據；只有告警或關係邊＝推測。你標成 inference 的一律算推測。
- 就算只有告警，也要根據告警的名稱、數值、先後與關係邊列出可能的原因（kind 標 inference），不要只回「無法判定」。
  有別的合理可能就列出來，不要只列一個。連推測的依據都沒有時 candidates 才可以是空的。
- summary 是寫給值班人員看的一句結論，用白話講這個問題為什麼會發生：哪裡出了什麼事、造成什麼現象。
  不要只寫哪個系統是源頭，也不要抱怨缺了什麼證據（那個由程式另外顯示）。列的原因全部是 inference 時，開頭寫「目前只能推測」。
- 另外列 1 到 3 條「下一步建議」（actions）：值班人員現在該去確認或處理什麼，照先後排。要具體到看哪個系統的什麼
  （哪一種日誌、哪個設定、哪條連線、哪個資源），而且要對得上你列的可能原因。先寫能確認原因的檢查，再寫處置。
  不要寫「檢查日誌」「聯絡負責人」「持續觀察」這種對任何事故都成立的話；也不要建議證據裡看不出依據的操作。這些只是建議，不會被自動執行。
- 只輸出一個 JSON 物件，不要 Markdown：
  {"summary": "繁體中文，80 字以內", "candidates": [{"source": "系統名稱", "kind": "evidence", "reason": "一句話"}], "actions": ["下一步建議，一句話"], "systems": ["你點名的系統"], "ids": ["你點名的機台／批號／載具"]}"""

MAX_CANDIDATES = 3
MIN_NAME_CHARS = 3   # 名稱至少這麼長，才用「原樣出現在證據文字裡」當憑據：一兩個字的名稱在任何文字裡都找得到。沒有依據的數字，未驗證泛化
@dataclass(frozen=True)
class LlmOptions:
    """呼叫 LLM 與檢查回答的選項（config.yaml 的 llm.*；都是選填，下面是預設值）。"""
    timeout_seconds: float = 60.0
    temperature: float | None = None     # None = 不送，用模型的預設值
    max_summary_chars: int = 300         # 摘要超過就截斷（註記第 2 行）
    system_prompt: str = ""              # 空字串 = 用內建的 SYSTEM_PROMPT；來自 llm.system_prompt_file
    id_pattern: str = DEFAULT_ID_PATTERN  # 空字串 = 不檢查摘要文字裡的識別碼（systems／ids 清單照樣檢查）


def build_prompt(evidence: dict[str, Any], summary_of_evidence: str = "") -> str:
    dump = lambda x: json.dumps(x, ensure_ascii=False, indent=1)
    head = f"【0. 證據概況（程式統計的事實：已有的證據；缺的、查詢失敗的、沒接的各自讓哪一種分析做不了）】\n{summary_of_evidence}\n\n" if summary_of_evidence else ""
    text = (f"{head}【1. 本次收斂的告警（依開始時間排序；values 是觸發當下規則各節點的數值）】\n{dump(evidence['anomalies'])}\n\n"
            f"【2. 系統上下游關係（upstream 的問題會影響 downstream。有 source 的是程式觀察到的：service_graph、trace＝downstream 實際呼叫過 upstream；"
            f"log＝downstream 的錯誤日誌提到過 upstream，方向不一定對。kind 是 messaging 的是經訊息佇列的非同步關係：upstream 送訊息給 downstream，"
            f"upstream 或佇列出問題會讓 downstream 收不到或處理失敗，downstream 出問題通常不會回頭影響 upstream。requests、failed 是這段時間這條呼叫的請求數與失敗數。hops：1＝直接連到告警的系統，2＝再隔一個。有些邊另外有這段時間與事故前 {evidence.get('baseline_minutes', 30)} 分鐘（基線）的數字：rps＝每秒請求數、error_rate_pct＝失敗率（%）、p95_ms＝第 95 百分位的耗時、avg_ms＝平均耗時，加 _baseline 的是基線，_ratio＝事故期間 ÷ 基線；latency_from 是耗時取自哪一端（server＝被呼叫的自己量的，client＝呼叫的那一端量的、含網路，沒埋點的資料庫只有這一端）。p95 是從直方圖的桶內插出來的，桶寬粗時不準（比最小的那一桶還快的呼叫，彼此分不出差別），快的呼叫看平均。這些只是數字，哪一段變慢、上下游的請求量差幾倍、是不是重試放大，要你自己比對）】\n{dump(evidence['edges'])}\n\n"
            f"【3. 事故時間窗內的錯誤日誌樣本（同一種錯誤只列最早的一筆原文；similar_in_sample 是樣本裡同類的筆數；numbers_in_sample 是訊息裡會變的數字在同類樣本中的範圍，in_example 是這一筆剛好寫的值，不要把它當成固定的事實）】\n"
            f"{dump(evidence['error_logs'])}\n\n"
            f"【4. 錯誤日誌裡各識別碼的出現次數】\n{dump(evidence['id_counts'])}\n\n")
    windows = evidence.get("count_windows") or {}
    counts_note = (f"during＝事故期間 {windows.get('during_minutes', '?')} 分鐘內的筆數，before＝事故前 {windows.get('before_minutes', '?')} 分鐘內的筆數；"
                   f"new=true 表示事故前一筆都沒有；沒有這些欄位的是數不出來")
    # pattern（把數字換成 * 的樣板）只是程式歸類用的，不給模型看：它會把樣板裡的字當成真的名稱照抄
    # （現場遇到的：k8s 被寫成 k*s）。模型看 example，那是原文。
    shown = lambda rows: [{k: v for k, v in row.items() if k != "pattern"} for row in rows]
    if evidence.get("last_errors"):
        text += ("【3p. 時間窗內沒有錯誤日誌的系統，往回找到的最後一筆錯誤（minutes_before_window＝它比事故時間窗的起點早幾分鐘；"
                 "none_in_hours＝往回這麼多小時也沒有錯誤。只是時間與原文：它在告警之前就停了，跟這次有沒有關係程式沒有判斷）】\n"
                 f"{dump(evidence['last_errors'])}\n\n")
    if evidence.get("error_patterns"):
        text += (f"【3d. 主要的錯誤種類與事故前後的次數（example 是這一種錯誤最早的一筆原文；{counts_note}）】\n"
                 f"{dump(shown(evidence['error_patterns']))}\n\n")
    if evidence.get("warnings"):
        text += (f"【3e. 警告（等級是警告的日誌，還不是錯誤；{counts_note}）】\n{dump(shown(evidence['warnings']))}\n\n")
    if evidence.get("timelines"):
        text += ("【5. 關聯時間線：同一個批號／載具在各系統的紀錄，由舊到新（只代表先後，不代表因果；各系統時鐘可能有誤差）】\n"
                 f"{dump(evidence['timelines'])}\n\n")
    if evidence.get("call_chains"):
        text += ("【6. 呼叫鏈：失敗請求實際的呼叫路徑（called_by 是呼叫它的系統；origin=true 是底下沒有其他失敗呼叫的失敗點）】\n"
                 f"{dump(evidence['call_chains'])}\n\n")
    if evidence.get("trace_logs"):
        text += ("【6b. 請求的日誌：用上面呼叫鏈的編號找到的、同一個請求在各系統留下的日誌，不限等級，由舊到新】\n"
                 f"{dump(evidence['trace_logs'])}\n\n")
    if evidence.get("metrics"):
        text += ("【7. 指標走勢：各系統的數值在事故前後的變化（baseline 是事故開始前的中位數；during 是事故開始後；"
                 "points 是整段時間平均抽樣的點，時間是 UTC。只說明數值怎麼變，不代表因果）】\n"
                 f"{dump(evidence['metrics'])}\n\n")
    return text + f"【補充】{'；'.join(evidence['notes']) or '無'}"


def call_llm(api_url: str, api_key: str, model: str, prompt: str, transport: httpx.BaseTransport | None = None,
             timeout: float = 60.0, temperature: float | None = None, system_prompt: str = "") -> str:
    """OpenAI 相容的 chat completions。llm.url 可以是 …/v1 或完整的 …/chat/completions。失敗丟例外。
    temperature 預設不送（用模型自己的預設值）：有些模型只接受預設值，硬送 0 會被 400 拒絕（實測 gpt-6-luna 就是）。"""
    url = api_url.rstrip("/")
    if not url.endswith("/chat/completions"):
        url += "/chat/completions"
    with httpx.Client(timeout=timeout, transport=transport) as http:
        r = http.post(url, headers={"Authorization": f"Bearer {api_key}"} if api_key else {}, json={
            "model": model, **({} if temperature is None else {"temperature": temperature}),
            "messages": [{"role": "system", "content": system_prompt or SYSTEM_PROMPT},
                         {"role": "user", "content": prompt}]})
    r.raise_for_status()
    return str(r.json()["choices"][0]["message"]["content"])


def read_answer(text: str, evidence: dict[str, Any], *, max_summary_chars: int = 300,
                id_pattern: str = DEFAULT_ID_PATTERN) -> tuple[str, list[dict[str, Any]]]:
    """幻覺檢查：LLM 點名的系統與機台／批號必須真的在證據裡。通過回傳 (摘要, 可能的原因)，不通過丟 ValueError。

    什麼叫「在證據裡」：是告警的系統、關係邊或呼叫鏈裡的系統，**或者名稱原樣出現在送給模型的證據文字裡**（不分大小寫）。
    後者是為了日誌訊息裡指名的元件：`Cannot connect to MariaDb`、`datasource vmselect-…-k8s-stack returned 503`。
    它們沒有告警、也沒有人把它寫進關係邊，但確實是日誌寫的，不是模型編的。關係邊有寫當然更好，沒寫不該擋掉分析
    （使用者的決定，2026-11-26，現場 AP1105 的兩張事故就是這樣被退回的）。
    證據裡完全找不到的名稱：排第一的可能原因是它、或結論的句子提到它 → 整份不採用；只是排在後面的可能原因 → 只拿掉那一個。

    可能的原因最多留 MAX_CANDIDATES 個，每個是 {source, grade, reason}。**模型不給分數**：grade 由 evidence_grade() 照證據判定；
    模型自己標成推測（kind=inference）的一律是 inference（只會往下調，不會往上）。排序照等級：直接證據在前、推測在後，同一級照模型給的順序。"""
    body = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    answer = json.loads(body)
    summary = " ".join(str(answer["summary"]).split())
    if not summary:
        raise ValueError("summary 是空的")
    systems = {a["system"] for a in evidence["anomalies"]} | {e[k] for e in evidence["edges"] for k in ("upstream", "downstream")}
    # 時間線與呼叫鏈裡實際出現的系統也算有憑據（例如呼叫鏈指到一個沒有告警、也不在關係邊裡的資料庫）
    systems |= {s for group in (evidence.get("timelines") or [], evidence.get("call_chains") or [], evidence.get("trace_logs") or [])
                for item in group for s in item.get("systems") or []}
    ids = {v for counts in evidence["id_counts"].values() for v in counts}
    # 比對用的文字 = 送給模型的那些內容。pattern（數字換成 * 的樣板）模型看不到，也不能當憑據：k*s 不是真的名稱
    visible = {**evidence, **{key: [{k: v for k, v in row.items() if k != "pattern"} for row in evidence.get(key) or []]
                              for key in ("error_patterns", "warnings")}}
    corpus = json.dumps(visible, ensure_ascii=False)
    lowered = corpus.lower()

    def grounded(name: Any) -> bool:
        """這個名稱有憑據：是已知的系統，或原樣（不分大小寫）出現在證據文字裡。太短的名稱不用文字比對（隨便都對得到）。"""
        text = str(name).strip()
        return text in systems or (len(text) >= MIN_NAME_CHARS and text.lower() in lowered)

    # systems 清單是模型自己列的「我提到了誰」。清單裡找不到憑據的名稱，只有當結論的句子真的提到它才算問題
    unknown = [s for s in answer.get("systems") or [] if not grounded(s) and str(s).strip() and str(s).lower() in summary.lower()]
    source = answer.get("source") or ""   # 舊格式（自訂提示詞可能還在用）：有填就必須有憑據
    if source and not grounded(source):
        unknown.append(source)
    # ids：在識別碼統計裡，或原樣出現在送給模型的證據裡（跟下面摘要文字同一個標準）都算有憑據。
    # 只認統計會誤擋兩種真的有憑據的情況：日誌沒拆欄位、機台與批號只寫在訊息文字裡；以及有拆欄位但沒排進 top_ids 的。
    # 這是字串比對：編出來的 EQP-1 剛好是證據裡 EQP-10 的開頭時擋不到（摘要文字的檢查本來就有同樣的限制）。
    unknown += [i for i in answer.get("ids") or [] if i not in ids and not (isinstance(i, str) and i.strip() and i in corpus)]
    raw = answer.get("candidates")
    raw = raw if isinstance(raw, list) else []   # 選填：自訂提示詞可以不要求
    candidates: list[dict[str, Any]] = []
    for rank, item in enumerate(raw[:MAX_CANDIDATES]):
        if not isinstance(item, dict) or not str(item.get("source") or "").strip():
            raise ValueError("candidates 的每一項都要有 source")
        name = str(item["source"]).strip()
        if not grounded(name):
            if rank == 0:   # 排第一的就是結論在講的那一個：它沒有憑據，整份不能用
                unknown.append(name)
            continue        # 排在後面的：只拿掉這一個，其他照用
        # 模型自己說這一條是推測的（證據沒有直接寫）：不管那個系統有多少證據，這一條就是推測
        inferred = str(item.get("kind") or "").strip().lower() == "inference"
        candidates.append({"source": name, "grade": "inference" if inferred else evidence_grade(name, evidence),
                           "reason": " ".join(str(item.get("reason") or "").split())})
    candidates.sort(key=lambda c: GRADES.index(c["grade"]))   # 穩定排序：同一級照模型給的順序
    # 摘要與理由的文字裡夾數字的識別碼（機台、批號樣式）也要在證據裡找得到：防它只寫在句子裡、沒列進 ids
    # 泛稱（HTTP2、v2 這類）不在證據裡也會被擋：樣式不合用就改 llm.id_pattern，設成空字串 = 關掉這一項
    if id_pattern:
        for sentence in (summary, *(c["reason"] for c in candidates)):
            unknown += [t for t in re.findall(id_pattern, sentence) if t not in corpus]
    if unknown:
        raise ValueError(f"點名了證據裡沒有的名稱：{sorted(set(map(str, unknown)))}")
    return summary[:max_summary_chars], candidates


MAX_ACTIONS = 3        # 註記上最多列幾條下一步建議（版面的上限）
ACTION_CHARS = 120     # 每條建議最多幾個字


def read_actions(text: str, evidence: dict[str, Any], *, id_pattern: str = DEFAULT_ID_PATTERN) -> list[str]:
    """LLM 回答裡的下一步建議（actions）：最多 MAX_ACTIONS 條，每條截到 ACTION_CHARS 個字。選填，沒有就是空的。
    建議裡出現機台、批號樣式的識別碼，一樣要在證據裡找得到；找不到的那一條拿掉（不因此退回整份分析）。
    這些只是寫在註記上給人看的文字：程式不會執行，也不會拿它去觸發修復流程。"""
    body = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    raw = json.loads(body).get("actions")
    corpus = json.dumps(evidence, ensure_ascii=False)
    out = []
    for item in raw if isinstance(raw, list) else []:
        line = " ".join(str(item).split()) if isinstance(item, str) else ""
        if line and not (id_pattern and any(token not in corpus for token in re.findall(id_pattern, line))):
            out.append(line[:ACTION_CHARS])
    return out[:MAX_ACTIONS]


def check_answer(text: str, evidence: dict[str, Any], *, max_summary_chars: int = 300,
                 id_pattern: str = DEFAULT_ID_PATTERN) -> str:
    """read_answer 只取摘要的那一半（通過檢查回傳摘要文字，不通過丟 ValueError）。"""
    return read_answer(text, evidence, max_summary_chars=max_summary_chars, id_pattern=id_pattern)[0]


@dataclass(frozen=True)
class Analysis:
    line: str          # 註記的「根因分析」那一行：[LLM 推論] … 或 [未經分析] …
    analyzed: bool     # 是不是通過檢查的 LLM 推論
    reason: str        # 沒通過的原因
    candidates: str    # 註記上「可能原因」的幾行（candidates_text）；沒有就是空字串
    actions: str = ""  # 註記上「下一步建議｜…」那一行（actions_text）；沒有就是空字串
    failure: str = ""  # 沒有分析的話是哪一種：unreachable（連不上、逾時、回錯誤碼）／rejected（有回答但沒通過檢查）；其餘是空的


def analyze_full(incident: Incident, evidence: dict[str, Any], *, api_url: str, api_key: str, model: str,
                 transport: httpx.BaseTransport | None = None, options: LlmOptions = LlmOptions(),
                 overview_text: str = "") -> Analysis:
    """問 LLM 並檢查回答。任何失敗都退回原始摘要（沒有可能原因），不丟例外。"""
    fallback = f"[未經分析] {report.raw_summary(incident)}"
    if not api_url or not model:
        return Analysis(fallback, False, "沒有設定 llm.url、llm.model", "")
    try:
        prompt = build_prompt(evidence, overview_text)
        log.debug("事故 %s 送給 LLM 的內容：\n%s", incident.uid, prompt)
        answer = call_llm(api_url, api_key, model, prompt, transport, timeout=options.timeout_seconds,
                          temperature=options.temperature, system_prompt=options.system_prompt)
        log.debug("事故 %s LLM 的原始回應：%s", incident.uid, answer)
        summary, candidates = read_answer(answer, evidence, max_summary_chars=options.max_summary_chars,
                                          id_pattern=options.id_pattern)
    except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as e:  # ValueError 含 JSON 解析失敗
        reason = f"{type(e).__name__}: {e}"[:300]
        log.warning("事故 %s 的 LLM 分析沒有採用：%s", incident.uid, reason)
        # 連不上、逾時、回 4xx／5xx 是「LLM 不通」；其餘是它有回答、但格式不對或沒通過幻覺檢查
        return Analysis(fallback, False, reason, "", failure="unreachable" if isinstance(e, httpx.HTTPError) else "rejected")
    return Analysis(f"[LLM 推論] {summary}", True, "", report.candidates_text(candidates, options.max_summary_chars),
                    report.actions_text(read_actions(answer, evidence, id_pattern=options.id_pattern)))


def analyze(incident: Incident, evidence: dict[str, Any], *, api_url: str, api_key: str, model: str,
            transport: httpx.BaseTransport | None = None, options: LlmOptions = LlmOptions(),
            overview_text: str = "") -> tuple[str, bool, str]:
    """analyze_full 的前三項：(註記的根因分析那一行, 是不是通過檢查的 LLM 推論, 沒通過的原因)。"""
    got = analyze_full(incident, evidence, api_url=api_url, api_key=api_key, model=model, transport=transport,
                       options=options, overview_text=overview_text)
    return got.line, got.analyzed, got.reason
