"""機率模型的所有數字：先驗、條件機率、顯著門檻。要調模型只改這一檔，不用動推論程式。

怎麼讀這張表（模型在 model.py，這裡只放數字）：
- 假設空間 = 「根因是實體 X、故障子型 k」×（所有實體 × 所有子型）＋「根因不在模型裡（UNKNOWN）」。單一根因假設。
- 每個「觀察」（Fact 離散化成顯著／不顯著，或 EvidenceItem 的軟證據）掛在某個實體上；那個實體跟根因的關係決定它的出現機率：
    1. 它就是根因                → DIRECT[子型][症狀]
    2. 它被根因傳導到（呼叫了根因）→ 混合三種呼叫端機制 MECHANISM_PRIOR：傳導變慢／熔斷快速失敗／逾時放棄，
                                   各機制的症狀機率在 MECHANISM[機制][症狀]，離根因每多一跳乘 HOP_DECAY
    3. 它被根因呼叫（根因的下游）  → CALLEE_OF_ROOT（例如根因直接回錯，就不再呼叫下游 → 下游流量掉）
    4. 跟根因無關                 → 只有 LEAK（背景雜訊、其他小毛病）
- noisy-OR：P(症狀出現) = 1 − (1 − LEAK)(1 − 原因機率)。所以表裡的數字是「這個原因單獨能讓症狀出現的機率」，
  漏失率永遠墊在底下，不會出現 0 或 1 的絕對判斷。

數字的來源分三類，每一列註解會標：
- 【依據】有公開資料或機制上必然（例如 CPU 飽和一定讓自身耗時上升）。
- 【機制】從呼叫機制推得出方向與大小級距（例如熔斷時呼叫端不等下游，延遲不會漲），具體數字仍是估計。
- 【拍腦袋】只有方向感，數字是初始猜測，要靠混沌回放與事後回饋校正。
"""
from __future__ import annotations

# ── 假設空間 ──

# 根因不在模型裡（網路、基礎設施、沒進拓撲的外部服務…）的先驗。【拍腦袋】給 10%：
# 太小會讓模型對「拓撲沒涵蓋的根因」過度自信，太大會讓證據稍弱就整份回「無法判定」。
UNKNOWN_PRIOR = 0.10

# 實體的相對先驗權重。【拍腦袋】事件群裡的實體（偵測器真的抓到異常）權重 1；
# 只出現在拓撲鄰居、事實或證據裡的實體 0.4（熔斷時被切斷的下游常常沒有自己的告警，不能給 0，但也比較不可能）。
INCIDENT_ENTITY_WEIGHT = 1.0
OTHER_ENTITY_WEIGHT = 0.4

# 自身故障子型的先驗（同一個實體內，加總 = 1）。
SUBTYPE_PRIOR: dict[str, float] = {
    # 【依據】Google SRE 書與多份事故統計：變更（部署、設定）引起的故障約占七成，表現多半是直接回錯而不是變慢
    "error": 0.24,
    # 【拍腦袋】CPU 飽和／熱點迴圈：自身變慢最常見的形態
    "cpu": 0.20,
    # 【拍腦袋】下游資源（DB、外部 API）變慢，服務自己的程式沒問題
    "dependency": 0.16,
    # 【拍腦袋】連線池／執行緒池耗盡
    "conn_pool": 0.12,
    # 【拍腦袋】鎖競爭、死結
    "lock": 0.09,
    # 【拍腦袋】JVM／runtime GC 停頓
    "gc": 0.08,
    # 【拍腦袋】記憶體耗盡（OOM、被 kill 重啟）
    "memory": 0.07,
    # 【拍腦袋】訊息中介異常或消費端積壓
    "messaging": 0.04,
}

SUBTYPE_LABEL: dict[str, str] = {
    "error": "程式錯誤／壞版本（直接回錯）",
    "cpu": "計算資源飽和（CPU）",
    "dependency": "等待的外部依賴變慢",
    "conn_pool": "連線池／執行緒池耗盡",
    "lock": "鎖競爭／死結",
    "gc": "GC 停頓",
    "memory": "記憶體耗盡",
    "messaging": "訊息中介／消費積壓",
}

# ── 症狀詞彙（內部用）──
# Fact.kind → 症狀；EvidenceItem.kind 直接當症狀名。
FACT_SYMPTOM: dict[str, str] = {
    "self_time_delta": "self_time",
    "wait_delta": "wait",
    "entry_latency": "entry_latency",
    "entry_failure_rate": "entry_failure",
    "fanout_ratio": "fanout",
    "failed_without_failed_child": "fail_no_child",
    "traffic_drop": "traffic_drop",
    "log_template_surge": "log_surge",
    "mq_lag": "mq_lag",
    "child_outlives_parent": "child_outlives",
}
# 熔斷證據掛在「沒有下游」的實體上時，代表它是被熔斷的那一端（日誌寫的是「到 X 的熔斷器打開」）
BREAKER_TARGET = "breaker_target"

# ── 背景漏失率：跟根因無關也會出現的機率 ──
LEAK: dict[str, float] = {
    "self_time": 0.05,       # 【拍腦袋】正常波動越過顯著門檻
    "wait": 0.05,
    "entry_latency": 0.05,
    "entry_failure": 0.04,
    "fanout": 0.03,
    "fail_no_child": 0.03,
    "traffic_drop": 0.05,    # 【拍腦袋】流量本來就有起伏、上游改路由
    "log_surge": 0.08,       # 【拍腦袋】日誌樣板暴增很常見、很多是無害的
    "mq_lag": 0.04,
    "child_outlives": 0.08,  # 【機制】時鐘偏移、非同步收尾（fire-and-forget）也會讓子 span 晚結束
    # 證據（LLM 抽出、已驗證有原文）：服務沒壞也會印的機率
    "circuit_breaker_open": 0.02,
    BREAKER_TARGET: 0.02,
    "gc_pause": 0.06,        # 【依據】JVM 平常就會印 GC log，只是停頓短
    "oom": 0.01,
    "connection_pool_exhausted": 0.02,
    "deadlock": 0.01,
    "lock_wait": 0.03,
    "client_timeout": 0.04,
    "dependency_unreachable": 0.03,
    "deployment_change": 0.05,  # 【拍腦袋】部署很頻繁，事件期間剛好有部署不代表是它
    "broker_unavailable": 0.02,
}
DEFAULT_LEAK = 0.05

# ── 1. 實體就是根因：DIRECT[子型][症狀] ──
# 所有子型共用的部分（ANY），再由各子型覆寫。
DIRECT_ANY: dict[str, float] = {
    "entry_latency": 0.75,    # 【機制】大多數故障都會讓入口變慢
    "entry_failure": 0.35,
    "log_surge": 0.6,         # 【拍腦袋】出事的服務通常會噴新的錯誤樣板
    "deployment_change": 0.25,  # 【依據】變更占事故多數；但 LLM 不一定看得到部署日誌
    "traffic_drop": 0.35,     # 【機制】它的呼叫端熔斷了 → 打進它的流量掉（只有呼叫端有熔斷器時才會）
    BREAKER_TARGET: 0.5,      # 【機制】日誌指名「到它的熔斷器打開」
    "self_time": 0.2,
    "wait": 0.1,
    "fail_no_child": 0.25,
    "fanout": 0.05,
}
DIRECT: dict[str, dict[str, float]] = {
    "error": {
        "entry_failure": 0.9,     # 【機制】壞版本／程式錯誤就是回錯
        "fail_no_child": 0.8,     # 【機制】錯誤在自己身上產生，底下沒有失敗的子 span
        "entry_latency": 0.3,     # 【機制】回錯通常很快
        "self_time": 0.15,
        "deployment_change": 0.45,  # 【依據】約七成事故跟變更有關
    },
    "cpu": {
        "self_time": 0.9,         # 【依據】CPU 飽和一定讓自身（非等待）耗時上升
        "entry_latency": 0.85,
        "entry_failure": 0.25,
        "fail_no_child": 0.15,
    },
    "dependency": {
        "wait": 0.9,              # 【機制】慢在等下游資源，等待增量占主導
        "self_time": 0.1,         # 【機制】自己的程式沒變慢
        "entry_latency": 0.85,
        "client_timeout": 0.35,   # 【機制】等太久會自己逾時放棄
        "dependency_unreachable": 0.4,
        "fail_no_child": 0.2,
    },
    "conn_pool": {
        "connection_pool_exhausted": 0.85,  # 【機制】池子滿了框架幾乎一定印這句
        "wait": 0.5,              # 【機制】等拿連線的時間有時會算進等待
        "entry_failure": 0.6,     # 【機制】拿不到連線逾時 → 回錯
        "fail_no_child": 0.55,    # 【機制】連線都沒拿到，所以沒有失敗的子 span
        "self_time": 0.3,
        "entry_latency": 0.8,
    },
    "lock": {
        "lock_wait": 0.8,
        "deadlock": 0.5,          # 【拍腦袋】有一半是真的死結、一半是一般鎖競爭
        "wait": 0.6,              # 【機制】等鎖常在 DB span 裡
        "self_time": 0.35,
        "entry_latency": 0.85,
        "entry_failure": 0.4,
    },
    "gc": {
        "gc_pause": 0.85,         # 【依據】長停頓一定有 GC log
        "self_time": 0.8,         # 【機制】停頓期間所有請求的自身耗時都被拉長
        "entry_latency": 0.85,
        "oom": 0.1,
        "entry_failure": 0.2,
        "fail_no_child": 0.15,
    },
    "memory": {
        "oom": 0.8,
        "gc_pause": 0.4,          # 【機制】OOM 前常有連續 Full GC
        "self_time": 0.5,
        "entry_failure": 0.6,     # 【機制】被 kill 重啟期間請求失敗
        "fail_no_child": 0.5,
    },
    "messaging": {
        "broker_unavailable": 0.7,
        "mq_lag": 0.7,
        "entry_latency": 0.4,
    },
}

# 根因實體「等待的資源」本身也是模型裡的實體時，這份等待比較該算在那個實體頭上（由它當根因來解釋），
# 不是這個實體的「外部依賴變慢」。【機制】乘這個折扣，避免同一份等待被兩個假設重複拿去當證據。
MODELED_RESOURCE_PENALTY = 0.3

# ── 2. 實體被根因傳導到（它直接或間接呼叫了根因）──
# 呼叫端怎麼反應是一個隱變數（混合模型）：同一個呼叫端的所有症狀共用同一個機制，不能各自挑最有利的。
MECHANISM_PRIOR: dict[str, float] = {
    "propagate": 0.70,  # 【拍腦袋】一般情況：跟著等、跟著變慢
    "breaker": 0.10,    # 【拍腦袋】有熔斷器而且跳開：多數服務沒裝或門檻沒到
    "timeout": 0.20,    # 【拍腦袋】客戶端逾時很普遍（HTTP client 預設都有）
}
MECHANISM_LABEL: dict[str, str] = {"propagate": "被傳導變慢", "breaker": "熔斷（快速失敗）", "timeout": "逾時放棄"}
MECHANISM: dict[str, dict[str, float]] = {
    "propagate": {
        "entry_latency": 0.85,    # 【機制】同步呼叫：下游慢多少、上游就跟著慢
        "wait": 0.85,             # 只算「朝根因方向」的等待（等的是根因或傳導路徑上的實體）
        "entry_failure": 0.4,
        "fail_no_child": 0.1,     # 【機制】失敗是從下游傳上來的，底下有失敗的子 span
        "self_time": 0.1,         # 【機制】呼叫端自己的程式沒變慢——這是跟「自身 CPU 故障」分開的關鍵
        "fanout": 0.15,           # 【拍腦袋】重試
        "log_surge": 0.5,
        "client_timeout": 0.1,
        "dependency_unreachable": 0.15,
        "lock_wait": 0.05,
        "connection_pool_exhausted": 0.2,  # 【機制】下游慢 → 呼叫端連線被佔住 → 池子也滿（連鎖耗盡）
        "child_outlives": 0.1,
    },
    "breaker": {
        "entry_failure": 0.9,     # 【機制】熔斷 = 直接回錯
        "fail_no_child": 0.85,    # 【機制】根本沒呼叫下游，所以沒有子 span 可以失敗（場景 17：0.2 ms 快速失敗）
        "entry_latency": 0.1,     # 【機制】快速失敗，延遲不漲甚至下降
        "self_time": 0.05,
        "wait": 0.1,              # 【機制】不呼叫就沒有等待
        "circuit_breaker_open": 0.8,  # 【依據】Resilience4j／Hystrix 狀態轉換預設會印 log
        "log_surge": 0.6,
        "fanout": 0.05,           # 【機制】不呼叫 → 扇出反而下降
        "child_outlives": 0.02,
    },
    "timeout": {
        "entry_failure": 0.85,    # 【機制】逾時放棄 → 回錯
        "fail_no_child": 0.6,     # 【機制】錯誤在呼叫端產生；子 span 還在跑、不算失敗
        "entry_latency": 0.8,     # 【機制】延遲漲到逾時上限（場景 16：上游 2 秒）
        "wait": 0.9,
        "self_time": 0.1,
        "client_timeout": 0.85,   # 【依據】HTTP client／gRPC deadline 逾時都有固定錯誤訊息
        "child_outlives": 0.7,    # 【機制】上游放棄後子 span 還在跑（場景 16：DB span 15 秒）
        "fanout": 0.3,            # 【機制】逾時常伴隨重試
        "log_surge": 0.6,
        "dependency_unreachable": 0.2,
    },
}
# 根因是鎖或連線池時，呼叫端也常印同類訊息（等的是同一把鎖／同一個池）。【機制】覆寫 MECHANISM 的 propagate。
PROPAGATE_BY_SUBTYPE: dict[str, dict[str, float]] = {
    "lock": {"lock_wait": 0.6},           # 鎖車隊（場景 18）：一大群呼叫端都在等同一把鎖
    "conn_pool": {"connection_pool_exhausted": 0.35},
}
# 每多隔一跳，傳導效果打折。【拍腦袋】0.7：兩跳以外的呼叫端常被中間層的快取、逾時吸收掉一部分。
HOP_DECAY = 0.7
# 同一個強連通分量（循環呼叫 A↔B）裡的成員互相傳導的強度。【機制】環上的方向沒有意義（誰都能傳給誰），
# 所以不看跳數、一律用同一個打折係數，避免「離得近」被誤當證據；分量內誰是源頭只看各自身上的證據。
INTRA_SCC_FACTOR = 0.6

# ── 3. 實體被根因呼叫（根因的下游）──
CALLEE_OF_ROOT: dict[str, float] = {
    "traffic_drop": 0.3,  # 【機制】根因自己先失敗，就不再呼叫下游 → 下游流量掉（所以「流量掉」兩邊都能解釋）
}

# ── 4. 根因不在模型裡（UNKNOWN）：每個實體都當成被一個看不到的根因傳導一跳，強度打折 ──
# 【拍腦袋】0.5：外部根因通常只打到一部分實體
UNKNOWN_REACH = 0.5

# ── 事實的顯著門檻（數值 → 顯著／不顯著）──
# 毫秒類事實：增量至少 ABS 毫秒，而且（有 before 時）事件期間 ÷ 基準至少 RATIO 倍。
MS_THRESHOLD: dict[str, tuple[float, float]] = {
    "self_time_delta": (20.0, 1.5),   # 【拍腦袋】20 ms 以下多半是量測抖動
    "wait_delta": (20.0, 1.5),
    "entry_latency": (50.0, 1.5),     # 【拍腦袋】入口延遲比單一 span 抖得大
    "mq_lag": (1000.0, 2.0),          # 【拍腦袋】訊息延遲以秒計
    "child_outlives_parent": (500.0, 0.0),  # 【依據】主機間時鐘偏移通常在百毫秒內；超過 0.5 秒才當真
}
# 比例類事實：值至少 MIN（entry_failure_rate 另外要求比基準多 MIN_INCREASE）。
RATIO_THRESHOLD: dict[str, float] = {
    "entry_failure_rate": 0.05,         # 【拍腦袋】5% 錯誤率
    "failed_without_failed_child": 0.5,  # 【機制】過半失敗都沒有失敗的子 span，才算「錯誤從這裡產生」
    "fanout_ratio": 1.5,                # 【拍腦袋】每筆請求的呼叫數變成 1.5 倍（重試風暴）
    "traffic_drop": 0.5,                # 流量掉一半以上（值 = 下降比例，見 observe.py）
    "log_template_surge": 3.0,          # 【拍腦袋】樣板頻率變成 3 倍
}
FAILURE_MIN_INCREASE = 0.05

# ── 輸出 ──
# 說明「這個候選為什麼」時最多列幾筆證據
TOP_REASONS = 3
EVIDENCE_USED_LIMIT = 10
