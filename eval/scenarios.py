"""評估用的案例：每個案例是一組告警、關係邊、原始日誌，以及「正確答案」。日誌會真的寫進 Elasticsearch，
證據由 aiops-lite 的程式經 Grafana 撈回來，所以查詢條件與前處理的改動都會反映在結果上。

時間都用「幾秒前」表示（相對於執行當下）。告警的 startsAt 比真正出事的時間晚是刻意的：規則通常有持續時間的條件。
答案的欄位：
  source   因果鏈裡要出現的系統；清單 = 其中任何一個在鏈裡都算（兩端共同構成、證據分不出哪一端的情況）；
           "" = 正確答案是「還無法判定」；None = 這個案例不評
  any      摘要裡至少要出現其中一個詞（有講到重點）
這些案例是人造的，用來比較改動前後，不代表產線上的準確率。"""
from __future__ import annotations

NOISE_FROM = 2400   # 常態雜訊從 40 分鐘前開始，一路到現在：事故前後的頻率一樣


def every(step: int, service: str, level: str, message: str, start: int = NOISE_FROM, stop: int = 0, **fields):
    """從 start 秒前到 stop 秒前，每 step 秒一筆。message 裡的 {n} 換成流水號（模擬每筆不同的數字）。"""
    return [(ago, service, level, message.format(n=i), fields) for i, ago in enumerate(range(start, stop, -step))]


def at(times: list[int], service: str, level: str, message: str, **fields):
    return [(ago, service, level, message.format(n=i), fields) for i, ago in enumerate(times)]


SCENARIOS = [
    {
        "name": "s01_disk_full_chain", "about": "上游 broker 磁碟滿，一路影響下游（基本題）",
        "edges": [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")],
        "alerts": [("mq-broker", "MQ queue depth", "P1", 170), ("cim-eap", "CIM T3 timeout", "P2", 140), ("amhs-mcs", "AMHS transfer duration", "P2", 100)],
        "logs": at([175, 165], "mq-broker", "ERROR", "disk usage 9{n}% on /data, producers blocked")
                + at([150], "mq-broker", "FATAL", "cannot append to queue eap.events: no space left on device")
                + at([140, 130, 125], "cim-eap", "ERROR", "publish to eap.events timed out after 5000ms", eqp_id="EQP-CVD-07", lot_id="LOT24A0031")
                + at([100, 95], "amhs-mcs", "ERROR", "no dispatch command received in 60s, transfer aborted", carrier_id="FOUP0912"),
        "source": "mq-broker", "any": ["磁碟", "disk", "空間", "space"],
    },
    {
        "name": "s02_noise_hides_real_error", "about": "上游一直在噴常態錯誤，真正的新錯誤在下游，而且比較早、被洗掉",
        "edges": [("cim-eap", "amhs-mcs")],
        "alerts": [("cim-eap", "CIM error log rate", "P2", 150), ("amhs-mcs", "AMHS transfer aborted", "P1", 140)],
        "logs": every(3, "cim-eap", "ERROR", "heartbeat to MES retry failed attempt={n}")
                + at([420, 410, 400, 390], "amhs-mcs", "ERROR", "PLC connection lost on STK01 (10.3.2.15:502), transfers halted"),
        "source": "amhs-mcs", "any": ["PLC"],
    },
    {
        "name": "s03_single_system_oom", "about": "只有一個系統告警，它自己記憶體不足",
        "edges": [],
        "alerts": [("mes-api", "MES API 5xx rate", "P1", 150)],
        "logs": at([200, 190], "mes-api", "ERROR", "java.lang.OutOfMemoryError: Java heap space")
                + at([180], "mes-api", "ERROR", "GC overhead limit exceeded, heap 3987/4096 MB")
                + every(12, "mes-api", "ERROR", "request {n} failed: 500 internal error", start=170),
        "source": "mes-api", "any": ["記憶體", "OutOfMemory", "heap", "Heap"],
    },
    {
        "name": "s04_service_name_mismatch", "about": "日誌的系統名稱寫法跟告警不一樣，撈不到",
        "edges": [],
        "alerts": [("test-svc", "High error rate", "P2", 150)],
        "logs": at([170, 160, 150], "test_svc", "ERROR", "upstream returned 503"),
        "source": "", "any": [],
    },
    {
        "name": "s05_middle_fails_first", "about": "中間的系統先出錯，上游後來才大量報錯，把早期的錯誤洗掉",
        "edges": [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")],
        "alerts": [("cim-eap", "CIM processing stalled", "P2", 170), ("amhs-mcs", "AMHS transfer duration", "P2", 160), ("mq-broker", "MQ queue depth", "P1", 150)],
        "logs": at([520, 510, 500], "cim-eap", "ERROR", "worker pool stalled: 0 of 16 workers responding")
                + at([430, 420], "amhs-mcs", "ERROR", "no dispatch command received in 60s, transfer aborted")
                + every(2, "mq-broker", "ERROR", "consumer cim-eap not acking, queue eap.events length {n}", start=330),
        "source": "cim-eap", "any": ["worker", "停擺", "stalled", "沒有回應", "無回應"],
    },
    {
        "name": "s06_warning_before_error", "about": "出錯前先有警告講出原因，錯誤本身只寫 failed",
        "edges": [],
        "alerts": [("db-proxy", "DB proxy error rate", "P1", 150)],
        "logs": at([430, 400, 370], "db-proxy", "WARN", "connection pool usage 9{n}% (184/200)")
                + at([340], "db-proxy", "WARN", "connection pool exhausted, 37 requests waiting")
                + every(10, "db-proxy", "ERROR", "request {n} failed", start=300),
        "source": "db-proxy", "any": ["連線池", "pool"],
    },
    {
        "name": "s07_shared_equipment_unparsed", "about": "日誌沒拆欄位，兩個系統的訊息都提到同一台機台",
        "edges": [("cim-eap", "amhs-mcs")],
        "alerts": [("cim-eap", "CIM T3 timeout", "P2", 150), ("amhs-mcs", "AMHS transfer aborted", "P2", 130)],
        "logs": at([200, 185, 170, 160, 150, 140], "cim-eap", "ERROR", "HSMS T3 timeout eqp=EQP-CVD-07 stream=6 function=11")
                + at([135, 125, 115, 105], "amhs-mcs", "ERROR", "transfer aborted: destination EQP-CVD-07 load port not ready")
                + at([300, 250], "cim-eap", "INFO", "recipe download ok eqp=EQP-ETCH-02"),
        "source": None, "any": ["EQP-CVD-07"],
    },
    {
        "name": "s08_only_chronic_noise", "about": "兩個系統告警，日誌裡只有一直都在的常態錯誤：正確答案是還無法判定",
        "edges": [("registry-sync", "svc-b")],
        "alerts": [("registry-sync", "Registry sync error log rate", "P3", 150), ("svc-b", "svc-b latency p95", "P2", 140)],
        "logs": every(6, "registry-sync", "ERROR", "heartbeat to registry retry failed attempt={n}")
                + every(60, "svc-b", "INFO", "request batch {n} served"),
        "source": "", "any": [],
    },
    {
        "name": "s09_dependency_without_alert", "about": "只有一個系統告警，它的日誌指出是沒有告警的相依服務連不上",
        "edges": [("redis-cache", "order-api")],
        "alerts": [("order-api", "Order API 5xx rate", "P1", 150)],
        "logs": every(12, "order-api", "ERROR", "cache lookup {n} failed: connection refused redis-cache:6379", start=200),
        "source": "redis-cache", "any": ["redis-cache"],
    },
    # ── 2026-11-26 現場回報（AP1105，真的日誌）做成的兩個案例。日誌內容是照回報裡看得到的片段重寫的，不是原文 ──
    {
        "name": "s11_dependency_only_in_log_text", "about": "現場回報：告警的系統日誌寫連不上某個元件，那個元件沒有告警、也沒有寫進關係邊",
        "edges": [],
        "alerts": [("instana-agent", "instana-agent 錯誤日誌異常", "P2", 150)],
        "logs": every(20, "instana-agent", "ERROR", "MariaDb | com.instana.sensor-mariadb - 1.1.33 | Cannot connect to MariaDb at db-host:3306 (attempt {n})", start=240)
                + every(20, "instana-agent", "ERROR", "MariaDb | com.instana.sensor-mariadb - 1.1.33 | Error during sensor activation, retry {n}", start=235),
        # 答案鍵 2026-11-28 改過（使用者決定）：原本只認 MariaDb。只有 instana-agent 自己的日誌、沒有 MariaDb 的日誌，分不出是感測器、
        # 網路還是資料庫的問題；鏈寫 instana-agent、把 MariaDb 放在「另一種可能」是誠實的寫法，不該算錯。兩個任何一個在鏈裡都算，
        # 但說明裡一定要提到 MariaDb（any）。改之前這一題在因果鏈的版本是 0/5。
        "source": ["instana-agent", "MariaDb"], "any": ["MariaDb", "MariaDB", "mariadb"],
    },
    {
        "name": "s12_component_name_with_digits", "about": "現場回報：錯誤訊息裡的元件名稱帶數字（k8s），而且沒有寫進關係邊",
        "edges": [],
        "alerts": [("vmalert", "vmalert 錯誤日誌異常", "P2", 150)],
        "logs": every(15, "vmalert", "ERROR", 'VictoriaMetrics/app/vmalert/rule/group.go:394 group "kubernetes-apps": failed to execute rule {n}: '
                                               "datasource vmselect-vmstack-victoria-metrics-k8s-stack:8481 returned 503 Service Unavailable", start=260),
        "source": "vmselect-vmstack-victoria-metrics-k8s-stack", "any": ["vmselect"],
    },
    {
        "name": "s10_upstream_is_innocent", "about": "上游有告警但只有常態錯誤，新錯誤在下游自己（設定檔壞了）",
        "edges": [("mq-broker", "cim-eap")],
        "alerts": [("mq-broker", "MQ error log rate", "P3", 160), ("cim-eap", "CIM recipe failures", "P1", 150)],
        "logs": every(8, "mq-broker", "ERROR", "slow consumer on queue audit.log, lag {n}")
                + at([230], "cim-eap", "ERROR", "config parse error at line 42 after reload (recipe-map v2.3.1)")
                + every(9, "cim-eap", "ERROR", "recipe lookup {n} failed: recipe-map not loaded", start=220),
        "source": "cim-eap", "any": ["config", "設定", "recipe-map"],
    },
]


# ── 留出案例：寫完之後才第一次跑，沒有拿來調整程式、提示詞或任何常數。用來看改動是不是只對上面那十個有效 ──
# unparsed = 日誌沒拆欄位（K8s 容器標準輸出）：系統名稱在 kubernetes.container_name，等級只寫在訊息文字裡。
HELD_OUT = [
    {
        "name": "h01_expired_certificate", "about": "單一系統：一直有的雜訊之外，新出現憑證過期",
        "edges": [],
        "alerts": [("api-gateway", "Gateway 5xx rate", "P1", 150)],
        "logs": every(15, "api-gateway", "ERROR", "metrics push to collector failed, batch {n} dropped")
                + every(8, "api-gateway", "ERROR", "TLS handshake failed for request {n}: certificate has expired (CN=mes.internal)", start=260),
        "source": "api-gateway", "any": ["憑證", "certificate", "TLS"],
    },
    {
        "name": "h02_database_deadlock_under_spam", "about": "最下游的資料庫先出事、筆數很少；上面兩層後來大量報錯",
        "edges": [("inventory-db", "order-api"), ("order-api", "api-gw")],
        "alerts": [("api-gw", "Gateway 504 rate", "P1", 150), ("order-api", "Order API latency", "P2", 170), ("inventory-db", "DB aborted transactions", "P2", 180)],
        "logs": at([560, 540], "inventory-db", "WARN", "replication lag {n}5s on replica-2")
                + at([500, 480, 470], "inventory-db", "ERROR", "deadlock detected, transaction aborted (table stock_ledger)")
                + every(3, "order-api", "ERROR", "query {n} to inventory store timed out after 3000ms", start=400)
                + every(2, "api-gw", "ERROR", "upstream returned 504 for /orders/{n}", start=360),
        "source": "inventory-db", "any": ["deadlock", "死結", "死鎖", "鎖"],
    },
    {
        "name": "h03_nothing_new_on_either_side", "about": "兩個系統都只有平常就有的錯誤：正確答案是還無法判定",
        "edges": [("billing-api", "report-job")],
        "alerts": [("billing-api", "Billing error log rate", "P3", 150), ("report-job", "Report job duration", "P2", 140)],
        "logs": every(20, "billing-api", "ERROR", "currency table refresh skipped, attempt {n}")
                + every(45, "report-job", "ERROR", "optional column note_{n} missing, using default"),
        "source": "", "any": [],
    },
    {
        "name": "h04_existing_error_becomes_frequent", "about": "不是新錯誤，而是原本偶爾出現的錯誤暴增；另一個系統只有固定頻率的雜訊",
        "edges": [("scheduler", "worker-pool")],
        "alerts": [("scheduler", "Scheduler error log rate", "P3", 150), ("worker-pool", "Worker dropped jobs", "P1", 160)],
        "logs": every(12, "scheduler", "ERROR", "cron drift detected: {n} ms behind")
                + every(300, "worker-pool", "ERROR", "retry queue full, dropping job {n}", stop=320)
                + every(2, "worker-pool", "ERROR", "retry queue full, dropping job 9{n}", start=300),
        "source": "worker-pool", "any": ["retry queue", "佇列", "queue"],
    },
    {
        "name": "h05_unparsed_container_logs", "about": "現場那種沒拆欄位的容器日誌：等級只在訊息文字裡，INFO 的訊息也會提到 error 這個詞",
        "unparsed": True,
        "edges": [("mq-broker", "cim-eap"), ("cim-eap", "amhs-mcs")],
        "alerts": [("mq-broker", "MQ queue depth", "P1", 170), ("cim-eap", "CIM T3 timeout", "P2", 140), ("amhs-mcs", "AMHS transfer duration", "P2", 100)],
        "logs": every(20, "amhs-mcs", "INFO", "health probe {n}: 0 errors")
                + every(30, "cim-eap", "INFO", "recovered after error on retry {n}, continuing")
                + at([200, 190], "mq-broker", "ERROR", "disk usage 9{n}% on /data, producers blocked")
                + at([180], "mq-broker", "FATAL", "cannot append to queue eap.events: no space left on device")
                + at([150, 140, 130], "cim-eap", "ERROR", "publish to eap.events timed out after 5000ms lot=LOT24A0031 eqp=EQP-CVD-07")
                + at([110, 100], "amhs-mcs", "ERROR", "no dispatch command received in 60s, transfer aborted carrier=FOUP0912"),
        "source": "mq-broker", "any": ["磁碟", "disk", "空間", "space"],
    },
    {
        "name": "h06_timeout_names_the_dependency", "about": "只有呼叫端告警，它的日誌寫等不到另一個服務（逾時，不是連線被拒）",
        "edges": [("risk-engine", "payment-api")],
        "alerts": [("payment-api", "Payment API latency p95", "P1", 150)],
        "logs": every(7, "payment-api", "ERROR", "payment {n} rejected: timeout after 3000ms calling risk-engine /score", start=240),
        "source": "risk-engine", "any": ["risk-engine"],
    },
    {   # 2026-11-26 補的：放寬幻覺檢查之後才寫、才第一次跑。h01 從這一輪起不算乾淨的留出案例（見 README.md 第 12 節）
        "name": "h07_unlisted_storage_backend", "about": "告警的系統寫不進一個沒有告警、沒有關係邊的儲存元件（換一種說法、名稱帶數字）",
        "edges": [("report-job", "bi-portal")],
        "alerts": [("report-job", "Report job failures", "P2", 150), ("bi-portal", "Portal stale data", "P3", 120)],
        "logs": every(18, "report-job", "ERROR", "export {n} aborted: PUT to s3-archive-02 bucket reports failed with 507 Insufficient Storage", start=250)
                + every(40, "bi-portal", "WARN", "dashboard {n} shows data older than 30m", start=200),
        "source": "s3-archive-02", "any": ["s3-archive-02", "空間", "Storage", "storage"],
    },
]
