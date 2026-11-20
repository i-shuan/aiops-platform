"""產線指標模擬（MQ／CIM／AMHS）：每個指標在每一分鐘的值，只由「哪一分鐘」決定（固定種子）。

這是模擬資料，不是任何真實產線的量測：用來讓「Grafana 規則 -> 平台演算法 -> 告警卡」這條路有東西可以跑，
數字（佇列深度 40 上下、120 台機台、48 台天車…）是為了看起來像而挑的，沒有對照過真實環境。

同一個函式給兩個地方用，所以回補的歷史跟即時的值接得起來：
  - history.py：產生過去 N 天的 OpenMetrics 檔，回補進 Prometheus（訓練用的基準線）
  - exporter.py：即時 /metrics，可以注入故障

正常狀態的形狀：
  - 24 小時輪班、白天略忙（±15% 的日週期）、週末少一成
  - 佇列深度、吞吐、搬運耗時是連續值加雜訊；逾時數、搬運失敗數是稀疏計數（大多是 0）
  - 在線機台數、可用天車數是整數，偶爾少一台幾分鐘

故障 mq_consumer_down（骨牌）：以故障開始後第 e 分鐘計
  e >= 0  MQ 消費者全掉（4 -> 0）、吞吐掉到 2%、佇列每分鐘多積 280 筆
  e >= 3  CIM 收不到機台回報：逾時數每分鐘約 4 筆
  e >= 4  在線機台數每分鐘少 6 台，最多少 30 台
  e >= 6  AMHS 沒接到派工：搬運失敗每分鐘約 2 筆、搬運耗時每分鐘多 8 秒（最多 +120 秒）
  e >= 7  可用天車每分鐘少 3 台，最多少 20 台
"""
from __future__ import annotations

import hashlib
import math
import random
from datetime import datetime, timezone

LINE = "fab12"
# (指標名稱, 標籤, 說明)。service 標籤是事件的實體（平台用它收斂與畫關係）。
SERIES = [
    ("mq_queue_depth", {"line": LINE, "service": "mq-broker", "queue": "eap.lot.event"}, "佇列裡還沒被消費的訊息數"),
    ("mq_consumer_count", {"line": LINE, "service": "mq-broker", "queue": "eap.lot.event"}, "連著這個佇列的消費者數"),
    ("mq_msg_rate", {"line": LINE, "service": "mq-broker", "queue": "eap.lot.event"}, "每秒處理的訊息數"),
    ("cim_connected_eqp_count", {"line": LINE, "service": "cim-eap"}, "SECS/GEM 連線中的機台數"),
    ("cim_timeout_count", {"line": LINE, "service": "cim-eap"}, "這一分鐘機台通訊逾時的次數"),
    ("amhs_transfer_duration_seconds", {"line": LINE, "service": "amhs-mcs"}, "搬運任務平均耗時"),
    ("amhs_abort_count", {"line": LINE, "service": "amhs-mcs"}, "這一分鐘中止的搬運任務數"),
    ("amhs_active_vehicles", {"line": LINE, "service": "amhs-mcs"}, "可派工的天車數"),
]
FAULT_MQ = "mq_consumer_down"


def _rng(name: str, minute: int) -> random.Random:
    digest = hashlib.blake2b(f"{name}:{minute}".encode(), digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))


def _poisson(rng: random.Random, rate: float) -> int:
    """Knuth 的乘法取樣；rate 都很小（< 10），夠用。"""
    limit, k, p = math.exp(-rate), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def activity(ts: datetime) -> float:
    """產線有多忙（1 = 平均）：日週期 ±15%，高峰在下午三點；週末少一成。"""
    hour = ts.hour + ts.minute / 60
    return 1 + 0.15 * math.sin(2 * math.pi * (hour - 9) / 24) - (0.1 if ts.weekday() >= 5 else 0.0)


def _short_dip(name: str, minute: int, every: int, lasts: int) -> bool:
    """偶爾少一台、持續 lasts 分鐘：以 every 分鐘為一格，每格有 1/4 的機會在格子裡某個位置發生一次。"""
    slot = minute // every
    rng = _rng(f"{name}:dip", slot)
    if rng.random() >= 0.25:
        return False
    start = slot * every + rng.randrange(every - lasts)
    return start <= minute < start + lasts


def normal(name: str, ts: datetime) -> float:
    minute = int(ts.timestamp() // 60)
    rng, a = _rng(name, minute), activity(ts)
    if name == "mq_queue_depth":
        return max(0.0, round(40 * a + rng.gauss(0, 8)))
    if name == "mq_consumer_count":
        return 4.0
    if name == "mq_msg_rate":
        return max(0.0, round(850 * a + rng.gauss(0, 25), 1))
    if name == "cim_connected_eqp_count":
        return 119.0 if _short_dip(name, minute, 360, 5) else 120.0
    if name == "cim_timeout_count":
        return float(_poisson(rng, 0.02))
    if name == "amhs_transfer_duration_seconds":
        return max(1.0, round(95 + 40 * (a - 1) + rng.gauss(0, 4), 2))
    if name == "amhs_abort_count":
        return float(_poisson(rng, 0.01))
    if name == "amhs_active_vehicles":
        return 47.0 if _short_dip(name, minute, 480, 8) else 48.0
    raise KeyError(name)


def faulted(name: str, ts: datetime, elapsed_minutes: float, backlog: float) -> float:
    """故障 mq_consumer_down 進行中的值。backlog 是到這一刻為止多積的訊息數（由呼叫端逐分鐘累加）。"""
    e, base = elapsed_minutes, normal(name, ts)
    rng = _rng(f"{name}:fault", int(ts.timestamp() // 60))
    if name == "mq_queue_depth":
        return base + round(backlog)
    if name == "mq_consumer_count":
        return 0.0
    if name == "mq_msg_rate":
        return round(base * 0.02, 1)
    if name == "cim_timeout_count" and e >= 3:
        return float(_poisson(rng, 4.0))
    if name == "cim_connected_eqp_count" and e >= 4:
        return base - min(30, 6 * math.floor(e - 3))
    if name == "amhs_abort_count" and e >= 6:
        return float(_poisson(rng, 2.0))
    if name == "amhs_transfer_duration_seconds" and e >= 6:
        return round(base + min(120, 8 * (e - 5)), 2)
    if name == "amhs_active_vehicles" and e >= 7:
        return base - min(20, 3 * math.floor(e - 6))
    return base


BACKLOG_PER_MINUTE = 280.0   # 消費者全掉時每分鐘多積的訊息數
DRAIN_PER_MINUTE = 600.0     # 恢復後每分鐘消掉的積壓


def utc(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
