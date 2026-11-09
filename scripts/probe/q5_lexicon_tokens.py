"""Q5：PR B（claude/hardening-b-lexicon）封閉詞表的 token 邊界。

對一份貼近真實的 briefing context（cart 結帳鏈：payment-gateway、tax-gw:443、inventory_svc.v2、GET /api/v1/carts、
一個 32 碼 trace id），把各種三行摘要丟進 validate_briefing，看是放行（None）還是退回（原因）。
「期望」欄是審查者的判斷：escape = 應擋沒擋（幻覺逃逸），false-reject = 不該擋卻擋（誤拒）。

用法：python scripts/probe/q5_lexicon_tokens.py --root <PR B 的 aiops 原始碼根>
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402

TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"
CONTEXT = {
    "incident_meta": {"entities": ["cart-api", "payment-gateway"], "duration_minutes": 12, "severity": "P1"},
    "root_cause": {"candidate": "payment-gateway", "score": 0.82, "dependency": "tax-gw:443（external/http）",
                   "attribution": {"resource": "tax-gw:443", "kind": "external", "subtype": "http",
                                   "self_ms": {"before": 12.0, "during": 101.4},
                                   "wait_ms": {"before": 8.0, "during": 95.0}, "share": 0.93, "by": "latency"}},
    "root_note": None,
    "sample_trace": {"trace_id": TRACE, "endpoint": "GET /api/v1/carts", "duration_ms": 640.2, "status_code": 504,
                     "failed": True, "failed_resource": "tax-gw:443"},
    "top_error_logs": [{"service": "inventory_svc.v2", "error_type": "java.net.SocketTimeoutException",
                        "template": "Read timed out after <num> ms calling tax-gw", "count": 120, "ratio": 8.0}],
    "matched_runbook": {"id": "external-dependency-latency", "name": "下游外部依賴變慢", "actions": []},
}


def lines(summary: str, action: str = "【立即止血動作】對 tax-gw:443 啟用 circuit breaker",
          verify: str = "【驗證與排除項】看 p95 latency 回到 baseline") -> dict:
    return {"summary_line": "【一句話根因】" + summary, "action_line": action, "verification_line": verify}


CASES = [
    # (說明, 摘要第一行, 期望: pass / reject)
    ("中英夾雜無空白", "請檢查payment-gateway的設定", "pass"),
    ("中英夾雜＋幻覺元件", "請重啟kafka集群", "reject"),
    ("全形冒號 tax-gw：443", "payment-gateway 等 tax-gw：443 變慢", "pass"),
    ("全形冒號＋幻覺 port", "payment-gateway 等 tax-gw：8443 變慢", "reject"),
    ("半形冒號＋幻覺 port（±10%內）", "payment-gateway 等 tax-gw:480 變慢", "reject"),
    ("底線與點 inventory_svc.v2", "inventory_svc.v2 大量 SocketTimeoutException", "pass"),
    ("底線與點＋幻覺版本 v3", "inventory_svc.v3 大量 SocketTimeoutException", "reject"),
    ("斜線端點 GET /api/v1/carts", "GET /api/v1/carts 504", "pass"),
    ("斜線端點＋幻覺路徑", "GET /api/v1/orders 504", "reject"),
    ("斜線端點路徑穿越", "GET /api/v1/carts/../admin 504", "reject"),
    ("trace id 完整", f"範例 trace {TRACE} 504", "pass"),
    ("trace id 截半（前 16 碼）", f"範例 trace {TRACE[:16]} 504", "pass"),
    ("trace id 截一段純數字", "範例 trace 35773 504", "reject"),
    ("連字號結尾 k8s-node-", "k8s-node- 資源不足", "reject"),
    ("大小寫混用 Payment-Gateway", "Payment-Gateway 等 TAX-GW:443 變慢", "pass"),
    ("大小寫混用＋幻覺 Kafka", "Payment-Gateway 等 Kafka 變慢", "reject"),
    ("URL 編碼 payment%2Dgateway", "payment%2Dgateway 變慢", "pass"),
    ("URL 編碼路徑 %2Fapi%2Fv1%2Fcarts", "GET %2Fapi%2Fv1%2Fcarts 504", "pass"),
    ("URL 編碼藏幻覺 redis%2Ecache", "請清 redis%2Ecache", "reject"),
    ("全形英文 ｒｅｄｉｓ（幻覺）", "請重啟ｒｅｄｉｓ", "reject"),
    ("西里爾字母假冒 rеdis（е=U+0435）", "請重啟 rеdis", "reject"),
    ("幻覺 IP 10.0.0.5", "payment-gateway 連 10.0.0.5 逾時", "reject"),
    ("幻覺錯誤碼 ORA-12541", "tax-gw 回 ORA-12541", "reject"),
    ("幻覺純數字錯誤碼 1045", "tax-gw 回錯誤碼 1045", "reject"),
    ("中文幻覺無識別字（已知死區）", "請重啟資料庫主機", "reject"),
    ("幻覺純數字錯誤碼但撞到 context 數值 95", "tax-gw 回錯誤碼 95", "reject"),
    ("幻覺 port 純數字 480（443 的 ±10%）", "payment-gateway 改連 port 480", "reject"),
    ("數字黏單位 101.4ms", "payment-gateway 自身耗時 101.4ms", "pass"),
    ("數字黏幻覺單位 101.4qx", "payment-gateway 自身耗時 101.4qx", "reject"),
    ("JSON 鍵名當字 status_code", "payment-gateway status_code 504", "pass"),
    ("鍵名當字 api_key（context 沒有）", "payment-gateway 的 api_key 過期", "reject"),
]


def main() -> int:
    use_root(sys.argv)
    from aiops.stages.enrichment import narrative as n
    if not hasattr(n, "unknown_identifier"):
        print("這個 root 沒有 unknown_identifier（不是 PR B）")
        return 1
    print("_IDENT =", n._IDENT.pattern)
    # 拓撲已知實體只放白名單內的兩個：只測封閉詞表（第 3、4 步），不讓第 2 步的子字串比對先擋
    known = {"cart-api", "payment-gateway"}
    bad = 0
    for desc, summary, expect in CASES:
        reason = n.validate_briefing(lines(summary), CONTEXT, known)
        got = "pass" if reason is None else "reject"
        tokens = n._IDENT.findall(summary)
        verdict = "ok" if got == expect else ("ESCAPE" if expect == "reject" else "FALSE-REJECT")
        bad += verdict != "ok"
        print(f"{verdict:12} {desc:34} tokens={tokens} -> {reason or 'pass'}")
    print(f"cases={len(CASES)} mismatches={bad}")
    # 第 2 步（實體白名單）是子字串比對：拓撲裡有個服務叫 tax-gw（不在白名單），歸因資源是 tax-gw:443（在白名單）
    for desc, known2, summary in [
            ("拓撲有 tax-gw 服務、摘要寫白名單內的 tax-gw:443", {"cart-api", "payment-gateway", "tax-gw"},
             "payment-gateway 等 tax-gw:443 變慢"),
            ("拓撲有 cart 服務、摘要寫白名單內的 cart-api", {"cart-api", "payment-gateway", "cart"},
             "cart-api 變慢")]:
        reason = n.validate_briefing(lines(summary), CONTEXT, known2)
        print(f"{'FALSE-REJECT' if reason else 'ok':12} {desc} -> {reason or 'pass'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
