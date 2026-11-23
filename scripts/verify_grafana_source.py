"""對真的 Grafana 驗證 aiops/sources/grafana.py（只讀）：check() -> 撈一小段 span 與錯誤日誌 -> 印第一個原始回應與解析後的筆數。

    .venv/Scripts/python scripts/verify_grafana_source.py [--minutes 15] [--end 2026-11-20T08:00:00Z] [--max 200] [--service cart-api]

環境變數（跟平台用的同一組，見 aiops/sources/grafana.py 檔頭）：
  AIOPS_GRAFANA_URL、AIOPS_GRAFANA_TOKEN（或 AIOPS_GRAFANA_USER＋AIOPS_GRAFANA_PASSWORD）、
  AIOPS_GRAFANA_TRACES_UID、AIOPS_GRAFANA_LOGS_UID；選填 AIOPS_ES_DATA_MODEL、AIOPS_ES_TRACE_INDEX、AIOPS_ES_LOG_INDEX。
只送 GET /api/health、GET /api/datasources/uid/<uid>、POST /api/datasources/proxy/uid/<uid>/_msearch。
不印 token／密碼（只印請求的方法與路徑）；原始回應只印前 --raw-chars 個字元，裡面是 ES 文件的內容，貼出去之前自己看一眼。
結束碼：0 = check 通過且 span、日誌兩段都沒有丟例外（撈到 0 筆不算失敗，會寫出來）；1 = 其他。
"""
from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from aiops.engine.errors import PlatformError  # noqa: E402
from aiops.sources.es_models import current_model  # noqa: E402
from aiops.sources.grafana import UID_ENVS, GrafanaSpec  # noqa: E402


def main() -> int:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--minutes", type=int, default=15, help="時窗長度（預設 15 分鐘）")
    ap.add_argument("--end", help="時窗結束（ISO 8601，預設現在）")
    ap.add_argument("--max", type=int, default=200, help="span／日誌各撈幾筆為上限（超過會走抽樣，順便驗 random_score）")
    ap.add_argument("--service", action="append", help="只撈這些服務（可重複；預設全部）")
    ap.add_argument("--raw-chars", type=int, default=1500)
    args = ap.parse_args()

    if not GrafanaSpec.configured():
        print("沒有設定：需要 AIOPS_GRAFANA_URL 與 AIOPS_GRAFANA_TOKEN（或 AIOPS_GRAFANA_USER＋AIOPS_GRAFANA_PASSWORD）")
        return 1
    end = datetime.fromisoformat(args.end.replace("Z", "+00:00")) if args.end else datetime.now(timezone.utc)
    start = end - timedelta(minutes=args.minutes)

    exchanges: list[tuple[httpx.Request, httpx.Response]] = []
    client = httpx.Client(timeout=30.0, event_hooks={"response": [lambda r: (r.read(), exchanges.append((r.request, r)))]})
    spec = GrafanaSpec.from_env()
    spec.client = client
    model = current_model()
    print(f"Grafana：{spec.base_url}　資料模型：{model.name}　時窗：{start.isoformat()} ~ {end.isoformat()}")
    print(f"資料源 UID：{ {signal: spec.uids.get(signal) for signal in UID_ENVS} }")

    print("\n== check() ==")
    check = spec.check()
    print(json.dumps(check, ensure_ascii=False, indent=1))
    ok = bool(check["ok"])

    for title, fetch in (("fetch_spans", lambda: spec.fetch_spans(start, end, args.service, args.max)),
                         ("fetch_logs", lambda: spec.fetch_logs(start, end, args.service, args.max))):
        print(f"\n== {title}（上限 {args.max}）==")
        before = len(exchanges)
        try:
            rows, truncated = fetch()
        except PlatformError as e:
            ok = False
            print(f"失敗：{type(e).__name__}: {e}")
            rows, truncated = None, None
        mine = exchanges[before:]
        for req, resp in mine:
            header = req.content.decode("utf-8", "replace").splitlines()[0] if req.content else ""
            print(f"  {req.method} {req.url.path} -> HTTP {resp.status_code}　{header}")
        if mine:
            req, resp = mine[0]
            print("  第一個請求的查詢 body：", req.content.decode("utf-8", "replace").splitlines()[-1][:args.raw_chars])
            print("  第一個原始回應：", resp.text[:args.raw_chars])
        if rows is None:
            continue
        print(f"  解析後：{len(rows)} 筆，truncated={truncated}，請求 {len(mine)} 次")
        if not rows:
            print("  0 筆：時窗內沒有資料、索引樣式不對、或 Grafana 資料源的 ES 帳號沒有權限（看上面原始回應的 _shards.total）")
            continue
        services: dict[str, int] = {}
        for r in rows:
            services[r["service"]] = services.get(r["service"], 0) + 1
        print("  各服務筆數：", dict(sorted(services.items(), key=lambda kv: -kv[1])[:10]))
        print("  第一筆：", json.dumps(rows[0], ensure_ascii=False, default=str)[:args.raw_chars])
        if title == "fetch_spans":
            ids = {r["span_id"] for r in rows}
            linked = sum(1 for r in rows if r.get("parent_id") in ids)
            print(f"  父 span 也在樣本裡的比例：{linked}/{len(rows)}（抽樣以 trace 為單位，應該明顯大於 0）")
    print("\n結果：", "通過（請對照上面的原始回應確認解析沒有漏欄位）" if ok else "未通過")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
