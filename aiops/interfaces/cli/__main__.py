"""排程入口：只負責呼叫 Pipeline，不做任何運算。

  python -m aiops.interfaces.cli run-once -w it_otel                  跑一輪就結束（適合 K8s CronJob）
  python -m aiops.interfaces.cli run-once -w ./my-pipeline.yaml       用自己的 workflow 檔
  python -m aiops.interfaces.cli loop -w it_otel --interval 60        常駐，每隔 N 秒跑一輪（只適合單一副本）
  python -m aiops.interfaces.cli source-check                         資料源連線自我檢查（有失敗就非 0）

-w 給名字就是內建 workflow（aiops/pipelines/ 任一子資料夾的 <名字>.yaml），給路徑就是自己的檔。
跑一輪的流程：載入輸入 -> 抓資料 -> 偵測 -> 心跳／結案 -> 收斂 -> 計分 -> 抑制 -> 通知 -> 寫入最終結果。
某條序列失敗不影響其他序列：記在輸出的 errors，並以非 0 結束碼回報；基礎設施故障（DB、資料源、Alertmanager）整輪失敗。
loop 模式只適合單一副本；多副本要改用 K8s CronJob 或加分散式鎖，否則同一輪會被跑多次。
"""
import argparse
import json
import sys
import time
from datetime import datetime

from aiops.engine.errors import PlatformError
from aiops.interfaces.bootstrap.fallback import report_failure
from aiops.interfaces.bootstrap import init_infrastructure
from aiops.interfaces.runner import run, run_scheduled


def run_once(workflow: str, now: datetime | None) -> int:
    try:
        ctx = run_scheduled(workflow, now)  # CLI 可以指定自己的 YAML 檔；定義載入失敗會留一筆健康度紀錄
    except PlatformError as e:
        report_failure(e, {"workflow": workflow})
        print(json.dumps({"workflow": workflow, "error": str(e)}, ensure_ascii=False), file=sys.stderr, flush=True)
        return 1
    result = ctx.to_result()
    summary = {"workflow": workflow, "anomalies": result.get("anomalies", 0),
               "incidents": len(result.get("incidents", [])),
               "notified_incidents": sum(1 for i in result.get("incidents", []) if not i["suppressed_by"]),
               "suppressed_incidents": sum(1 for i in result.get("incidents", []) if i["suppressed_by"]),
               "notifications": result.get("notifications", 0),
               "resolved_incidents": result.get("resolved_incidents", 0), "errors": result.get("errors", [])}
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 1 if ctx.errors else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m aiops.interfaces.cli")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("run-once", "loop"):
        p = sub.add_parser(name)
        p.add_argument("-w", "--workflow", required=True, help="內建 workflow 名字或自己的 YAML 路徑")
        p.add_argument("--now", help="回放用：ISO 時間，不給就用現在")
        if name == "loop":
            p.add_argument("--interval", type=int, default=60, help="每輪間隔秒數")
            p.add_argument("--max-runs", type=int, default=None, help="跑幾輪就停（測試用）")
    sub.add_parser("source-check")
    args = parser.parse_args(argv)

    if args.cmd == "source-check":
        result = run("admin_sources_check")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["ok"] else 1

    init_infrastructure()
    now = datetime.fromisoformat(args.now) if args.now else None
    if args.cmd == "run-once":
        return run_once(args.workflow, now)

    runs = 0
    while args.max_runs is None or runs < args.max_runs:
        run_once(args.workflow, now)
        runs += 1
        if args.max_runs is None or runs < args.max_runs:
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
