"""排程入口。

  python -m aiops.tasks run-once -c configs/jobs.yaml [--job NAME]   # 跑一輪就結束（適合 K8s CronJob）
  python -m aiops.tasks loop -c configs/jobs.yaml --interval 60      # 常駐，每隔 N 秒跑一輪
  python -m aiops.tasks source-check                                 # 資料源連線自我檢查（有失敗就非 0）

一個 job 失敗不影響其他 job：錯誤走看守機制（本機日誌、AIOPS_FALLBACK_WEBHOOK），最後以非 0 結束碼回報。
注意：loop 模式只適合單一副本；多副本要改用 CronJob 或加上分散式鎖，否則同一輪會被跑多次。
"""
import argparse
import json
import sys
import time

from aiops.commands.base import CommandError
from aiops.domains.ingestion.commands.sources import CheckSourcesCommand
from aiops.infrastructure.errors import InfrastructureError
from aiops.infrastructure.fallback import report_failure
from aiops.initialization import init_infrastructure, init_plugins
from aiops.tasks.config import Job, load_jobs
from aiops.tasks.pipeline import run_job


def run_all(jobs: list[Job]) -> int:
    failures = 0
    for job in jobs:
        try:
            print(json.dumps(run_job(job), ensure_ascii=False), flush=True)
        except (CommandError, InfrastructureError) as e:
            failures += 1
            report_failure(e, {"job": job.name})
            print(json.dumps({"job": job.name, "error": str(e)}, ensure_ascii=False), file=sys.stderr, flush=True)
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m aiops.tasks")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("run-once", "loop"):
        p = sub.add_parser(name)
        p.add_argument("-c", "--config", required=True, help="jobs YAML 檔")
        p.add_argument("--job", help="只跑這個 job")
        if name == "loop":
            p.add_argument("--interval", type=int, default=60, help="每輪間隔秒數")
            p.add_argument("--max-runs", type=int, default=None, help="跑幾輪就停（測試用）")
    sub.add_parser("source-check")
    args = parser.parse_args(argv)

    if args.cmd == "source-check":
        result = CheckSourcesCommand().run()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["ok"] else 1

    init_infrastructure()
    init_plugins()
    jobs = [j for j in load_jobs(args.config) if args.job in (None, j.name)]
    if not jobs:
        print(f"找不到 job：{args.job}", file=sys.stderr)
        return 2

    if args.cmd == "run-once":
        return 1 if run_all(jobs) else 0

    runs = 0
    while args.max_runs is None or runs < args.max_runs:
        run_all(jobs)
        runs += 1
        if args.max_runs is None or runs < args.max_runs:
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
