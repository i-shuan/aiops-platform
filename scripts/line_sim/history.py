"""產線指標模擬的歷史資料：過去 N 天、每分鐘一點的 OpenMetrics 檔（沒有故障），回補進 Prometheus 當訓練期。

  python history.py --days 14 --end <epoch 秒> > hist.om
  promtool tsdb create-blocks-from openmetrics hist.om /prometheus

end 預設是現在；給定值可以重現同一份檔案（每一分鐘的值只由時間決定）。
"""
from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import model  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--end", type=int, default=int(time.time()))
    args = ap.parse_args()
    end = args.end - args.end % 60
    start = end - args.days * 86400
    out = sys.stdout
    for name, labels, help_text in model.SERIES:  # 同一個指標的樣本要連在一起（OpenMetrics 的規定）
        label_text = ",".join(f'{k}="{v}"' for k, v in labels.items())
        out.write(f"# HELP {name} {help_text}\n# TYPE {name} gauge\n")
        for t in range(start, end, 60):
            out.write(f"{name}{{{label_text}}} {model.normal(name, model.utc(t))} {t}\n")
    out.write("# EOF\n")


if __name__ == "__main__":
    main()
