"""跑全部（或指定）破壞性場景並輸出報表。

用法（需要一個 PostgreSQL，連線字串用環境變數 CHAOS_PG_URL，預設 postgresql+psycopg://postgres:pgchaos@127.0.0.1:55433/chaos）：
    .venv/Scripts/python scripts/chaos/run.py            # 全部
    .venv/Scripts/python scripts/chaos/run.py 3 7 10     # 指定場景
    --out docs/CHAOS_E2E_REPORT.md                        # 報表寫到檔案
場景只讀平台、不改平台：任何場景的「未通過」都是平台現況與預期的落差，不是被放寬後的綠燈。
"""
import os
import sys
import time

sys.path.insert(0, os.getcwd())
from scripts.chaos import scenarios as S  # noqa: E402


def render(results) -> str:
    lines = []
    ok_n = sum(r.verdict == "通過" for r in results)
    dead_n = sum(r.verdict == "已知死區" for r in results)
    lines.append(f"## 總表\n\n| # | 場景 | 結果 | 通過／斷言 |\n|---|---|---|---|")
    for r in results:
        n_ok = sum(c.ok for c in r.checks)
        lines.append(f"| {r.n} | {r.title} | {r.verdict} | {n_ok}／{len(r.checks)} |")
    lines.append(f"\n{len(results)} 個場景中完全通過 {ok_n} 個" + (f"、已知死區 {dead_n} 個" if dead_n else "") + f"；斷言合計 {sum(c.ok for r in results for c in r.checks)}／{sum(len(r.checks) for r in results)}。\n")
    for r in results:
        lines.append(f"## 場景 {r.n}：{r.title}　【{r.verdict}】\n")
        lines.append(f"**注入特徵**：{r.injected}\n")
        lines.append("| 斷言 | 預期 | 實測 | |\n|---|---|---|---|")
        for c in r.checks:
            lines.append(f"| {c.what} | {c.expected} | {c.actual.replace('|', '／').replace(chr(10), ' ')[:300]} | {'✓' if c.ok else ('✗（已知死區）' if c.dead else '✗')} |")
        for n in r.notes:
            lines.append(f"\n> {n.replace(chr(10), ' ')[:900]}")
        if r.crash:
            lines.append(f"\n**場景崩潰**：\n```\n{r.crash}\n```")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    args = sys.argv[1:]
    out = None
    if "--out" in args:
        i = args.index("--out")
        out = args[i + 1]
        args = args[:i] + args[i + 2:]
    t = time.time()
    results = S.run([int(a) for a in args] or None)
    text = render(results)
    print(text)
    print(f"\n（耗時 {time.time() - t:.0f} 秒）")
    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(text)
