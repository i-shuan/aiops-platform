"""跑全部（或指定）破壞性場景並輸出報表。

用法（需要一個 PostgreSQL，連線字串用環境變數 CHAOS_PG_URL，預設 postgresql+psycopg://postgres:pgchaos@127.0.0.1:55433/chaos）：
    .venv/Scripts/python scripts/chaos/run.py            # 全部
    .venv/Scripts/python scripts/chaos/run.py 3 7 10     # 指定場景
    --out docs/CHAOS_E2E_REPORT.md                        # 報表寫到檔案
    --rca legacy|cognitive    # RCA 用哪支 workflow：legacy = rca_report（預設，輸出跟以前逐列相同）；cognitive = rca_report_cognitive
    --compare                 # 每個場景先跑 legacy 再跑 cognitive，輸出並排表（每條斷言兩欄＋根因排名、分數、Runbook）
    --llm faithful|down|slow|garbage|hallucinate|yaml
                              # cognitive 時 LLM 節點換成腳本化假 LLM 的哪個模式（預設 faithful；yaml = 不換，照 YAML 的供應商）
不帶場景編號 = 全部；但 --rca legacy（預設）時不含只對 cognitive 有意義的場景（24 LLM 全掛），legacy 全量輸出跟加這些選項之前相同。
cognitive workflow 還沒建立時：--rca cognitive 直接報錯結束（exit 2），不退回 legacy；--compare 照跑 legacy，cognitive 欄標「尚無 cognitive 引擎」。
場景只讀平台、不改平台：任何場景的「未通過」都是平台現況與預期的落差，不是被放寬後的綠燈。
"""
import os
import sys
import time

sys.path.insert(0, os.getcwd())
from scripts.chaos import harness as H  # noqa: E402
from scripts.chaos import scenarios as S  # noqa: E402
from scripts.chaos.scen_cognitive import COGNITIVE_ONLY  # noqa: E402

NL = "\n"


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


# ───────────────────────── --compare：新舊並排 ─────────────────────────
def _cell(c) -> str:
    if c is None:
        return "—"
    mark = "✓" if c.ok else ("✗（已知死區）" if c.dead else "✗")
    return f"{mark} {c.actual.replace('|', '／').replace(NL, ' ')[:140]}"


def _pair(a: list, b: list) -> list[tuple]:
    """同一場景兩次的斷言配對：用「斷言文字＋第幾次出現」對齊（cognitive 中途 return 時兩邊列數會不同）。"""
    def keyed(checks):
        seen, out = {}, {}
        for c in checks:
            k = (c.what, seen.get(c.what, 0))
            seen[c.what] = k[1] + 1
            out[k] = c
        return out
    ka, kb = keyed(a), keyed(b)
    return [(k[0], ka.get(k), kb.get(k)) for k in list(ka) + [k for k in kb if k not in ka]]


def _score(r) -> str:
    return f"{r.verdict} {sum(c.ok for c in r.checks)}／{len(r.checks)}"


def _rca_rows(log: list[dict], engine: str) -> list[str]:
    rows = []
    for e in log:
        top = "、".join(f"{i + 1}. {ent}（{score}{'／' + res if res else ''}）" for i, (ent, score, res) in enumerate(e["top"])) or "（無候選）"
        # legacy 不寫 rca_status（欄位預設值 ok 沒有意義），只有 cognitive 列狀態與原因
        status = (e["status"] + (f"：{e['reason']}" if e["reason"] else "")) if engine == "cognitive" else "—"
        ev = "、".join(f"{k}@{ent}" for k, ent in e["evidence"]) or "—"
        rows.append(f"| {engine} | {e['incident']} | {status} | {top} | {e['runbook']} | {ev} |")
    return rows


def render_compare(pairs, missing: str) -> str:
    """pairs：[(場景編號, legacy 結果列表, legacy RCA_LOG, cognitive 結果列表或 None, cognitive RCA_LOG)]"""
    lines = ["## 新舊並排總表（legacy = rca_report、cognitive = rca_report_cognitive）\n"]
    if missing:
        lines.append(f"> **cognitive 欄沒有跑**：{missing}\n")
    lines.append("| # | 場景 | legacy | cognitive |\n|---|---|---|---|")
    tot = {"l": [0, 0], "c": [0, 0]}
    diff = 0
    for _, leg, _, cog, _ in pairs:
        cmap = {r.n: r for r in (cog or [])}
        for r in leg:
            cr = cmap.get(r.n)
            lines.append(f"| {r.n} | {r.title} | {_score(r)} | {_score(cr) if cr else ('尚無 cognitive 引擎' if missing else '—')} |")
            tot["l"][0] += sum(c.ok for c in r.checks)
            tot["l"][1] += len(r.checks)
            if cr:
                diff += sum(1 for _, a, b in _pair(r.checks, cr.checks) if (a is not None and a.ok) != (b is not None and b.ok))
        for cr in (cog or []) if not leg else []:
            lines.append(f"| {cr.n} | {cr.title} | （只有 cognitive）| {_score(cr)} |")
        for cr in cog or []:
            tot["c"][0] += sum(c.ok for c in cr.checks)
            tot["c"][1] += len(cr.checks)
    lines.append(f"\n斷言合計：legacy {tot['l'][0]}／{tot['l'][1]}；cognitive {tot['c'][0]}／{tot['c'][1]}；兩邊結果不同的斷言 {diff} 條。\n")
    for n, leg, llog, cog, clog in pairs:
        cmap = {r.n: r for r in (cog or [])}
        for lr, cr in ([(r, cmap.get(r.n)) for r in leg] if leg else [(None, r) for r in cog or []]):
            r = lr or cr
            lines.append(f"## 場景 {r.n}：{r.title}　【legacy {lr.verdict if lr else '—'}｜cognitive {cr.verdict if cr else '—'}】\n")
            lines.append("| 斷言 | legacy | cognitive | 不同 |\n|---|---|---|---|")
            for what, a, b in _pair(lr.checks if lr else [], cr.checks if cr else []):
                same = "≠" if (a is not None and b is not None and a.ok != b.ok) else ""
                lines.append(f"| {what} | {_cell(a)} | {_cell(b)} | {same} |")
            for res, tag in ((lr, "legacy"), (cr, "cognitive")):
                if res and res.crash:
                    lines.append(f"\n**{tag} 場景崩潰**：\n```\n{res.crash}\n```")
                for note in (res.notes if res and res.blocked else []):
                    lines.append(f"\n> {tag}：{note}")
            lines.append("")
        lines.append(f"**場景 {n} 的 RCA 關鍵數字**（每次 rca() 一列，子場景依序）\n")
        lines.append("| 引擎 | 事件群 | 狀態 | 根因排名（分數／資源） | Runbook | LLM 證據 |\n|---|---|---|---|---|---|")
        lines += _rca_rows(llog, "legacy") + _rca_rows(clog, "cognitive")
        lines.append("")
    return "\n".join(lines)


def _opt(args: list[str], name: str, default):
    if name not in args:
        return default, args
    i = args.index(name)
    return args[i + 1], args[:i] + args[i + 2:]


def compare(numbers: list[int], llm: str, missing: str) -> list[tuple]:
    pairs = []
    for n in numbers or list(S.SCENARIOS):
        leg, llog = [], []
        if n not in COGNITIVE_ONLY:
            H.use_rca("legacy")
            H.RCA_LOG.clear()
            leg, llog = S.run([n]), list(H.RCA_LOG)
        cog, clog = None, []
        if not missing or n in COGNITIVE_ONLY:   # cognitive 不存在時 24 自己標「尚無 cognitive 引擎」，其他場景不跑
            H.use_rca("cognitive", llm)
            H.RCA_LOG.clear()
            cog, clog = S.run([n]), list(H.RCA_LOG)
        pairs.append((n, leg, llog, cog, clog))
    H.use_rca("legacy")
    return pairs


if __name__ == "__main__":
    args = sys.argv[1:]
    out, args = _opt(args, "--out", None)
    mode, args = _opt(args, "--rca", "legacy")
    llm, args = _opt(args, "--llm", "faithful")
    do_compare = "--compare" in args
    args = [a for a in args if a != "--compare"]
    if mode not in H.RCA_WORKFLOWS:
        sys.exit(f"--rca 只能是 {'／'.join(H.RCA_WORKFLOWS)}，收到 {mode!r}")
    numbers = [int(a) for a in args]
    missing = H.cognitive_missing()
    if mode == "cognitive" and not do_compare and missing:
        print(f"錯誤：--rca cognitive 無法執行。{missing}。不會退回 legacy。", file=sys.stderr)
        sys.exit(2)
    t = time.time()
    if do_compare:
        text = render_compare(compare(numbers, llm, missing), missing)
    else:
        H.use_rca(mode, llm)
        if not numbers and mode == "legacy":
            numbers = [n for n in S.SCENARIOS if n not in COGNITIVE_ONLY]
        text = render(S.run(numbers or None))
    print(text)
    print(f"\n（耗時 {time.time() - t:.0f} 秒）")
    if out:
        with open(out, "w", encoding="utf-8") as f:
            f.write(text)
