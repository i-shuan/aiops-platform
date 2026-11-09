"""Q1：stages/rca、stages/detectors、stages/enrichment 每個模組在「封鎖 storage／sources／interfaces／control_plane／
sqlalchemy／fastapi／httpx」下能不能單獨 import，以及它實際拉進哪些 aiops 模組、原始碼裡有沒有讀 os.environ。

每個模組各開一個乾淨的子行程（避免前一個 import 的快取污染結果）。
用法：python scripts/probe/q1_import_matrix.py [--root <aiops 原始碼根>]
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402

CHILD = r'''
import importlib.abc, json, sys
sys.path.insert(0, ROOT)
BLOCKED = ("aiops.storage", "aiops.sources", "aiops.interfaces", "aiops.control_plane",
           "sqlalchemy", "fastapi", "starlette", "httpx", "psycopg", "elasticsearch", "requests")
hits = []
class B(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if any(name == b or name.startswith(b + ".") for b in BLOCKED):
            hits.append(name); raise ImportError("blocked " + name)
sys.meta_path.insert(0, B())
err = ""
try:
    __import__(MOD)
except Exception as e:
    err = f"{type(e).__name__}: {e}"
out = sorted(m for m in sys.modules if m.startswith("aiops.") and m != MOD)
print(json.dumps({"err": err, "hits": sorted(set(hits)), "aiops": out}))
'''


def main() -> int:
    root = use_root(sys.argv)
    mods = []
    for pkg in ("aiops/stages/rca", "aiops/stages/detectors", "aiops/stages/enrichment"):
        for p in sorted((root / pkg).rglob("*.py")):
            if p.name == "__init__.py":
                continue
            mods.append((p, ".".join(p.relative_to(root).with_suffix("").parts)))
    env_re = re.compile(r"os\.environ|getenv")
    bad = 0
    for path, mod in mods:
        code = CHILD.replace("ROOT", repr(str(root))).replace("MOD", repr(mod))
        r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, encoding="utf-8")
        try:
            d = json.loads(r.stdout.strip().splitlines()[-1])
        except Exception:
            d = {"err": r.stderr.strip()[-200:], "hits": [], "aiops": []}
        foreign = [m for m in d["aiops"] if not m.startswith(("aiops.engine", "aiops.stages.rca", "aiops.stages.detectors",
                                                              "aiops.stages.enrichment")) and m != "aiops.stages"]
        env = bool(env_re.search(path.read_text(encoding="utf-8")))
        status = "OK" if not d["err"] else "BROKEN"
        bad += status != "OK"
        print(f"{status:6} {mod:48} env={'Y' if env else '-'} cross={foreign or '-'} "
              f"{('blocked=' + ','.join(d['hits'])) if d['hits'] else ''} {d['err'][:80]}")
    print(f"modules={len(mods)} broken_under_block={bad}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
