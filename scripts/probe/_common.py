"""探測腳本共用：--root 指定要測的 aiops 原始碼根目錄（預設 = 本 worktree，可指到 PR A 的快照）。"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def use_root(argv: list[str]) -> Path:
    root = REPO
    if "--root" in argv:
        root = Path(argv[argv.index("--root") + 1]).resolve()
    sys.path.insert(0, str(root))
    os.chdir(root)
    return root
