"""偵測管線樣本（scripts/pipelines/sample_*.yaml）讓測試與回測、混沌腳本可以用名稱載入。

使用者決定（2026-11-15）：平台的偵測演算法只用來監控 AIOps 平台自己，不給使用者用。所以出貨的使用者管線
（aiops/pipelines/detection/）只吃各團隊推進來的外部告警、不跑偵測器；原本那四條帶偵測器的版本搬到這裡改名 sample_*，
給偵測機制的端到端測試、回測（scripts/backtest/）與混沌場景（scripts/chaos/）用。這裡不在 aiops/、不進映像檔。

做法：把 aiops/pipelines/ 與這些樣本一起複製到一個暫存目錄，再把 aiops.interfaces.runner.WORKFLOW_DIR 指過去——
生產程式不用知道樣本的存在，workflow_path、rca_workflow_for、API 都照常用名稱找得到 sample_*。
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

SAMPLE_DIR = Path(__file__).resolve().parent / "pipelines"


def merged_workflow_dir(dest: Path) -> Path:
    """dest 底下建 pipelines/：出貨的管線原樣，加上 samples/ 子資料夾放樣本。回傳那個 pipelines/ 目錄。"""
    from aiops.interfaces import runner

    root = dest / "pipelines"
    shutil.copytree(runner.WORKFLOW_DIR, root)
    shutil.copytree(SAMPLE_DIR, root / "samples")
    return root


def use_sample_pipelines() -> Path:
    """腳本用：建好合併目錄並把 runner.WORKFLOW_DIR 指過去（這個行程結束為止）。回傳合併目錄。"""
    from aiops.interfaces import runner

    if runner.WORKFLOW_DIR.parent.name.startswith("aiops-sample-pipelines-"):
        return runner.WORKFLOW_DIR  # 已經切過了
    root = merged_workflow_dir(Path(tempfile.mkdtemp(prefix="aiops-sample-pipelines-")))
    runner.WORKFLOW_DIR = root
    return root
