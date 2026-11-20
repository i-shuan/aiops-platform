"""CLAUDE.md 第 3 節的範例必須是真的、跑得起來的程式碼（以前的範例用了不存在的欄位，照抄會直接失敗）。

- 範例一必須跟 aiops/stages/detectors/residual_shift.py 逐字相同：改了偵測器沒改文件、或改了文件沒改程式，這裡就會紅。
- 範例二在乾淨的子行程裡執行：它會往 LLM 供應商註冊表登記 echo，放在子行程才不會污染這個測試行程的註冊表。
"""
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _section3_blocks() -> list[str]:
    text = (ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    section = text.split("## 3. ", 1)[1].split("\n## 4. ", 1)[0]
    return re.findall(r"```python\n(.*?)```", section, flags=re.S)


def test_section3_has_exactly_the_two_examples():
    assert len(_section3_blocks()) == 2


def test_example_one_is_the_real_residual_shift_detector_verbatim():
    real = (ROOT / "aiops" / "stages" / "detectors" / "residual_shift.py").read_text(encoding="utf-8")
    assert _section3_blocks()[0].strip() == real.strip()


def test_example_two_runs_in_a_clean_process():
    code = "from aiops.interfaces.runner import load_all\nload_all()\n" + _section3_blocks()[1] + "\nprint('EXAMPLE_OK')\n"
    r = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    assert r.returncode == 0 and "EXAMPLE_OK" in r.stdout, r.stderr[-2000:]
