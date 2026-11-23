"""架構不變量審查（docs/ARCH_INVARIANTS_REVIEW.md 第 1 題）的探測測試：stages/rca 在封鎖 storage／sources／interfaces／
control_plane／sqlalchemy／fastapi／httpx 的乾淨子行程裡，用純記憶體物件跑完 rca.root_cause.evidence 與 rca.report 全部段落。

這支測試記錄的是「現況」：唯一越界是 rca.section.trace_evidence 經 stages/notification/links 讀 AIOPS_GRAFANA_URL。
哪天那條依賴被拿掉，第二個斷言會紅——那是好消息，請連同報告一起更新。
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_rca_core_runs_with_storage_sources_and_api_blocked():
    r = subprocess.run([sys.executable, str(ROOT / "scripts/probe/q1_rca_pure_memory.py")], capture_output=True,
                       text=True, encoding="utf-8", env={"PYTHONIOENCODING": "utf-8", "SYSTEMROOT": "C:\\Windows",
                                                          "PATH": ""}, cwd=ROOT)
    out = r.stdout
    assert r.returncode == 0, out + r.stderr
    assert "RESULT: PASS" in out
    assert "blocked import attempts: 無" in out
    assert "sections skipped: []" in out  # 10 個段落全部產出
    assert "candidates: [('cart-api'" in out  # evidence 指認出手刻劇本裡變慢的 cart-api
    # 現況的唯一越界：trace_evidence 段落 -> stages.notification.links -> os.environ["AIOPS_GRAFANA_URL"]
    assert "non-engine/non-rca: ['aiops.stages', 'aiops.stages.notification', 'aiops.stages.notification.links']" in out
    assert "AIOPS_* reads during run: ['AIOPS_GRAFANA_URL']" in out
