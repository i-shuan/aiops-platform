"""機密不得進 repo（CLAUDE.md 第 5 節與 ES 規則）：掃 deploy/、configs/、docker-compose。不需要 helm。"""
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCAN = [p for base in ("deploy", "configs") for p in (ROOT / base).rglob("*") if p.is_file()] + [ROOT / "docker-compose.yaml"]

# 長的 base64／十六進位字串、常見金鑰前綴、Slack／OpenAI 樣式
KEY_LIKE = re.compile(r"sk-(?:proj-|ant-)?[A-Za-z0-9_-]{20,}|xox[bp]-[A-Za-z0-9-]{10,}|"
                      r"hooks\.slack\.com/services/T[A-Z0-9]{8,}/B[A-Z0-9]{8,}/[A-Za-z0-9]{16,}|"
                      r"(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{48,}={0,2}(?![A-Za-z0-9+/])")
# 這些檔是整份讀進測試的樣板或 JSON 儀表板，不是機密（儀表板裡的長 base64 不在範圍內）
SKIP_SUFFIX = {".json"}


def test_no_key_like_strings_in_deploy_and_configs():
    bad = []
    for p in SCAN:
        if p.suffix in SKIP_SUFFIX or not p.exists():
            continue
        for n, line in enumerate(p.read_text(encoding="utf-8", errors="ignore").splitlines(), 1):
            if KEY_LIKE.search(line):
                bad.append(f"{p.relative_to(ROOT).as_posix()}:{n}")
    assert not bad, bad


def test_snapshots_contain_no_secret_resources_or_plain_passwords():
    """ES 快照：不得有 Secret 資源；環境變數若跟密碼有關，只能是 valueFrom 引用。"""
    for snap in (ROOT / "deploy" / "elasticsearch").glob("*.yaml"):
        for doc in (d for d in yaml.safe_load_all(snap.read_text(encoding="utf-8")) if d):
            assert doc["kind"] != "Secret", f"{snap.name} 含有 Secret 資源"
            spec = doc.get("spec", {}).get("template", {}).get("spec", {})
            for c in spec.get("containers", []) + spec.get("initContainers", []):
                for env in c.get("env", []) or []:
                    if re.search(r"PASSWORD|TOKEN|API_?KEY|SECRET", env["name"], re.I):
                        assert "valueFrom" in env and "value" not in env, f"{snap.name}:{env['name']} 是明文"


def test_compose_credentials_are_labeled_dev_only():
    """docker-compose 只能有明確標成開發用的假值。"""
    text = (ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
    assert "dev-only" in text and "只給開發用" in text
    for secret in re.findall(r"(?:PASSWORD|TOKEN)[:=]\s*(\S+)", text):
        assert "dev-only" in secret or secret.startswith("${"), secret


def test_values_files_do_not_default_real_secrets():
    for values in (ROOT / "deploy" / "helm").glob("*/values.yaml"):
        data = yaml.safe_load(values.read_text(encoding="utf-8"))
        for key in ("devSecretEnv", "secretData"):
            assert not data.get(key), f"{values.parent.name} 的 {key} 預設必須是空的"
