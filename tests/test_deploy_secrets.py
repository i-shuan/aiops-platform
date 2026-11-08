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
# 唯一允許放開發用金鑰的檔案。它自己有另一組檢查（見下面），不走上面的長字串掃描
DEV_VALUES = ROOT / "deploy" / "helm" / "aiops-platform" / "values-dev.yaml"
DEV_ALLOWED_SECRET_KEYS = {"AIOPS_API_TOKEN", "AIOPS_ES_API_KEY"}
DEV_ALLOWED_TOP_LEVEL = {"env", "devSecretEnv"}


def test_no_key_like_strings_in_deploy_and_configs():
    bad = []
    for p in SCAN:
        if p.suffix in SKIP_SUFFIX or not p.exists() or p == DEV_VALUES:
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


def test_dev_values_is_the_only_place_with_keys_and_is_labeled_dev_only():
    """values-dev.yaml 可以放開發用金鑰，但只限：標明 dev-only、只有 env 與 devSecretEnv、金鑰名單固定。
    其他 values 檔照舊不准有 devSecretEnv；有人想把別的金鑰（OpenAI、Slack…）塞進來會在這裡被擋下。"""
    text = DEV_VALUES.read_text(encoding="utf-8")
    assert "dev-only" in text.splitlines()[0] and "正式環境不要用" in text
    data = yaml.safe_load(text)
    assert set(data) <= DEV_ALLOWED_TOP_LEVEL, set(data)
    assert set(data["devSecretEnv"]) <= DEV_ALLOWED_SECRET_KEYS, set(data["devSecretEnv"])
    assert not re.search(r"sk-|xox[bp]-|hooks\.slack\.com", text), "values-dev.yaml 不得放 OpenAI／Slack 金鑰"
    es_key = data["devSecretEnv"].get("AIOPS_ES_API_KEY")
    assert es_key is None or (es_key and "<" not in es_key), "AIOPS_ES_API_KEY 要嘛不設，要嘛填真的值"
    others = [p for p in (ROOT / "deploy" / "helm").rglob("values*.yaml") if p != DEV_VALUES and p.name != "values-local.yaml"]
    for p in others:
        assert not (yaml.safe_load(p.read_text(encoding="utf-8")) or {}).get("devSecretEnv"), p.name


def test_values_files_do_not_default_real_secrets():
    for values in (ROOT / "deploy" / "helm").glob("*/values.yaml"):
        data = yaml.safe_load(values.read_text(encoding="utf-8"))
        for key in ("devSecretEnv", "secretData"):
            assert not data.get(key), f"{values.parent.name} 的 {key} 預設必須是空的"
