"""Grafana chart 必須維持「只顯示」（CLAUDE.md 第 5 節）：不發通知、不放機密、不依賴已移除的舊引擎。需要 helm；沒裝就略過。"""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1] / "deploy" / "helm" / "aiops-grafana"
pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="沒有安裝 helm")

# 會讓 Grafana 自己「發出通知」或做告警判斷的東西
ALERTING_FILES = ("alert-rules", "contact-points", "policies", "notification-policies", "mute-timings", "templates")
# 舊引擎已移除：這些參照出現就代表搬進了壞掉的東西
DEAD_REFERENCES = re.compile(r"aiops-db|aiops_alerts|aiops_anomalies|aiops_tuning|aiops_feedback|localhost:18080|"
                             r"grafana-postgresql-datasource|aiops-api")


def _render(*sets: str) -> list[dict]:
    args = [a for s in sets for a in ("--set", s)]
    out = subprocess.run(["helm", "template", "g", str(CHART), *args],
                         capture_output=True, text=True, encoding="utf-8", check=True).stdout
    return [d for d in yaml.safe_load_all(out) if d]


def _datasources() -> list[dict]:
    return yaml.safe_load((CHART / "files" / "datasource.yaml").read_text(encoding="utf-8"))["datasources"]


def test_no_alerting_provisioning_files_or_mounts():
    """Grafana 不放告警規則、Contact Point、Policy：沒有這些檔、沒有 alerting 的 ConfigMap 與掛載。"""
    files = [p.name for p in (CHART / "files").rglob("*") if p.is_file()]
    assert not [f for f in files if any(k in f for k in ALERTING_FILES)], files
    docs = _render()
    assert not [d["metadata"]["name"] for d in docs if d["kind"] == "ConfigMap" and "alerting" in d["metadata"]["name"]]
    dep = next(d for d in docs if d["kind"] == "Deployment")
    pod = dep["spec"]["template"]["spec"]
    mounts = [m["mountPath"] for m in pod["containers"][0]["volumeMounts"]]
    assert not [m for m in mounts if "alerting" in m], mounts
    assert not [v["name"] for v in pod["volumes"] if "alerting" in v["name"]]


def test_datasources_are_read_only_and_known():
    ds = {d["uid"]: d for d in _datasources()}
    assert set(ds) == {"apm-metrics", "apm-traces", "apm-logs", "aiops-alertmanager"}
    assert {d["type"] for d in ds.values()} == {"elasticsearch", "alertmanager"}
    am = ds["aiops-alertmanager"]
    assert am["jsonData"]["handleGrafanaManagedAlerts"] is False  # 不把 Grafana 自己的告警送進去
    assert am["jsonData"]["implementation"] == "prometheus"
    for d in ds.values():
        assert d["access"] == "proxy" and not d.get("editable", False)


def test_datasource_credentials_come_from_environment_only():
    text = (CHART / "files" / "datasource.yaml").read_text(encoding="utf-8")
    for d in _datasources():
        assert re.fullmatch(r"\$\{[A-Z_]+\}", d["url"]), f"{d['uid']} 的 url 必須是環境變數：{d['url']}"
    for secret in re.findall(r"(?:httpHeaderValue1|password|basicAuthPassword):\s*(.+)", text):
        assert "${" in secret, f"疑似明文機密：{secret[:30]}"


def test_no_dead_references_to_removed_engine():
    for path in (p for p in CHART.rglob("*") if p.is_file() and p.suffix in (".yaml", ".json", ".tpl")):
        assert not DEAD_REFERENCES.search(path.read_text(encoding="utf-8")), f"{path.name} 含有已移除的舊引擎參照"


def test_dashboards_read_only_from_provisioned_datasources():
    uids = {d["uid"] for d in _datasources()}
    boards = list((CHART / "files" / "dashboards").glob("*.json"))
    assert sorted(b.stem for b in boards) == ["aiops-incidents", "apm-overview"]  # 新增事件群儀表板（只讀 aiops-alertmanager）
    for board in boards:
        d = json.loads(board.read_text(encoding="utf-8"))
        used = {(p.get("datasource") or {}).get("uid") for p in d["panels"] if p["type"] != "text"}  # 說明用的 text 面板沒有資料源
        used |= {(v.get("datasource") or {}).get("uid") for v in d.get("templating", {}).get("list", []) if v.get("datasource")}
        assert used <= uids, f"{board.name} 用了沒定義的資料源：{used - uids}"
        assert d["panels"] and all(p["type"] != "text" or "iframe" not in json.dumps(p) for p in d["panels"])


def test_grafana_does_not_loosen_html_sanitizing_or_allow_editing():
    """不得關掉 HTML 消毒（舊 chart 的 embedFeedback 會）；匿名只能是 Viewer。"""
    text = "\n".join(p.read_text(encoding="utf-8") for p in CHART.rglob("*") if p.is_file() and p.suffix in (".yaml", ".tpl"))
    assert "DISABLE_SANITIZE_HTML" not in text
    env = {e["name"]: e.get("value") for e in
           next(d for d in _render("anonymousViewer=true") if d["kind"] == "Deployment")
           ["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["GF_AUTH_ANONYMOUS_ORG_ROLE"] == "Viewer"
    assert "GF_AUTH_ANONYMOUS_ENABLED" not in {e["name"] for e in next(
        d for d in _render() if d["kind"] == "Deployment")["spec"]["template"]["spec"]["containers"][0]["env"]}


def test_values_contain_no_secrets_and_secret_only_from_dev_flag():
    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    assert values["devSecretEnv"] == {} and values["anonymousViewer"] is False
    assert not any(re.search(r"KEY|PASSWORD|TOKEN|SECRET", k) for k in values["env"])
    assert "Secret" not in [d["kind"] for d in _render()]  # 預設不建 Secret
    dev = _render("devSecretEnv.GF_SECURITY_ADMIN_PASSWORD=x")
    assert [d["kind"] for d in dev].count("Secret") == 1


def test_resource_names_match_the_running_release_so_helm_can_take_over():
    names = {(d["kind"], d["metadata"]["name"]) for d in _render()}
    assert {("Deployment", "grafana"), ("Service", "grafana"), ("ConfigMap", "grafana-datasources"),
            ("ConfigMap", "grafana-dashboards"), ("ConfigMap", "grafana-dashboard-provider")} <= names
