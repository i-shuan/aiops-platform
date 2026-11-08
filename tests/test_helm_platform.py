"""平台 helm chart：機密只能從 Secret 進來、排程不重疊、容器以非 root 唯讀執行。需要 helm；沒裝就略過。"""
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1] / "deploy" / "helm" / "aiops-platform"
pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="沒有安裝 helm")


def _render(*sets: str) -> dict[str, list[dict]]:
    args = [a for s in sets for a in ("--set", s)]
    out = subprocess.run(["helm", "template", "p", str(CHART), *args],
                         capture_output=True, text=True, encoding="utf-8", check=True).stdout
    docs: dict[str, list[dict]] = {}
    for d in yaml.safe_load_all(out):
        if d:
            docs.setdefault(d["kind"], []).append(d)
    return docs


def _containers(docs):
    api = docs["Deployment"][0]["spec"]["template"]["spec"]["containers"][0]
    tasks = docs["CronJob"][0]["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
    return api, tasks


def test_refuses_to_render_without_a_secret():
    r = subprocess.run(["helm", "template", "p", str(CHART)], capture_output=True, text=True, encoding="utf-8")
    assert r.returncode != 0 and "existingSecret" in r.stderr


def test_production_uses_existing_secret_and_creates_none():
    docs = _render("existingSecret=aiops-env")
    assert "Secret" not in docs
    for c in _containers(docs):
        assert c["envFrom"] == [{"secretRef": {"name": "aiops-env"}}]


def test_default_values_contain_no_secrets():
    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    assert values["existingSecret"] == "" and values["devSecretEnv"] == {}
    assert not any("TOKEN" in k or "KEY" in k or "PASSWORD" in k for k in values["env"])


def test_containers_run_as_non_root_read_only():
    for c in _containers(_render("existingSecret=s")):
        sc = c["securityContext"]
        assert sc["runAsNonRoot"] and sc["readOnlyRootFilesystem"] and sc["capabilities"]["drop"] == ["ALL"]


def test_cronjob_runs_once_per_tick_without_overlap():
    cron = _render("existingSecret=s")["CronJob"][0]
    assert cron["spec"]["concurrencyPolicy"] == "Forbid"
    _, tasks = _containers(_render("existingSecret=s"))
    assert tasks["args"] == ["python", "-m", "aiops.tasks", "run-once", "-w", "metrics_only"]


def test_values_dev_renders_a_dev_secret_and_the_es_url():
    out = subprocess.run(["helm", "template", "p", str(CHART), "-f", str(CHART / "values-dev.yaml")],
                         capture_output=True, text=True, encoding="utf-8", check=True).stdout
    docs = [d for d in yaml.safe_load_all(out) if d]
    secret = next(d for d in docs if d["kind"] == "Secret")
    assert secret["metadata"]["annotations"]["aiops/warning"] and "AIOPS_API_TOKEN" in secret["stringData"]
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    env = {e["name"]: e.get("value") for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["AIOPS_ES_URL"] == "http://elasticsearch.es-test.svc:9200"


def test_dev_postgres_wires_database_url_automatically():
    docs = _render("devSecretEnv.AIOPS_API_TOKEN=x", "devPostgres.enabled=true")
    for c in _containers(docs):
        url = next(e["value"] for e in c["env"] if e["name"] == "AIOPS_DATABASE_URL")
        assert url.startswith("postgresql+psycopg://") and "p-dev-postgres:5432" in url
    assert any(d["metadata"]["name"] == "p-dev-postgres" for d in docs["Deployment"])
