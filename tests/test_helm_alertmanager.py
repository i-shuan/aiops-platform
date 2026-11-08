"""Alertmanager chart 必須維持「純路由」（CLAUDE.md 第 5 節）。需要 helm；沒裝就略過。"""
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[1] / "deploy" / "helm" / "aiops-alertmanager"
# 接收端只准依這些 label 路由
ROUTABLE_LABELS = {"dept", "severity"}

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="沒有安裝 helm")


def _render(*args: str) -> dict:
    out = subprocess.run(
        ["helm", "template", "am", str(CHART), "--show-only", "templates/config-secret.yaml", *args],
        capture_output=True, text=True, encoding="utf-8", check=True,
    ).stdout
    return yaml.safe_load(yaml.safe_load(out)["stringData"]["alertmanager.yml"])


ROUTED = ["--set", "deptRoutes[0].dept=payments,deptRoutes[0].receiver=pay",
          "--set", "receivers[0].name=unrouted,receivers[1].name=pay"]


def test_default_config_is_pure_routing():
    cfg = _render()
    assert "inhibit_rules" not in cfg
    assert cfg["route"]["receiver"] == "unrouted"


def test_dept_route_matches_only_routable_labels():
    cfg = _render(*ROUTED)
    routes = cfg["route"]["routes"]
    assert routes == [{"receiver": "pay", "matchers": ['dept = "payments"']}]
    for route in routes:
        for m in route["matchers"]:
            assert re.match(r"\s*(\w+)", m).group(1) in ROUTABLE_LABELS, m


def test_groups_by_globally_unique_incident_uid():
    assert _render()["route"]["group_by"] == ["incident_uid", "dept"]


def test_resolve_timeout_is_a_safety_net_longer_than_heartbeat():
    """平台每輪心跳重送、安靜就送 endsAt；這個時限只在平台停擺時才用得到，要比排程間隔長很多。"""
    assert _render()["global"]["resolve_timeout"] == "1h"


def test_channel_example_renders_and_routes_only_by_dept():
    cfg = _render("-f", str(CHART / "values-channels.example.yaml"))
    assert "inhibit_rules" not in cfg
    names = {r["name"] for r in cfg["receivers"]}
    assert {r["receiver"] for r in cfg["route"]["routes"]} <= names
    assert all(re.match(r"\s*dept\b", m) for r in cfg["route"]["routes"] for m in r["matchers"])
    channels = [c for r in cfg["receivers"] for key in ("slack_configs", "msteamsv2_configs") for c in r.get(key, [])]
    assert channels and all(c["send_resolved"] is True for c in channels)
    # 範例檔不能放真的 webhook
    assert all("REPLACE_ME" in (c.get("api_url") or c.get("webhook_url")) for c in channels)


def test_persistence_uses_pvc_and_recreate_strategy():
    out = subprocess.run(["helm", "template", "am", str(CHART), "--set", "persistence.enabled=true"],
                         capture_output=True, text=True, encoding="utf-8", check=True).stdout
    docs = {d["kind"]: d for d in yaml.safe_load_all(out) if d}
    assert docs["PersistentVolumeClaim"]["spec"]["resources"]["requests"]["storage"] == "1Gi"
    assert docs["Deployment"]["spec"]["strategy"]["type"] == "Recreate"


def test_undefined_receiver_fails_render():
    r = subprocess.run(
        ["helm", "template", "am", str(CHART), "--set", "deptRoutes[0].dept=x,deptRoutes[0].receiver=nope"],
        capture_output=True, text=True, encoding="utf-8",
    )
    assert r.returncode != 0 and "nope" in r.stderr


def test_chart_templates_have_no_inhibit_or_silence():
    text = "\n".join(p.read_text(encoding="utf-8") for p in (CHART / "templates").glob("*.yaml"))
    assert "inhibit_rules:" not in text
    assert "silences" not in text
