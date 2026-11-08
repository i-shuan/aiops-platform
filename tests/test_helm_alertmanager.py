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


def test_resolve_timeout_is_long_because_platform_sends_no_ends_at():
    assert _render()["global"]["resolve_timeout"] == "24h"


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
