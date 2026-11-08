"""scripts/dev_es_key.py 的純邏輯：把 key 換進 secrets.local.yaml，不弄壞其他內容。不連叢集。"""
import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("dev_es_key", ROOT / "scripts" / "dev_es_key.py")
dev_es_key = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev_es_key)


def test_set_key_replaces_only_the_key_line_and_keeps_comments_and_chinese():
    text = (ROOT / "secrets.local.example.yaml").read_text(encoding="utf-8")
    out = dev_es_key.set_key(text, "ZmFrZTppZA+/==")
    data = yaml.safe_load(out)
    assert data["es"]["AIOPS_ES_API_KEY"] == "ZmFrZTppZA+/==" and data["es"]["url"] == "http://127.0.0.1:19200"
    assert data["api"] == {"AIOPS_API_TOKEN": ""} and "唯讀 key" not in out.split("AIOPS_ES_API_KEY")[1].splitlines()[0]
    assert "本機開發的連線設定與金鑰範本" in out  # 其他註解還在


def test_set_key_can_run_twice_and_complains_when_the_line_is_missing():
    text = (ROOT / "secrets.local.example.yaml").read_text(encoding="utf-8")
    twice = dev_es_key.set_key(dev_es_key.set_key(text, "first"), "second")
    assert yaml.safe_load(twice)["es"]["AIOPS_ES_API_KEY"] == "second"
    with pytest.raises(ValueError, match="AIOPS_ES_API_KEY"):
        dev_es_key.set_key("es: {}\n", "x")


def test_admin_password_is_optional_and_read_from_the_local_file():
    text = (ROOT / "secrets.local.example.yaml").read_text(encoding="utf-8")
    assert dev_es_key.admin_password_from_file(text) is None  # 範本是空的 -> 走叢集 Secret
    filled = text.replace('admin_password: ""', 'admin_password: "pa:ss#word"')
    assert dev_es_key.admin_password_from_file(filled) == "pa:ss#word"
    assert dev_es_key.admin_password_from_file("") is None
    # 填了密碼之後，key 那一行照樣能換
    assert yaml.safe_load(dev_es_key.set_key(filled, "K"))["es"]["admin_password"] == "pa:ss#word"


def test_local_secrets_file_is_gitignored_and_the_example_has_no_values():
    assert "secrets.local.yaml" in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    example = yaml.safe_load((ROOT / "secrets.local.example.yaml").read_text(encoding="utf-8"))
    secrets = [v for section in example.values() for k, v in section.items() if k != "url"]
    assert all(v == "" for v in secrets), "範本進 git，不得有任何值"
