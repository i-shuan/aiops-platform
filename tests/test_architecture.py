"""架構守則的機械式檢查（對應 CLAUDE.md 第 1 節）。違規就讓 CI / pytest 變紅。"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "aiops"


def _py_files(pattern: str):
    return sorted(ROOT.glob(pattern))


def _imports(path: Path) -> list[str]:
    mods = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            mods += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.append(node.module)
    return mods


def _rel(p: Path) -> str:
    return str(p.relative_to(ROOT.parent))


def test_plugins_do_not_import_domains():
    bad = [
        f"{_rel(f)} -> {m}"
        for f in _py_files("plugins/**/*.py")
        for m in _imports(f)
        if m.startswith("aiops.domains")
    ]
    assert not bad, bad


def test_api_only_depends_on_own_domain_commands_and_schemas():
    allowed = ("fastapi", "typing", "pydantic")
    bad = []
    for f in _py_files("domains/*/api.py"):
        domain = f.parent.name
        for m in _imports(f):
            ok = m.startswith(allowed) or m.startswith(
                (f"aiops.domains.{domain}.commands", f"aiops.domains.{domain}.schemas")
            )
            if not ok:
                bad.append(f"{_rel(f)} -> {m}")
    assert not bad, bad


def test_api_has_no_logic_statements():
    banned = (ast.If, ast.For, ast.While, ast.Try, ast.With, ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp)
    bad = [
        f"{_rel(f)}:{n.lineno} {type(n).__name__}"
        for f in _py_files("domains/*/api.py")
        for n in ast.walk(ast.parse(f.read_text(encoding="utf-8")))
        if isinstance(n, banned)
    ]
    assert not bad, bad


def test_commands_only_use_plugin_abstractions():
    allowed = {"aiops.plugins.base", "aiops.plugins.registry"}
    bad = [
        f"{_rel(f)} -> {m}"
        for f in _py_files("domains/*/commands/**/*.py")
        for m in _imports(f)
        if m.startswith("aiops.plugins") and m not in allowed
    ]
    assert not bad, bad


def test_domains_do_not_import_concrete_datasource_specs():
    # 只能走 aiops.datasource_specs 套件入口的 get_datasource_spec，不可直接 import mock/prometheus 等實作
    bad = [
        f"{_rel(f)} -> {m}"
        for f in _py_files("domains/**/*.py")
        for m in _imports(f)
        if m.startswith("aiops.datasource_specs.") and m != "aiops.datasource_specs.base"
    ]
    assert not bad, bad


def test_no_service_modules():
    bad = [_rel(f) for f in _py_files("**/*_service.py")] + [_rel(f) for f in _py_files("**/services/**/*.py")]
    assert not bad, bad
