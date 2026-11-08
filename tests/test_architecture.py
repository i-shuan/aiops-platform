"""架構防線（CLAUDE.md 第 1 節）：用 AST 檢查 import 與禁用語法，違規就紅燈。"""
import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "aiops"


def _py(*parts: str):
    base = ROOT.joinpath(*parts)
    return [base] if base.is_file() else sorted(base.rglob("*.py"))


def _imports(path: Path) -> set[str]:
    out = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            out |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
            out |= {f"{node.module}.{a.name}" for a in node.names}
    return out


def _violations(files, forbidden_prefixes, allow=()):
    bad = []
    for f in files:
        if f.relative_to(ROOT).as_posix() in allow:
            continue
        for imp in _imports(f):
            if any(imp == p or imp.startswith(p + ".") for p in forbidden_prefixes):
                bad.append(f"{f.relative_to(ROOT).as_posix()} imports {imp}")
    return bad


def test_old_layers_are_gone():
    for old in ("domains", "plugins", "presets", "commands", "llm", "notifiers", "utils"):
        assert not (ROOT / old).exists(), f"舊層 aiops/{old} 不該存在"
    text = "\n".join(p.read_text(encoding="utf-8") for p in ROOT.rglob("*.py"))
    assert "ReadDAO" not in text


def test_operators_do_not_touch_orm_or_storage_except_admin():
    bad = _violations(_py("operators"), ["aiops.storage", "sqlalchemy"], allow=())
    bad = [b for b in bad if not b.startswith("operators/admin/")]
    assert not bad, bad


def test_operators_do_not_use_the_network_except_llm_notifiers_admin():
    # fetch.py 是唯一的抓資料算子，透過 datasource_specs 取資料
    allowed = ("operators/llm/", "operators/notifiers/", "operators/admin/", "operators/fetch.py")
    bad = [b for b in _violations(_py("operators"), ["httpx", "requests", "urllib", "socket", "aiops.datasource_specs"])
           if not b.startswith(allowed)]
    assert not bad, bad


def test_datasource_specs_do_not_import_storage_or_operators():
    assert not _violations(_py("datasource_specs"), ["aiops.storage", "aiops.operators", "aiops.models"])


def test_storage_has_no_business_logic_imports():
    assert not _violations(_py("storage"), ["aiops.operators", "aiops.api", "aiops.datasource_specs"])


def test_core_and_engine_do_not_import_concrete_operators():
    bad = _violations(_py("core"), ["aiops.operators", "aiops.storage", "aiops.api"])
    assert not bad, bad


def test_detectors_do_not_import_each_other():
    files = _py("operators", "detectors")
    names = {f.stem for f in files if f.stem not in ("__init__", "base")}
    bad = []
    for f in files:
        for imp in _imports(f):
            parts = imp.split(".")
            if "detectors" in parts:
                tail = parts[parts.index("detectors") + 1:]
                if tail and tail[0] in names and tail[0] != f.stem:
                    bad.append(f"{f.name} imports {imp}")
    assert not bad, bad


@pytest.mark.parametrize("path", _py("api"), ids=lambda p: p.name)
def test_api_layer_has_zero_business_logic(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    banned = (ast.If, ast.For, ast.While, ast.Try)
    bad = [type(n).__name__ for n in ast.walk(tree) if isinstance(n, banned)]
    assert not bad, f"{path.name} 含有 {bad}"
    # API 可以查 Storage（repository），但不能碰 ORM 或具體算子
    assert not _violations([path], ["aiops.storage.models", "aiops.storage.db", "aiops.operators", "sqlalchemy"])


def test_every_router_requires_the_token():
    src = (ROOT / "initialization" / "__init__.py").read_text(encoding="utf-8")
    assert "include_router(router, dependencies=[Depends(require_token)])" in src


def test_llm_noop_never_builds_a_client():
    from aiops.operators.llm.providers import build_llm

    assert build_llm("noop") is None


def test_every_workflow_loads_and_its_operators_are_registered():
    from aiops.core.pipeline import Pipeline, load_workflow
    from aiops.operators import load_all

    load_all()
    for f in sorted((ROOT / "workflows").glob("*.yaml")):
        Pipeline(load_workflow(f))
