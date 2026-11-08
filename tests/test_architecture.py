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
    for old in ("domains", "plugins", "presets", "commands", "llm", "notifiers", "utils", "core", "operators",
                "workflows", "tasks", "api", "initialization", "security", "infrastructure", "datasource_specs",
                "metric_query", "app.py", "runner.py"):
        assert not (ROOT / old).exists(), f"舊層 aiops/{old} 不該存在"
    text = "\n".join(p.read_text(encoding="utf-8") for p in ROOT.rglob("*.py"))
    assert "ReadDAO" not in text


def test_operators_do_not_touch_orm_or_storage_except_admin():
    bad = _violations(_py("stages") + _py("control_plane"), ["aiops.storage", "sqlalchemy"], allow=())
    bad = [b for b in bad if not b.startswith("control_plane/")]
    assert not bad, bad


def test_operators_do_not_use_the_network_except_llm_notifiers_admin():
    # fetch.py 是唯一的抓資料算子，透過 sources 取資料
    allowed = ("stages/enrichment/", "stages/notification/notifiers.py", "stages/notification/transport.py",
               "control_plane/", "stages/ingestion/fetch.py")
    bad = [b for b in _violations(_py("stages") + _py("control_plane"),
                                  ["httpx", "requests", "urllib", "socket", "aiops.sources"])
           if not b.startswith(allowed)]
    assert not bad, bad


def test_datasource_specs_do_not_import_storage_or_operators():
    assert not _violations(_py("sources"), ["aiops.storage", "aiops.stages", "aiops.control_plane", "aiops.models"])


def test_storage_has_no_business_logic_imports():
    assert not _violations(_py("storage"), ["aiops.stages", "aiops.control_plane", "aiops.interfaces.api",
                                            "aiops.sources"])


def test_core_and_engine_do_not_import_concrete_operators():
    bad = _violations(_py("engine"), ["aiops.stages", "aiops.control_plane", "aiops.storage", "aiops.interfaces.api"])
    assert not bad, bad


def test_detectors_do_not_import_each_other():
    files = _py("stages", "detectors")
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


@pytest.mark.parametrize("path", _py("interfaces", "api"), ids=lambda p: p.name)
def test_api_layer_has_zero_business_logic(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    banned = (ast.If, ast.For, ast.While, ast.Try)
    bad = [type(n).__name__ for n in ast.walk(tree) if isinstance(n, banned)]
    assert not bad, f"{path.name} 含有 {bad}"
    # API 可以查 Storage（repository），但不能碰 ORM 或具體算子
    assert not _violations([path], ["aiops.storage.models", "aiops.storage.db", "aiops.stages", "aiops.control_plane",
                                    "sqlalchemy"])


def test_every_router_requires_the_token():
    src = (ROOT / "interfaces" / "bootstrap" / "__init__.py").read_text(encoding="utf-8")
    assert "include_router(router, dependencies=[Depends(require_token)])" in src


def test_llm_noop_never_builds_a_client():
    from aiops.stages.enrichment.providers import build_llm

    assert build_llm("noop") is None


def test_no_insecure_model_serialization():
    """模型檔只能用安全格式（npz + allow_pickle=False）；pickle 系列讀回時可以執行任意程式。"""
    bad = _violations(_py(), ["pickle", "joblib", "dill", "cloudpickle", "shelve", "marshal"])
    assert not bad, bad
    for f in _py():
        assert "allow_pickle=True" not in f.read_text(encoding="utf-8"), f


def test_rca_sections_and_root_cause_algorithms_are_registered_not_hardcoded():
    """報告段落與根因演算法都走 OperatorRegistry；rca.report 不得 import 具體段落或演算法。"""
    report = _imports(ROOT / "stages" / "rca" / "report.py")
    assert not [i for i in report if ".rca.sections" in i or ".rca.algorithms" in i], report
    from aiops.engine.registry import OperatorRegistry
    from aiops.interfaces.runner import load_all

    load_all()
    assert len(OperatorRegistry.names("rca.section.")) >= 9
    assert {"rca.root_cause.topological", "rca.root_cause.pagerank", "rca.root_cause.cross_correlation"} <= set(
        OperatorRegistry.names("rca.root_cause."))


def test_every_workflow_loads_and_its_operators_are_registered():
    from aiops.engine.pipeline import Pipeline, load_workflow
    from aiops.interfaces.runner import load_all

    load_all()
    for f in sorted((ROOT / "pipelines").rglob("*.yaml")):
        Pipeline(load_workflow(f))
