"""架構守則的機械式檢查（對應 CLAUDE.md 第 1 節）。違規就讓 pytest 變紅。"""
import ast
from pathlib import Path

from aiops.initialization import init_plugins
from aiops.plugins.registry import (
    DetectorRegistry,
    GrouperRegistry,
    ScorerRegistry,
    SectionRegistry,
    SuppressorRegistry,
)
from aiops.presets import PRESETS

ROOT = Path(__file__).resolve().parents[1] / "aiops"


def _py_files(pattern: str) -> list[Path]:
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
    return p.relative_to(ROOT.parent).as_posix()


def _violations(pattern: str, is_bad) -> list[str]:
    return [f"{_rel(f)} -> {m}" for f in _py_files(pattern) for m in _imports(f) if is_bad(f, m)]


def _starts(m: str, *prefixes: str) -> bool:
    return any(m == p or m.startswith(p + ".") for p in prefixes)


# ── 依賴方向 ──


def test_plugins_are_pure_parts():
    """外掛不認識業務、不碰 DB、不碰基礎設施與資料源。"""
    banned = ("aiops.domains", "aiops.models", "aiops.infrastructure", "aiops.datasource_specs",
              "aiops.presets", "aiops.initialization", "aiops.notifiers")
    assert not _violations("plugins/**/*.py", lambda f, m: _starts(m, *banned))


def test_notifiers_only_transport():
    """通知出口只負責傳送：不認識領域、外掛與場景，也不碰資料表。"""
    banned = ("aiops.domains", "aiops.models", "aiops.plugins", "aiops.presets", "aiops.datasource_specs")
    assert not _violations("notifiers/**/*.py", lambda f, m: _starts(m, *banned))


def test_domains_do_not_import_concrete_notifiers():
    """只能用 aiops.notifiers 的 get_notifier() 與 notifiers.base。"""
    assert not _violations(
        "domains/**/*.py", lambda f, m: m.startswith("aiops.notifiers.") and m != "aiops.notifiers.base"
    )


def test_datasource_specs_do_not_know_domains():
    banned = ("aiops.domains", "aiops.models", "aiops.plugins")
    assert not _violations("datasource_specs/**/*.py", lambda f, m: _starts(m, *banned))


def test_presets_are_names_only():
    banned = ("aiops.domains", "aiops.plugins", "aiops.models", "aiops.datasource_specs")
    assert not _violations("presets/**/*.py", lambda f, m: _starts(m, *banned))


def test_no_cross_domain_imports():
    def bad(f: Path, m: str) -> bool:
        own = f.relative_to(ROOT / "domains").parts[0]
        return m.startswith("aiops.domains.") and m.split(".")[2] != own

    assert not _violations("domains/**/*.py", bad)


def test_only_dao_touches_models():
    assert not _violations("domains/**/*.py", lambda f, m: _starts(m, "aiops.models") and f.name != "dao.py")


_MUTATORS = {"insert", "update", "delete", "remove", "clear", "set", "create", "save", "add"}


def _table_access(dao: Path):
    """回傳 [(類別名, 資料表, 呼叫的方法或 None)]：dao.py 內每一處 core.<table> 的使用。"""
    tree = ast.parse(dao.read_text(encoding="utf-8"))
    def is_core_table(n) -> bool:
        return isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "core"

    out = []
    for cls in (n for n in tree.body if isinstance(n, ast.ClassDef)):
        called = set()  # 已被「core.<table>.method()」涵蓋的 core.<table> 節點，不重複記成裸存取
        for node in ast.walk(cls):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and is_core_table(node.func.value):
                called.add(id(node.func.value))
                out.append((cls.name, node.func.value.attr, node.func.attr))
        for node in ast.walk(cls):
            if is_core_table(node) and id(node) not in called:
                out.append((cls.name, node.attr, None))
    return out


def test_every_table_has_an_owner():
    from aiops.models import core

    tables = {k for k, v in vars(core).items() if isinstance(v, core.InMemoryTable)}
    assert tables == set(core.TABLE_OWNERS), tables ^ set(core.TABLE_OWNERS)


def test_cross_domain_table_access_is_read_only_read_dao():
    """非擁有者領域讀別人的資料表：類別必須叫 *ReadDAO，且不得有任何變更操作。"""
    from aiops.models import core

    bad = []
    for dao in _py_files("domains/*/dao.py"):
        domain = dao.parent.name
        for cls, table, method in _table_access(dao):
            if table not in core.TABLE_OWNERS:
                bad.append(f"{_rel(dao)}:{cls} 使用了未登記擁有者的資料表 {table}")
            elif core.TABLE_OWNERS[table] != domain:
                if not cls.endswith("ReadDAO"):
                    bad.append(f"{_rel(dao)}:{cls} 跨領域讀 {table}，類別必須以 ReadDAO 結尾")
                if method is None or method in _MUTATORS:
                    bad.append(f"{_rel(dao)}:{cls} 對別的領域的 {table} 做 {method or '非方法呼叫的存取'}")
    assert not bad, bad


def test_read_dao_has_no_mutating_methods():
    """任何 *ReadDAO（即使讀自己的表）都不得有變更資料的呼叫或方法名稱。"""
    bad = []
    for dao in _py_files("domains/*/dao.py"):
        tree = ast.parse(dao.read_text(encoding="utf-8"))
        for cls in (n for n in tree.body if isinstance(n, ast.ClassDef) and n.name.endswith("ReadDAO")):
            for fn in (n for n in cls.body if isinstance(n, ast.FunctionDef)):
                if fn.name.split("_")[0] in _MUTATORS:
                    bad.append(f"{_rel(dao)}:{cls.name}.{fn.name}")
            bad += [f"{_rel(dao)}:{cls.name} 呼叫 {m}" for c, t, m in _table_access(dao) if c == cls.name and m in _MUTATORS]
    assert not bad, bad


def test_commands_only_use_plugin_abstractions():
    allowed = {"aiops.plugins.base", "aiops.plugins.registry"}
    assert not _violations(
        "domains/*/commands/**/*.py", lambda f, m: _starts(m, "aiops.plugins") and m not in allowed
    )


def test_domains_do_not_import_concrete_datasource_specs():
    """只能走 aiops.datasource_specs 套件入口的 get_datasource_spec。"""
    assert not _violations(
        "domains/**/*.py", lambda f, m: m.startswith("aiops.datasource_specs.") and m != "aiops.datasource_specs.base"
    )


# ── api.py 零邏輯 ──


def test_api_only_depends_on_own_domain_commands_and_schemas():
    def bad(f: Path, m: str) -> bool:
        d = f.parent.name
        return not (_starts(m, "fastapi", "typing", "pydantic")
                    or _starts(m, f"aiops.domains.{d}.commands", f"aiops.domains.{d}.schemas"))

    assert not _violations("domains/*/api.py", bad)


def test_api_has_no_logic_statements():
    banned = (ast.If, ast.IfExp, ast.For, ast.While, ast.Try, ast.With,
              ast.ListComp, ast.DictComp, ast.SetComp, ast.GeneratorExp)
    bad = [
        f"{_rel(f)}:{n.lineno} {type(n).__name__}"
        for f in _py_files("domains/*/api.py")
        for n in ast.walk(ast.parse(f.read_text(encoding="utf-8")))
        if isinstance(n, banned)
    ]
    assert not bad, bad


# ── 檔案組織 ──


def test_no_service_modules():
    bad = [_rel(f) for f in _py_files("**/*_service.py")] + [_rel(f) for f in _py_files("**/services/**/*.py")]
    assert not bad, bad


def test_no_grafana_specific_code():
    """Grafana 只接收；平台側不為它開領域、模組或專用外掛。要改 Grafana 只動 helm chart。"""
    bad = [_rel(p) for p in ROOT.rglob("*") if "grafana" in p.name.lower() and "__pycache__" not in p.parts]
    assert not bad, bad


def test_presets_reference_only_registered_plugins():
    init_plugins()
    checks = (
        ("detectors", DetectorRegistry),
        ("groupers", GrouperRegistry),
        ("scorers", ScorerRegistry),
        ("rca_sections", SectionRegistry),
        ("suppressors", SuppressorRegistry),
    )
    missing = [
        f"{p.name}.{field}: {n}"
        for p in PRESETS.values()
        for field, reg in checks
        for n in getattr(p, field)
        if not reg.has(n)
    ]
    assert not missing, missing


def test_only_tuning_writes_active_detector_config():
    """現行偵測參數只能經 tuning 的提案 -> 回測 -> 核准寫入。"""
    bad = [
        _rel(f)
        for f in _py_files("**/*.py")
        if "active_detector_configs.insert" in f.read_text(encoding="utf-8")
        or "active_detector_configs.update" in f.read_text(encoding="utf-8")
        if _rel(f) != "aiops/domains/tuning/dao.py"
    ]
    assert not bad, bad
