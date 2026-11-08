"""架構守則的機械式檢查（對應 CLAUDE.md 第 1 節）。違規就讓 pytest 變紅。"""
import ast
import re
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


def test_alertmanager_behavior_lives_only_in_helm_chart():
    """路由、receiver、通道設定只能在 deploy/helm；aiops/ 的程式碼不得寫 Alertmanager 設定。"""
    import re

    terms = re.compile(
        r"\b(receivers?|inhibit_rules|route_matchers|matchers|group_by|group_wait|group_interval|repeat_interval"
        r"|slack_configs|webhook_configs|pagerduty_configs|msteams_configs|email_configs|resolve_timeout)\b"
    )
    bad = [
        f"{_rel(f)}:{n} {line.strip()[:60]}"
        for f in _py_files("**/*.py")
        for n, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1)
        if terms.search(line)
    ]
    assert not bad, bad


def test_domains_do_not_import_concrete_notifiers():
    """只能用 aiops.notifiers 的 get_notifier() 與 notifiers.base。"""
    assert not _violations(
        "domains/**/*.py", lambda f, m: m.startswith("aiops.notifiers.") and m != "aiops.notifiers.base"
    )


def test_datasource_specs_do_not_know_domains():
    banned = ("aiops.domains", "aiops.models", "aiops.plugins")
    assert not _violations("datasource_specs/**/*.py", lambda f, m: _starts(m, *banned))


def test_metric_query_is_pure_parsing():
    """metric_query 只做解析、組查詢、時間對齊：不連線、不碰平台其他層。"""
    def bad(f: Path, m: str) -> bool:
        return (m.startswith("aiops.") and not m.startswith("aiops.metric_query")) or _starts(m, "httpx", "requests")

    assert not _violations("metric_query/**/*.py", bad)


def test_llm_layer_is_transport_only():
    """LLM 出口只負責呼叫模型：不認識領域、外掛、資料表。輸出的驗證在各領域的 Command 裡做。"""
    banned = ("aiops.domains", "aiops.models", "aiops.plugins", "aiops.presets", "aiops.datasource_specs")
    assert not _violations("llm/**/*.py", lambda f, m: _starts(m, *banned))


def test_domains_do_not_import_concrete_llm_providers():
    """只能用 aiops.llm 的 get_llm() 與 llm.base。"""
    assert not _violations("domains/**/*.py", lambda f, m: m.startswith("aiops.llm.") and m != "aiops.llm.base")


def test_plugins_do_not_call_llm():
    assert not _violations("plugins/**/*.py", lambda f, m: _starts(m, "aiops.llm", "httpx"))


def test_security_does_not_know_business():
    banned = ("aiops.domains", "aiops.models", "aiops.plugins", "aiops.datasource_specs", "aiops.notifiers")
    assert not _violations("security/**/*.py", lambda f, m: _starts(m, *banned))


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


# ── 資料表擁有權（SQLAlchemy）：靠 DAO 類別裡引用了哪些 ORM 類別、呼叫了哪些寫入函式判斷 ──

# crud 的寫入函式、Session 的寫入方法、write_session，都算「寫」
_WRITE_CALLS = {"create", "create_many", "update", "upsert", "add", "add_all", "delete", "merge", "flush",
                "commit", "write_session", "execute", "bulk_save_objects", "setattr", "insert"}
_MUTATING_METHOD_PREFIXES = {"insert", "update", "delete", "remove", "clear", "set", "create", "save", "add", "upsert"}


def _model_tables() -> dict[str, str]:
    """ORM 類別名 -> 資料表名，直接從 SQLAlchemy 的 registry 取，新增資料表不用改這裡。"""
    from aiops.models.db import Base

    return {m.class_.__name__: m.class_.__tablename__ for m in Base.registry.mappers}


def _dao_classes(dao: Path) -> list[tuple[str, set[str], set[str], list[str]]]:
    """[(類別名, 用到的資料表, 呼叫過的函式名, 方法名)]"""
    models = _model_tables()
    tree = ast.parse(dao.read_text(encoding="utf-8"))
    out = []
    for cls in (n for n in tree.body if isinstance(n, ast.ClassDef)):
        tables = set()
        calls = set()
        for node in ast.walk(cls):
            if isinstance(node, ast.Name) and node.id in models:
                tables.add(models[node.id])
            elif isinstance(node, ast.Attribute) and node.attr in models:
                tables.add(models[node.attr])
            if isinstance(node, ast.Call):
                func = node.func
                calls.add(func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", ""))
        methods = [fn.name for fn in cls.body if isinstance(fn, ast.FunctionDef)]
        out.append((cls.name, tables, calls, methods))
    return out


def test_every_table_has_an_owner():
    from aiops.models import core
    from aiops.models.db import Base

    tables = set(Base.metadata.tables)
    assert tables == set(core.TABLE_OWNERS), tables ^ set(core.TABLE_OWNERS)


def test_cross_domain_table_access_is_read_dao():
    """非擁有者領域讀別人的資料表：類別必須叫 *ReadDAO（是否唯讀由下一條檢查）。"""
    from aiops.models import core

    bad = []
    for dao in _py_files("domains/*/dao.py"):
        domain = dao.parent.name
        for cls, tables, _calls, _methods in _dao_classes(dao):
            for table in sorted(tables):
                owner = core.TABLE_OWNERS.get(table)
                if owner is None:
                    bad.append(f"{_rel(dao)}:{cls} 使用了未登記擁有者的資料表 {table}")
                elif owner != domain and not cls.endswith("ReadDAO"):
                    bad.append(f"{_rel(dao)}:{cls} 跨領域碰 {table}（屬於 {owner}），類別必須以 ReadDAO 結尾且只讀")
    assert not bad, bad


def test_read_dao_is_read_only():
    """任何 *ReadDAO 都不得呼叫寫入函式（crud.create／update／upsert、write_session、Session.add…），也不得有寫入類方法名。"""
    bad = []
    for dao in _py_files("domains/*/dao.py"):
        for cls, _tables, calls, methods in _dao_classes(dao):
            if not cls.endswith("ReadDAO"):
                continue
            bad += [f"{_rel(dao)}:{cls} 呼叫 {c}" for c in sorted(calls & _WRITE_CALLS)]
            bad += [f"{_rel(dao)}:{cls}.{m}" for m in methods if m.split("_")[0] in _MUTATING_METHOD_PREFIXES]
    assert not bad, bad


def test_models_only_imported_by_dao_and_init():
    """資料庫只能經 DAO 碰：aiops.models 只准 models 自己、各領域 dao.py、initialization 匯入。"""
    def bad(f: Path, m: str) -> bool:
        rel = _rel(f)
        allowed = rel.startswith("aiops/models/") or rel.startswith("aiops/initialization/") or f.name == "dao.py"
        return _starts(m, "aiops.models") and not allowed

    assert not _violations("**/*.py", bad)


def test_tasks_only_orchestrate_commands():
    """tasks 只負責依序呼叫各領域的 Command；不得碰 DAO、資料表、外掛、資料源、通知出口、preset。"""
    def bad(f: Path, m: str) -> bool:
        if _starts(m, "aiops.models", "aiops.plugins", "aiops.datasource_specs", "aiops.notifiers", "aiops.presets"):
            return True
        if m.startswith("aiops.domains."):
            return not re.match(r"aiops\.domains\.\w+\.(commands(\.\w+)*|schemas)$", m)
        return False

    assert not _violations("tasks/**/*.py", bad)


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


def test_platform_only_reads_elasticsearch():
    """平台對 ES 只讀：只能呼叫 _search 與 GET 根路徑，不得出現寫入或管理端點。建索引、角色、key 是 es-setup 的事。"""
    text = (ROOT / "datasource_specs" / "elasticsearch.py").read_text(encoding="utf-8")
    assert re.search(r"/_search\b", text)
    forbidden = re.findall(r"_bulk|_doc\b|_create\b|_update\b|_delete_by_query|_security|_ilm|_template|"
                           r"\"PUT\"|\"DELETE\"|\.put\(|\.delete\(", text)
    assert not forbidden, forbidden
    posts = re.findall(r"\"(POST|GET|PUT|DELETE|PATCH)\"", text)
    assert set(posts) <= {"POST"}, posts  # POST 只用在 _search（查詢用 body）


def test_only_one_grafana_and_one_es_directory_under_deploy():
    """搬進來的 Grafana 與 ES 只能放在約定位置，不得在 deploy/ 底下再冒出第二份。"""
    deploy = ROOT.parent / "deploy"
    charts = {p.parent.name for p in (deploy / "helm").glob("*/Chart.yaml")}
    assert {"aiops-alertmanager", "aiops-platform", "aiops-grafana"} <= charts
    assert not [c for c in charts if "grafana" in c and c != "aiops-grafana"]
    assert not [p.name for p in deploy.iterdir() if p.is_dir() and "elastic" in p.name.lower() and p.name != "elasticsearch"]


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
    """現行偵測參數只能經 tuning 的提案 -> 回測 -> 核准寫入：tuning 以外碰這張表的類別都必須是 *ReadDAO。"""
    from aiops.models.core import TABLE_OWNERS

    assert TABLE_OWNERS["active_detector_configs"] == "tuning"
    bad = [
        f"{_rel(dao)}:{cls}"
        for dao in _py_files("domains/*/dao.py")
        if dao.parent.name != "tuning"
        for cls, tables, _calls, _methods in _dao_classes(dao)
        if "active_detector_configs" in tables and not cls.endswith("ReadDAO")
    ]
    assert not bad, bad
