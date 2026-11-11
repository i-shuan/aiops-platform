"""架構防線（CLAUDE.md 第 1 節）：用 AST 檢查 import 與禁用語法，違規就紅燈。"""
import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "aiops"


def _py(*parts: str):
    base = ROOT.joinpath(*parts)
    return [base] if base.is_file() else sorted(base.rglob("*.py"))


def _callee(call: ast.Call) -> str:
    return call.func.attr if isinstance(call.func, ast.Attribute) else getattr(call.func, "id", "")


def _imports(path: Path) -> set[str]:
    """靜態 import 敘述，加上 importlib.import_module("x")／__import__("x") 的字串引數：動態載入也是 import（稽核 G1）。"""
    out = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            out |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
            out |= {f"{node.module}.{a.name}" for a in node.names}
        elif isinstance(node, ast.Call) and _callee(node) in ("import_module", "__import__") and node.args \
                and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            out.add(node.args[0].value)
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


# urllib 只禁會開連線的 urllib.request；urllib.parse 是純字串處理（拼深度連結的網址用），不算連外
NETWORK_LIBS = ["httpx", "requests", "urllib.request", "socket", "http.client", "http.server", "aiohttp"]


def test_operators_do_not_use_the_network_except_llm_notifiers_admin():
    files = _py("stages") + _py("control_plane")
    # 連外函式庫：只有 LLM 供應商、通知傳輸、控制面可以用
    allowed = ("stages/enrichment/", "stages/notification/notifiers.py", "stages/notification/transport.py", "control_plane/")
    bad = [b for b in _violations(files, NETWORK_LIBS) if not b.startswith(allowed)]
    # 資料源：只有抓資料的算子 ingestion/fetch.py 與控制面（資料源檢查）可以用；enrichment 連的是 LLM，不是資料源
    bad += [b for b in _violations(files, ["aiops.sources"]) if not b.startswith(("stages/ingestion/fetch.py", "control_plane/"))]
    assert not bad, bad


def test_datasource_specs_do_not_import_storage_or_operators():
    assert not _violations(_py("sources"), ["aiops.storage", "aiops.stages", "aiops.control_plane", "aiops.models"])


def test_storage_has_no_business_logic_imports():
    assert not _violations(_py("storage"), ["aiops.stages", "aiops.control_plane", "aiops.interfaces.api",
                                            "aiops.sources"])


def test_core_and_engine_do_not_import_concrete_operators():
    bad = _violations(_py("engine"), ["aiops.stages", "aiops.control_plane", "aiops.storage", "aiops.interfaces.api"])
    assert not bad, bad


_OPERATOR_NAME = re.compile(r"^(detector|alert|notifier|rca|ingestion|enrichment|admin)\.")


def _operator_name_literals(source: str) -> list[str]:
    """原始碼裡長得像算子名稱的字串常數（含 f-string 的固定片段）。
    不算：docstring、OperatorRegistry.names(<前綴>) 的引數（列出某類算子，不是特判某一個）。"""
    tree = ast.parse(source)
    skip: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body and \
                isinstance(node.body[0], ast.Expr) and isinstance(node.body[0].value, ast.Constant):
            skip.add(id(node.body[0].value))
        if isinstance(node, ast.Call) and getattr(node.func, "attr", getattr(node.func, "id", "")) == "names":
            skip |= {id(a) for a in node.args}
    return [f"{n.lineno}: {n.value!r}" for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in skip and _OPERATOR_NAME.match(n.value)]


def test_engine_and_runner_never_special_case_an_operator_name():
    """CLAUDE.md 第 1 節「嚴禁在引擎內 hardcode 具體算子邏輯」（稽核 Q2）：引擎與 runner 只能用算子自己宣告的屬性
    （scope、has_baseline、include／exclude_entities）分流，不准拿算子名稱字串比對。"""
    allowed = '''"""detector.mad 這種說明文字可以。"""
from pydantic import Field
workflow: str = Field("rca_report", pattern=r"^[A-Za-z0-9_-]+$")
detectors = OperatorRegistry.names("detector.")
run("admin_tune")
'''
    flagged = '''if op.operator_name == "detector.mad":
    pass
if name.startswith("alert."):
    pass
label = f"notifier.{kind}"
'''
    assert _operator_name_literals(allowed) == []  # 檢查器本身：workflow 名稱、names(prefix) 不誤報
    assert len(_operator_name_literals(flagged)) == 3  # 檢查器本身：比對、前綴、f-string 都抓得到
    files = _py("engine") + _py("interfaces", "runner.py")
    assert len(files) > 5, "掃描範圍是空的，這個檢查會變成空轉"
    bad = [f"{f.relative_to(ROOT).as_posix()}:{hit}" for f in files for hit in _operator_name_literals(_src(f))]
    assert not bad, "\n".join(bad)


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


def test_legacy_llm_operators_still_default_to_noop():
    """CLAUDE.md §1 第 3 條：舊引擎的 llm_summary／llm_briefing 預設 provider=noop（完全不連外、直接用模板）。
    RCA 新算子（extract_evidence、rca_narrator）沒有 noop，不在這裡檢查；它們的降級是 rca_status=undetermined（各自的測試檔）。"""
    import inspect

    from aiops.engine.registry import OperatorRegistry
    import aiops.stages.enrichment.summarizer  # noqa: F401 — 註冊算子

    for name in ("enrichment.llm_summary", "enrichment.llm_briefing"):
        assert inspect.signature(OperatorRegistry.get(name)).parameters["provider"].default == "noop", name


# 告警路徑（偵測 → 收斂 → 計分 → 抑制 → 通知）的算子；rca 也列進來：證據層與機率模型不准 import enrichment
# （docs/COGNITIVE_RCA_DESIGN.md 第 4 節），LLM 只能透過 ctx.rca_evidence 進到 RCA 判斷
_NO_LLM_STAGES = ("ingestion", "detectors", "ensemble", "grouping", "scoring", "suppression", "notification", "rca")


def test_alert_path_does_not_depend_on_llm():
    """CLAUDE.md §1 第 3 條：告警路徑嚴禁依賴 LLM。
    1. stages/ 底下除了 enrichment/ 本身，沒有任何模組 import aiops.stages.enrichment（引擎也不行）。
    2. pipelines/detection/ 的告警 workflow 不准掛 enrichment.* 節點（LLM 掛了，告警照樣要送出去）。"""
    import yaml

    files = [f for d in _NO_LLM_STAGES for f in _py("stages", d)] + _py("engine")
    bad = _violations(files, ["aiops.stages.enrichment"])
    assert not bad, bad
    detection = sorted((ROOT / "pipelines" / "detection").glob("*.yaml"))
    assert detection
    bad = [f"{p.name}: {n.get('name')} = {n.get('operator')}" for p in detection
           for n in (yaml.safe_load(p.read_text(encoding="utf-8")) or {}).get("nodes") or []
           if str(n.get("operator", "")).startswith("enrichment.")]
    assert not bad, f"告警 workflow 不准依賴 LLM：{bad}"


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
    assert {"rca.root_cause.topological", "rca.root_cause.evidence", "rca.root_cause.llm"} <= set(
        OperatorRegistry.names("rca.root_cause."))


def test_every_workflow_loads_and_its_operators_are_registered():
    from aiops.engine.pipeline import Pipeline, load_workflow
    from aiops.interfaces.runner import load_all

    load_all()
    files = sorted((ROOT / "pipelines").rglob("*.yaml"))
    # 路徑寫錯時 rglob 是空的、迴圈一次都不跑也會「通過」：先確認三個子資料夾都真的掃到檔案
    assert {f.parent.name for f in files} == {"detection", "rca", "control"} and len(files) >= 20
    for f in files:
        assert Pipeline(load_workflow(f)).workflow.nodes, f"{f.name} 沒有任何節點"


def test_production_code_does_not_know_it_is_under_test():
    """CLAUDE.md 第 100 節第 2 條：生產代碼不可感知測試環境，測試隔離只能靠依賴注入。"""
    import re

    banned = re.compile(r"\bpytest\b|PYTEST_CURRENT_TEST|\bTESTING\b|\bis_test\b|\btest_env\b|_TEST_MODE|unittest\.mock")
    bad = []
    for f in ROOT.rglob("*.py"):
        for no, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if banned.search(line):
                bad.append(f"{f.relative_to(ROOT).as_posix()}:{no}: {line.strip()}")
    assert not bad, "\n".join(bad)


# ── 代碼純潔度與測試誠實性（CLAUDE.md 第 100 節）──

_MUTATORS = {"append", "extend", "update", "add", "insert", "pop", "popitem", "setdefault", "clear", "remove", "discard"}
_MUTABLE_CTORS = {"dict", "list", "set", "Counter", "defaultdict", "OrderedDict", "deque"}


def _src(f: Path) -> str:
    return f.read_text(encoding="utf-8-sig")


def _is_mutable(v) -> bool:
    if isinstance(v, (ast.Dict, ast.List, ast.Set, ast.DictComp, ast.ListComp, ast.SetComp)):
        return True
    if isinstance(v, ast.Call):
        fn = v.func
        return (fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", "")) in _MUTABLE_CTORS
    return False


def _assigned(st):
    if isinstance(st, ast.Assign):
        return [t.id for t in st.targets if isinstance(t, ast.Name)], st.value
    if isinstance(st, ast.AnnAssign) and isinstance(st.target, ast.Name):
        return [st.target.id], st.value
    return [], None


def _mutations(node) -> list[tuple[int, ast.expr]]:
    """node 底下所有「原地修改」的對象：x.append(...)、x[k] = ...、x[k] += ...、del x[k]。"""
    out = []
    for n in ast.walk(node):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in _MUTATORS:
            out.append((n.lineno, n.func.value))
        targets = n.targets if isinstance(n, (ast.Assign, ast.Delete)) else [n.target] if isinstance(n, ast.AugAssign) else []
        out += [(n.lineno, t.value) for t in targets if isinstance(t, ast.Subscript)]
    return out


def _local_names(fn) -> set[str]:
    args = fn.args
    names = {a.arg for a in args.posonlyargs + args.args + args.kwonlyargs}
    for n in ast.walk(fn):
        targets = n.targets if isinstance(n, ast.Assign) else [n.target] if isinstance(n, (ast.AnnAssign, ast.For)) else []
        names |= {t.id for t in targets if isinstance(t, ast.Name)}
    return names


# 只在 import 期間由裝飾器填入、之後唯讀的註冊表；不隨 Pipeline.run 改變
_IMPORT_TIME_REGISTRIES = {("engine/registry.py", "_items"), ("stages/enrichment/registry.py", "_ENTRIES")}
# 行程層級、刻意只建一次的東西：資料庫連線池（db.configure）與 LLM provider 的一次性掃描旗標
_ALLOWED_GLOBALS = {("storage/db.py", "_url"), ("storage/db.py", "_engine"), ("storage/db.py", "_factory"),
                    ("stages/enrichment/registry.py", "_scanned")}


def test_no_module_or_class_level_state_is_mutated_at_runtime():
    """第 100 節第 3 條：模組層級、類別層級的可變狀態一律不准（常駐進程跨輪殘留、無限長大）。
    宣告成常數的 dict／list 可以有，但不准在任何地方被原地修改；import 期的註冊表另列白名單。"""
    files = [(f.relative_to(ROOT).as_posix(), ast.parse(_src(f))) for f in _py()]
    class_level = {(rel, name) for rel, tree in files for cls in ast.walk(tree) if isinstance(cls, ast.ClassDef)
                   for st in cls.body for name in _assigned(st)[0] if _is_mutable(_assigned(st)[1])}
    class_attrs = {name for _, name in class_level}
    bad = []
    for rel, tree in files:
        module_level = {name for st in tree.body for name in _assigned(st)[0] if _is_mutable(_assigned(st)[1])}
        for fn in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
            local = _local_names(fn)
            for line, obj in _mutations(fn):
                if isinstance(obj, ast.Name) and obj.id in module_level and obj.id not in local:
                    owner = (rel, obj.id)
                elif isinstance(obj, ast.Attribute) and obj.attr in class_attrs:
                    owner = next(o for o in class_level if o[1] == obj.attr)
                else:
                    continue
                if owner not in _IMPORT_TIME_REGISTRIES:
                    bad.append(f"{rel}:{line}: 執行期修改 {owner[0]} 的模組／類別層級 {owner[1]}")
        for n in ast.walk(tree):
            if isinstance(n, ast.Global):
                bad += [f"{rel}:{n.lineno}: global {g}" for g in n.names if (rel, g) not in _ALLOWED_GLOBALS]
    assert class_level, "一個類別層級的可變物件都沒掃到（tunable 應該有），這個檢查會變成空轉"
    assert not bad, "\n".join(bad)


def test_no_mutable_default_arguments():
    bad = [f"{f.relative_to(ROOT).as_posix()}:{fn.lineno}: {fn.name}"
           for f in _py() for fn in ast.walk(ast.parse(_src(f))) if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
           for d in fn.args.defaults + [d for d in fn.args.kw_defaults if d is not None] if _is_mutable(d)]
    assert not bad, "\n".join(bad)


def _innermost_function(tree) -> dict[int, str]:
    owner = {}
    for fn in ast.walk(tree):  # 由外往內走：內層函式後到，會覆寫外層
        if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for n in ast.walk(fn):
                owner[id(n)] = fn.name
    return owner


# 刻意不出聲的窄型別 except（合約就是「失敗回 None」或「照樣往上丟原本的錯」）
_SILENT_EXCEPT_ALLOWED = {
    ("storage/crud.py", "insert_if_absent"),  # 撞唯一鍵 = 別人先寫了，合約是回 None
    ("storage/db.py", "_quiet_rollback"),  # 連線已斷時的 rollback；呼叫端會把原本的錯往上丟
}


def test_exceptions_are_never_swallowed_silently():
    """except Exception／裸 except 必須 re-raise、記進 *.errors，或 logger.warning 以上；
    任何 except 區塊若沒有任何呼叫也沒有 raise（pass、continue、return 常數），只能是白名單裡的合約。"""
    bad = []
    for f in _py():
        rel, tree = f.relative_to(ROOT).as_posix(), ast.parse(_src(f))
        owner = _innermost_function(tree)
        for h in [n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)]:
            body = list(ast.walk(ast.Module(body=h.body, type_ignores=[])))
            raises = any(isinstance(n, ast.Raise) for n in body)
            calls = [n.func for n in body if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
            recorded = any(c.attr == "append" and getattr(c.value, "attr", "") == "errors" for c in calls)
            logged = any(c.attr in ("warning", "error", "exception", "critical") for c in calls)
            types = (h.type.elts if isinstance(h.type, ast.Tuple) else [h.type]) if h.type else []
            names = {getattr(t, "id", getattr(t, "attr", "")) for t in types} or {"<bare>"}
            if names & {"Exception", "BaseException", "<bare>"} and not (raises or recorded or logged):
                bad.append(f"{rel}:{h.lineno}: 寬鬆 except 沒有 re-raise／errors／warning")
            if not raises and not any(isinstance(n, ast.Call) for n in body) and \
                    (rel, owner.get(id(h))) not in _SILENT_EXCEPT_ALLOWED:
                bad.append(f"{rel}:{h.lineno}: except 什麼都沒做（{ast.unparse(h.body[0])}）")
    assert not bad, "\n".join(bad)


def test_algorithms_do_not_special_case_entities_or_lengths():
    """不准對特定服務名或特定資料長度走不同邏輯（== "cart-api"、len(x) == 120）。
    服務名以內建 YAML 的 targets 與 kebab-case 字面值為準；CLI 子命令名（run-once 這類）不算。"""
    import re

    import yaml

    entities = {t["entity"] for f in (ROOT / "pipelines").rglob("*.yaml")
                for t in (yaml.safe_load(f.read_text(encoding="utf-8")) or {}).get("targets") or [] if "entity" in t}
    kebab = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)+$")
    bad = []
    for f in _py():
        rel = f.relative_to(ROOT).as_posix()
        if rel.startswith("interfaces/cli/"):
            continue
        for n in ast.walk(ast.parse(_src(f))):
            if not isinstance(n, ast.Compare):
                continue
            parts = [n.left, *n.comparators]
            lits = [e for p in parts for e in (p.elts if isinstance(p, (ast.Tuple, ast.List, ast.Set)) else [p])
                    if isinstance(e, ast.Constant) and isinstance(e.value, str)]
            if any(kebab.match(c.value) or c.value in entities for c in lits):
                bad.append(f"{rel}:{n.lineno}: {ast.unparse(n)}")
            exact = any(isinstance(op, (ast.Eq, ast.NotEq)) for op in n.ops)
            if exact and any(isinstance(p, ast.Call) and getattr(p.func, "id", "") == "len" for p in parts) and \
                    any(isinstance(p, ast.Constant) and type(p.value) is int and p.value > 2 for p in parts):
                bad.append(f"{rel}:{n.lineno}: {ast.unparse(n)}")
    assert entities, "內建 YAML 一個 target 都沒讀到，這個檢查會變成空轉"
    assert not bad, "\n".join(bad)


def test_every_test_asserts_something_and_dead_zones_are_strict():
    """第 100 節第 1、4 條：每個 test_* 都要有 assert、pytest.raises／warns，或呼叫含 assert 的輔助函式；
    xfail 一律 strict=True（死區被修好時要變紅、逼人把標註拿掉）；不准無條件 skip。"""
    tests = Path(__file__).resolve().parent
    bad = []
    for f in sorted(tests.glob("test_*.py")):
        tree = ast.parse(_src(f))
        helpers = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and not n.name.startswith("test_")
                   and any(isinstance(x, ast.Assert) for x in ast.walk(n))}
        for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")]:
            nodes = list(ast.walk(fn))
            ok = any(isinstance(n, ast.Assert) for n in nodes) or \
                any(isinstance(n, ast.Attribute) and n.attr in ("raises", "warns") for n in nodes) or \
                any(isinstance(n, ast.Call) and getattr(n.func, "id", "") in helpers for n in nodes)
            if not ok:
                bad.append(f"{f.name}:{fn.lineno}: {fn.name} 沒有任何斷言")
        called = {id(n.func) for n in ast.walk(tree) if isinstance(n, ast.Call)}
        for n in ast.walk(tree):
            if id(n) in called:
                continue  # @pytest.mark.xfail(...) 的 func 部分；由外層的 Call 處理
            mark = n.func if isinstance(n, ast.Call) else n
            if not (isinstance(mark, ast.Attribute) and isinstance(mark.value, ast.Attribute) and mark.value.attr == "mark"):
                continue
            kw = {k.arg: k.value for k in n.keywords} if isinstance(n, ast.Call) else {}
            if mark.attr == "xfail" and not (isinstance(kw.get("strict"), ast.Constant) and kw["strict"].value is True):
                bad.append(f"{f.name}:{n.lineno}: xfail 沒有 strict=True")
            if mark.attr == "skip":
                bad.append(f"{f.name}:{n.lineno}: 無條件 skip")
    assert not bad, "\n".join(bad)
