"""Q8：可變狀態掃描（補 tests/test_architecture.py 的 test_no_module_or_class_level_state_is_mutated_at_runtime 沒看的那一層）。

架構測試只看「模組層級／類別層級」的可變物件被執行期修改。這裡另外看：
  A. 每個類別裡、除了 __init__ 以外的方法對 self.<屬性> 的賦值或原地修改（append／update／[k]=…／+=）
     ——算子實例在 Pipeline.__init__ 建一次，常駐 loop（interfaces/cli）裡 Pipeline 是否每輪重建決定這些會不會跨輪殘留
  B. global 陳述式
  C. 模組層級的 ContextVar／lazy singleton
用法：python scripts/probe/q8_mutable_state.py [--root <aiops 原始碼根>]
"""
from __future__ import annotations

import ast
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _common import use_root  # noqa: E402

MUTATORS = {"append", "extend", "update", "add", "insert", "setdefault", "pop", "clear", "remove", "discard"}


def self_attr(node: ast.AST) -> str | None:
    while isinstance(node, ast.Subscript):
        node = node.value
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "self":
        return node.attr
    return None


def main() -> int:
    root = use_root(sys.argv)
    hits = []
    globals_ = []
    for path in sorted((root / "aiops").rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for n in ast.walk(tree):
            if isinstance(n, ast.Global):
                globals_.append(f"{rel}:{n.lineno} global {', '.join(n.names)}")
        for cls in [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]:
            for fn in [m for m in cls.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))]:
                if fn.name in ("__init__", "__post_init__") or fn.name.startswith("load_state") or fn.name in (
                        "fit", "load_artifact"):
                    continue  # 建構、還原、訓練（在每輪新建的 algo 實例上）：不算跨輪狀態
                for node in ast.walk(fn):
                    targets = []
                    if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
                        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in MUTATORS:
                        targets = [node.func.value]
                    for t in targets:
                        for el in (t.elts if isinstance(t, ast.Tuple) else [t]):
                            attr = self_attr(el)
                            if attr:
                                hits.append(f"{rel}:{node.lineno} {cls.name}.{fn.name} -> self.{attr}")
    print("A. 非 __init__／fit／load_state 方法裡改 self 屬性：")
    for h in sorted(set(hits)):
        print("  ", h)
    print("B. global：")
    for g in globals_:
        print("  ", g)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
