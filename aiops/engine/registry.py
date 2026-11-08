"""算子註冊中心：@OperatorRegistry.register("detector.mad")。名稱由 Workflow YAML 引用。"""
from __future__ import annotations

from typing import Any, ClassVar

from aiops.engine.errors import WorkflowError


class OperatorRegistry:
    _items: ClassVar[dict[str, type]] = {}

    @classmethod
    def register(cls, name: str):
        def deco(op_cls: type) -> type:
            if not callable(getattr(op_cls, "execute", None)):
                raise TypeError(f"{op_cls.__name__} 必須實作 execute(ctx)")
            existing = cls._items.get(name)
            if existing is not None and existing is not op_cls:
                raise TypeError(f"算子名稱重複：{name}")
            op_cls.operator_name = name
            cls._items[name] = op_cls
            return op_cls

        return deco

    @classmethod
    def has(cls, name: str) -> bool:
        return name in cls._items

    @classmethod
    def get(cls, name: str) -> type:
        if name not in cls._items:
            raise WorkflowError(f"未註冊的算子：{name}（可用：{', '.join(sorted(cls._items)) or '無'}）")
        return cls._items[name]

    @classmethod
    def names(cls, prefix: str = "") -> list[str]:
        return sorted(n for n in cls._items if n.startswith(prefix))

    @classmethod
    def create(cls, name: str, params: dict[str, Any] | None = None):
        """用 YAML 的 params 實例化；拼錯的參數名稱直接報錯，不要默默忽略。"""
        op_cls = cls.get(name)
        try:
            return op_cls(**(params or {}))
        except TypeError as e:
            raise WorkflowError(f"算子 {name} 的參數不合法：{e}") from e
