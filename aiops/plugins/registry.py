from aiops.plugins.base import BaseDetector, BaseGrouper, BaseLLMProvider, BaseScorer, BaseSectionBuilder


class _Registry:
    """每種外掛一個 Registry；register 時嚴格檢查是否實作對應契約。"""

    base: type = object
    _items: dict[str, type]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._items = {}

    @classmethod
    def register(cls, plugin_cls: type) -> type:
        if not (isinstance(plugin_cls, type) and issubclass(plugin_cls, cls.base)) or not plugin_cls.name:
            raise TypeError(f"{plugin_cls} 必須繼承 {cls.base.__name__} 並設定 name")
        existing = cls._items.get(plugin_cls.name)
        if existing is not None and existing is not plugin_cls:
            raise TypeError(f"{cls.__name__} 已有同名外掛: {plugin_cls.name}")
        cls._items[plugin_cls.name] = plugin_cls
        return plugin_cls

    @classmethod
    def has(cls, name: str) -> bool:
        return name in cls._items

    @classmethod
    def get(cls, name: str) -> type:
        return cls._items[name]

    @classmethod
    def names(cls) -> list[str]:
        return sorted(cls._items)


class DetectorRegistry(_Registry):
    base = BaseDetector


class GrouperRegistry(_Registry):
    base = BaseGrouper


class ScorerRegistry(_Registry):
    base = BaseScorer


class SectionRegistry(_Registry):
    base = BaseSectionBuilder


class LLMRegistry(_Registry):
    base = BaseLLMProvider
