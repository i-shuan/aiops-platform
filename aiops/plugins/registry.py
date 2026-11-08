from aiops.plugins.base import BaseDetector


class DetectorRegistry:
    _items: dict[str, type[BaseDetector]] = {}

    @classmethod
    def register(cls, detector_cls: type[BaseDetector]) -> type[BaseDetector]:
        if not issubclass(detector_cls, BaseDetector) or not detector_cls.name:
            raise TypeError(f"{detector_cls} 必須繼承 BaseDetector 並設定 name")
        cls._items[detector_cls.name] = detector_cls
        return detector_cls

    @classmethod
    def has(cls, name: str) -> bool:
        return name in cls._items

    @classmethod
    def get(cls, name: str) -> type[BaseDetector]:
        return cls._items[name]

    @classmethod
    def names(cls) -> list[str]:
        return sorted(cls._items)
