from aiops.commands.base import BaseCommand
from aiops.plugins.registry import DetectorRegistry


class ListDetectorsCommand(BaseCommand):
    def validate(self) -> None:
        pass

    def run(self) -> list[str]:
        return DetectorRegistry.names()
