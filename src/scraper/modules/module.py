from abc import ABC, abstractmethod
from typing import Dict, Union

from src.scraper.types import ScraperStep


class ScraperModule(ABC):

    def __init__(self, next_step: Union["ScraperModule", None], settings: ScraperStep):
        self.next_step = next_step
        self.settings = settings

    @abstractmethod
    def run(self, store: Dict) -> Dict:
        raise NotImplementedError()


class ScraperModuleError(Exception):
    pass
