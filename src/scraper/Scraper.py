import copy
import time
from typing import List, Callable, Dict, Optional, Union, Tuple

from src.scraper.modules import ScraperModule, WatchModule, OpenModule, ReturnModule
from src.scraper.types import TickerPayload, ScraperComponent, ScraperEntity
from src.utils import config, telemetry
from src.utils.logging import logger


class ScraperPayload(TickerPayload):
    def __init__(self, payload: Dict) -> None:
        super().__init__("scraper", payload)

    payload: Dict


class Scraper:

    modules: List[Tuple[ScraperModule, Dict]] = []

    def __init__(self,
                 callback: Callable[[TickerPayload], None],
                 settings: List[ScraperComponent] = config.get("scraping.components"),
                 ):
        self.callback = callback
        self.settings = settings
        self.wait_time = config.get("scraping.wait_time", 1)

        logger.info([s["entity"]["name"] for s in settings])

        for component in settings:
            self.modules.append(self.initialize_component(component))

        i = 0
        while True:
            i += 1
            with telemetry.tracer.start_as_current_span("Batch run"):
                for i, module in enumerate(self.modules):
                    with telemetry.tracer.start_as_current_span("Module run") as module_span:
                        module_span.set_attribute("index", i)
                        module_span.set_attribute("entity", copy.deepcopy(module[1].get("entity", {}).get("name", "")))
                        result = module[0].run(copy.deepcopy(module[1]))
                        if result:
                            callback(ScraperPayload(result))

                    time.sleep(self.wait_time)

    def initialize_component(self, component: ScraperComponent) -> Tuple[ScraperModule, Dict]:
        entity: Union[ScraperEntity, Dict] = {}
        if "entity" in component:
            entity = {
                "name": component["entity"].get("name", ""),
                "ticker": component["entity"].get("ticker", ""),
            }

        modules: List[ScraperModule] = []
        previous: Optional[ScraperModule] = None
        for step in reversed(component["steps"]):
            module: Optional[ScraperModule] = None
            if step["action"] == "watch":
                module = WatchModule(previous, step, config.get("scraping.mock", False))
            elif step["action"] == "open":
                module = OpenModule(previous, step)
            elif step["action"] == "return":
                module = ReturnModule(step)

            modules.append(module)
            previous = module

        return modules[-1], {"entity": copy.deepcopy(entity)}
