import pydash

from src.scraper.modules.module import ScraperModule
from src.utils import telemetry
from src.utils.logging import logger, bundle


class ReturnModule(ScraperModule):

    def __init__(self, settings):
        super().__init__(None, settings)

    def run(self, store):
        with telemetry.tracer.start_as_current_span("return module"):
            logger.debug(bundle(self.__class__.__name__, settings=self.settings))

            response = {"entity": store["entity"]}
            for key in self.settings["store"].keys():
                pydash.set_(
                    response,
                    key,
                    pydash.get(store, self.settings["store"][key]),
                )

            store["_prev"] = self.settings
            return response
