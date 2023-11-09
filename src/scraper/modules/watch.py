from typing import Dict

import pydash

from src.scraper.modules.open import OpenModule
from src.utils import telemetry
from src.utils.logging import logger, bundle


class WatchModule(OpenModule):

    state: Dict = {}

    def __init__(self, next_step, settings, mock=False):
        super().__init__(next_step, settings)

        self.mock = mock

        body = self._extract_body(self._make_request({}))
        for element in settings["target"]["elements"]:
            self.state[element] = body.get(element)
            pass

    def run(self, store):
        with telemetry.tracer.start_as_current_span("watch module") as span:
            logger.debug(bundle(self.__class__.__name__, settings=self.settings))

            body = self._extract_body(self._make_request(store))
            for element in self.settings["target"]["elements"]:
                if self.mock or self.state[element] != body.get(element):
                    for key in ((self.settings["store"] or {}).keys()):
                        pydash.set_(store, key, body.get(self.settings["store"][key]))

                    for e in self.settings["target"]["elements"]:
                        self.state[e] = body.get(e)

                    store["_prev"] = self.settings

                    span.end()
                    return self.next_step.run(store)

            return {}
