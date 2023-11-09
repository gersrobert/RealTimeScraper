import json
import re
from abc import ABC, abstractmethod
from typing import Optional
from urllib.parse import urljoin

import pydash
import requests
from lxml import etree
from random_user_agent.user_agent import UserAgent

from src.scraper.modules.module import ScraperModule, ScraperModuleError
from src.scraper.types import get_target
from src.utils import config, telemetry
from src.utils.logging import logger, bundle


class Body(ABC):
    @abstractmethod
    def get(self, path: str) -> str:
        return ""


class JsonBody(Body):
    def __init__(self, response: requests.Response):
        self.parsed = json.loads(response.text.encode("ascii", "ignore").decode())

    def get(self, path):
        value = pydash.get(self.parsed, path)
        value = re.sub("<[^<]+?>", "", str(value))  # Remove possible HTML tags

        return value


class XmlBody(Body):
    def __init__(self, response: requests.Response, _type: str):
        if _type == "html":
            self.tree = etree.HTML(
                response.text[response.text.find("<html"):],
            )
        elif _type == "rss":
            self.tree = etree.XML(
                response.text[response.text.find("<rss"):],
            )
            
    def get(self, path):
        elements = self.tree.xpath(path)
        if not elements:
            return None

        parsed = ""
        for element in elements:
            if hasattr(element, "text") and element.text is not None:
                parsed += element.text + " "
            elif isinstance(element, str):
                parsed += element + " "

        if hasattr(parsed, "strip"):
            parsed = parsed.strip()

        return parsed


class OpenModule(ScraperModule):

    def __init__(self, next_step, settings):
        super().__init__(next_step, settings)

        self.user_agent = UserAgent()

        if config.get("scraping.use_sessions", True):
            self.session = requests.session()
        else:
            self.session = requests

    def run(self, store):
        with telemetry.tracer.start_as_current_span("open module"):
            logger.debug(bundle(self.__class__.__name__, settings=self.settings))

            response = self._make_request(store)
            body = self._extract_body(response)

            for key in self.settings["store"].keys():
                pydash.set_(store, key, body.get(self.settings["store"][key]))

            store["_prev"] = self.settings

        return self.next_step.run(store)

    def _make_request(self, store) -> requests.Response:
        target = "".join(get_target(self.settings["target"], store).split())
        if target is None:
            raise ScraperModuleError("Scraping target is None")

        url = urljoin(
            get_target((store.get("_prev") or {}).get("target"), store),  # type: ignore
            target,
        )
        if not url:
            url = target

        response = self.session.get(
            url,
            headers={"user-agent": config.get("scraping.user_agent", self.user_agent.get_random_user_agent())},
        )

        if response.status_code == 200:
            return response

        raise ScraperModuleError(response)

    def _extract_body(self, response: requests.Response) \
            -> Optional[Body]:
        # with telemetry.tracer.start_as_current_span("extract body"):
        body: Optional[Body] = None
        if "application/json" in response.headers["Content-Type"]:
            body = JsonBody(response)
        elif "text/html" in response.headers["Content-Type"]:
            body = XmlBody(response, "html")
        elif "application/rss+xml" in response.headers["Content-Type"]:
            body = XmlBody(response, "rss")

        return body
