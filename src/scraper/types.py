from typing import TypedDict, List, Literal, Dict, Optional, Any

import pydash


class TickerPayload(object):
    source: str
    payload: Any

    def __init__(self, source: str, payload) -> None:
        self.source = source
        self.payload = payload


class ScraperTarget(TypedDict):
    type: Literal["url", "store"]  # noqa: A003
    value: str
    elements: Optional[List[str]]


class ScraperStep(TypedDict):
    action: Literal["watch", "open", "return"]
    target: ScraperTarget
    store: Dict[str, str]


class ScraperEntity(TypedDict):
    ticker: str
    name: str


class ScraperComponent(TypedDict):
    entity: ScraperEntity
    steps: List[ScraperStep]


def get_target(target: ScraperTarget, store: Dict[str, str]) -> Optional[str]:
    if target is not None:
        if target["type"] == "url":
            return target["value"]
        if target["type"] == "store":
            return pydash.get(store, target["value"])

    return None
