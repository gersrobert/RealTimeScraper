from src.utils.logging import linfo
from src.scraper import Scraper


def callback(payload):
    linfo(payload.payload)

   
Scraper(callback)
