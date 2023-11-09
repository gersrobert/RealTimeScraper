import json
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from google.cloud import logging as gcp_logging   # type: ignore
from google.oauth2 import service_account  # type: ignore
from google.cloud.logging.handlers import CloudLoggingHandler  # type: ignore

from src import ROOT_PATH
from src.utils.config import config


class CustomConsoleFormatter(logging.Formatter):
    grey = "\x1b[1;37m"
    white = "\x1b[1m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    style = "%(levelname)-8s [%(asctime)s] %(name)s (%(filename)s:%(funcName)s:%(lineno)d) -> %(message)s"

    FORMATS = {
        logging.DEBUG: grey + style + reset,
        logging.INFO: white + style + reset,
        logging.WARNING: yellow + style + reset,
        logging.ERROR: red + style + reset,
        logging.CRITICAL: bold_red + style + reset,
    }

    def format(self, record):  # noqa: A003
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class CustomFileFormatter(logging.Formatter):
    def format(self, record):  # noqa: A003
        log_fmt = "%(levelname)s\t[%(asctime)s] %(filename)s:%(funcName)s:%(lineno)d %(message)s"
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class CustomCloudFormatter(logging.Formatter):
    def format(self, record):  # noqa: A003
        log_fmt = "%(message)s"
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def initialize_logger(name=None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.NOTSET)
    logger.handlers = []
    logger.propagate = False

    if config.get("logging.console.level") != "DISABLED":
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(CustomConsoleFormatter())
        console_handler.setLevel(config.get("logging.console.level"))
        logger.addHandler(console_handler)

    if config.get("logging.file.level") != "DISABLED":
        path = os.path.join(ROOT_PATH, "logs")
        os.makedirs(path, exist_ok=True)
        file_handler = TimedRotatingFileHandler(os.path.join(path, "nextrade.log"),
                                                when="D", interval=1, backupCount=90)
        file_handler.setFormatter(CustomFileFormatter())
        file_handler.setLevel(config.get("logging.file.level"))
        logger.addHandler(file_handler)

    if config.get("logging.cloud.level") != "DISABLED" \
            and config.get("secret.gcp.logging.credentials", ignore_errors=True) is not None:
        logging.getLogger("google.cloud.logging_v2.handlers.transports.background_thread").setLevel(logging.WARNING)
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(str(config.get("secret.gcp.logging.credentials"))),
        )
        cloud_logging_client = gcp_logging.Client(credentials=credentials)
        cloud_logging_handler = CloudLoggingHandler(
            cloud_logging_client, name=name,
        )
        cloud_logging_handler.setFormatter(CustomCloudFormatter())
        cloud_logging_handler.setLevel(config.get("logging.cloud.level"))
        logger.addHandler(cloud_logging_handler)

    return logger
