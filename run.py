import importlib
import os
import sys

import hydra
from hydra.core.global_hydra import GlobalHydra
from omegaconf import DictConfig


def prepare():
    overrides = sys.argv[1:] + [
        f"{key[len('NEXTRADE_'):].lower().replace('__', '.')}={value}"
        for key, value in os.environ.items()
        if key.startswith("NEXTRADE_")
    ]

    if not GlobalHydra.instance().is_initialized():
        hydra.initialize(config_path="configs", version_base=None)
    conf: DictConfig = hydra.compose("config.yaml", overrides=overrides)

    from src.utils import config
    config.values = conf

    from src.utils.logging import logger, bundle
    logger.info(bundle("Preparing", overrides=overrides))

    from src.utils import telemetry
    telemetry.tracer = telemetry.initialize()


def main():
    from src.utils import config
    from src.utils.logging import logger

    module = config.get("path")
    module = module.replace(".py", "")
    module = module.replace("/", ".")
    logger.info(f"Running {module}")

    try:
        importlib.import_module(module)
    except Exception as e:
        logger.exception(e)
        raise e


if __name__ == "__main__":
    prepare()
    main()
