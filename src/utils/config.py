from omegaconf import DictConfig



class config:  # noqa
    values: DictConfig = {}  # type: ignore

    @staticmethod
    def get(key: str, default=None, ignore_errors=False):        
        value = config.values
        for item in key.split("."):
            value = value.get(item)
            if value is None:
                if not ignore_errors and default is None:
                    from src.utils.logging import logger
                    logger.warning(f"Config property '{item}' not found in {key}")
                    pass
                return default

        return value
