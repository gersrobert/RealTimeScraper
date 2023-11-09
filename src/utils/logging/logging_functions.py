from src.utils.logging import initialize_logger

logger = initialize_logger()

ldebug = logger.debug
linfo = logger.info
lwarn = logger.warning
lerr = logger.error
lexception = logger.exception
lcritical = logger.critical


def bundle(message=None, **kwargs):
    def _b(**_kwargs):
        return _kwargs

    if message is not None:
        return _b(message=message, **kwargs)
    return kwargs
