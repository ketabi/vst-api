import logging
import os

from vst_api import settings

logger = logging.getLogger(settings.app_name)

logger.setLevel(logging.INFO)


def apply_logger_handlers() -> None:
    """Apply generic configuration of logger."""
    # create handlers
    c_handler = logging.StreamHandler()

    file = f'./logs/{settings.app_name}.log'
    os.makedirs(os.path.dirname(file), exist_ok=True)
    f_handler = logging.FileHandler(file)

    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.INFO)

    # create formatters and add it to handlers
    formatter = logging.Formatter(
        "%(asctime)s - [%(filename)s:%(funcName)s()] %(levelname)s %(message)s"
    )
    c_handler.setFormatter(formatter)
    f_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)


apply_logger_handlers()