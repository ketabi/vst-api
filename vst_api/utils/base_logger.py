# base_logger.py

import logging

logger = logging
logger.basicConfig(
    filename="logs/app.log",
    format="%(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# logger.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
