import os
import logging
from logging.handlers import RotatingFileHandler
from config import LOG_DIR, LOG_FILE

def get_logger(name: str) -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        handler = RotatingFileHandler(
            os.path.join(LOG_DIR, LOG_FILE),
            maxBytes=5 * 1024 * 1024,   # 5 MB
            backupCount=5               # keep 5 logs
        )
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger
