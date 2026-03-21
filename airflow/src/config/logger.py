import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

class BangkokFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, ZoneInfo("Asia/Bangkok"))
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)

        formatter = BangkokFormatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.propagate = False

    return logger