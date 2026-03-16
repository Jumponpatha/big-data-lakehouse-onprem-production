"""Logging configuration."""
import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """Get configured logger."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)

        # Prevent duplicate logs
        logger.propagate = False
    return logger