import logging
import sys

from .colored_logger import ColoredLogger


def getlogger(name: str, level=logging.INFO) -> logging.Logger:
    return ColoredLogger(name, logging.StreamHandler(sys.stderr), level)
