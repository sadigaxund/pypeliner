from datetime import datetime
from typing import *
from functools import partial
import logging
import json
import time

MINIMUM_LOG_LEVEL = logging.DEBUG
DEFAULT_LOG_SEPERATOR = " || "
__spr__ = DEFAULT_LOG_SEPERATOR  # abbreviation

TRACE_LEVEL = logging.DEBUG + 5  # Set a custom level value
logger = None



def printlog(message: str, level: str = "info", payload: Dict = None):
    from .functions import custom_encoder

    if payload:
        message += f"{__spr__}{json.dumps(payload, ensure_ascii=False, default=custom_encoder)}"

    if logger != None:
        logfunc = {
            "debug": logger.debug,
            "trace": logger.trace,
            "warning": logger.warning,
            "error": logger.error,
            "critical": logger.critical,
            "info": logger.info
        }.get(level, logger.debug)
        logfunc(message)
    else:
        print(message)
        # raise NotImplementedError("Application Logger has not been implemented!")
        # ending = f"{__spr__}{payload}\n" if payload else '\n'
        # print(f"{level}{__spr__}{message}", end=ending)


class UnixTimestampFormatter(logging.Formatter):
    def format(self, record):
        # Add Unix timestamp to the log record
        record.unixts = int(datetime.now().timestamp() * 1000)
        return super().format(record)


def init_logger(log_file=None, log_level=MINIMUM_LOG_LEVEL, debug=False):
    global logger

    class CustomFilter(logging.Filter):
        def filter(self, record):
            # Add the custom variable to the LogRecord
            record.timestamp = int(time.time() * 1000)
            return True

    # Create a logger
    logger = logging.getLogger("Application Logger")
    
    # Close and remove each handler
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)

    
    logger.setLevel(log_level)
    # Create a formatter with a custom date format
    formatter = logging.Formatter(
        f'%(timestamp)s{__spr__}%(threadName)s{__spr__}%(name)s{__spr__}%(levelname)s{__spr__}%(message)s')

    console_handler = logging.StreamHandler()
    # Create a console handler and set the level
    console_handler.setLevel(log_level)
    # Set the formatter for the console handler
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)  # Create a file handler and set the level
    # Set the formatter for the file handler
    file_handler.setFormatter(formatter)

    # Create a custom filter and add it to the handlers
    custom_filter = CustomFilter()
    console_handler.addFilter(custom_filter)
    file_handler.addFilter(custom_filter)

    # Add both handlers to the logger
    if debug:
        logger.addHandler(console_handler)
    else:
        logger.addHandler(file_handler)

    # add a custom log level lower than info
    # Assign a name to the custom level
    logging.addLevelName(TRACE_LEVEL, "TRACE")
    # Add the custom level to the Logger class
    logger.trace = partial(logger._log, TRACE_LEVEL, args=None)
    return logger