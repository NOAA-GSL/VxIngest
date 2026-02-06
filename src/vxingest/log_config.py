import logging
import logging.handlers
import os
from multiprocessing import Queue
from pathlib import Path


def get_logformat() -> logging.Formatter:
    """A function to get a common log formatter"""
    return logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] <%(processName)s> (%(name)s): %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",  # ISO 8601
    )


def get_loglevel():
    """A function to get a common loglevel"""
    return logging.DEBUG if os.environ.get("DEBUG", False) else logging.INFO


def add_logfile(
    ql: logging.handlers.QueueListener, logpath: Path
) -> logging.FileHandler:
    """Adds a new logfile to the root logger, and updates the given QueueListener to log to it

    returns a FileHandler so that it can be removed if desired"""
    # Add a logging file handler with a unique name for just this job
    root_logger = logging.getLogger()
    # Set log format & log level
    log_format = get_logformat()
    level = get_loglevel()
    f_handler = logging.FileHandler(logpath)
    f_handler.setLevel(level)
    f_handler.setFormatter(log_format)
    root_logger.addHandler(f_handler)
    ql.handlers = ql.handlers + (f_handler,)
    return f_handler


def remove_logfile(
    filehandler: logging.FileHandler, ql: logging.handlers.QueueListener
) -> None:
    """Removes the given logging.FileHandler from the root logger and queue listener"""
    root_logger = logging.getLogger()
    root_logger.removeHandler(filehandler)
    queue_handlers = list(ql.handlers)
    queue_handlers.remove(filehandler)
    ql.handlers = tuple(queue_handlers)
    filehandler.close()


def configure_logging(
    queue: Queue, logpath: Path | None = None
) -> logging.handlers.QueueListener:
    """Configure the root logger so all subsequent loggers inherit this config

    By default, log INFO level messages. However, log DEBUG messages if DEBUG is set in
    the environment. This configuration creates a default handler to log messages to
    stderr. If given a filepath, it will also log messages to the given file.

    Logging can be done in other modules by calling:
      `logger = logging.getLogger(__name__)`
    and then logging with logger.info(), etc...

    Returns a QueueListener so that the caller can call QueueListener.stop()
    """
    # Set log format & log level
    log_format = get_logformat()
    level = get_loglevel()

    # Get the root logger and set the global log level - handlers can only accept levels that are higher than the root
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Create the handler for stderr with the same log level
    c_handler = logging.StreamHandler()
    c_handler.setLevel(level)
    c_handler.setFormatter(log_format)
    # Add handlers to the logger
    root_logger.addHandler(c_handler)

    # Create a file logger and required directory if we have a logpath
    f_handler = None
    if logpath:
        logpath.parent.mkdir(parents=True, exist_ok=True)
        f_handler = logging.FileHandler(logpath)
        f_handler.setLevel(level)
        f_handler.setFormatter(log_format)
        root_logger.addHandler(f_handler)

    if f_handler:
        ql = logging.handlers.QueueListener(
            queue, c_handler, f_handler, respect_handler_level=True
        )
    else:
        ql = logging.handlers.QueueListener(
            queue, c_handler, respect_handler_level=True
        )
    ql.start()

    return ql


def worker_log_configurer(queue):
    """Configures logging for multiprocess workers

    Needs to be called at the start of the worker's Process.Run() method"""
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(
        logging.DEBUG
    )  # Send everything to the log queue and let the root logger filter
