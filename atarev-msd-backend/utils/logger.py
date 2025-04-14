from inspect import getframeinfo, stack
from multiprocessing import Queue

import dotenv
import flask
import logging
import logging_loki
import os
import socket
import time
import uuid
from flask import g

dotenv.load_dotenv()

# cache host name
hostname = socket.gethostname()


def _configure_loggers():
    """Initialize python logging, should be called once to avoid performance issues"""

    def _add_file_logger(logger):
        log_folder = str(os.getenv("LOG_FOLDER")) if os.getenv("LOG_FOLDER") is not None else os.getcwd()
        log_filename = str(os.getenv("LOG_FILENAME")) if os.getenv("LOG_FILENAME") is not None else "log.txt"
        if not os.path.exists(log_folder):
            os.mkdir(log_folder)
        # logs_file = os.path.join(log_folder, log_filename)
        formatter = logging.Formatter(
            "%(asctime)s | %(app)s | %(env)s | %(hostname)s | %(name)s \t| %(file_name)s \t| %(line_number)d \t| %(threadName)s \t|  %(correlationID)s | %(levelname)s \t| %(message)s"
        )
        rotating_log_handler = logging.handlers.RotatingFileHandler(log_filename, mode=str, maxBytes=1024 * 1024 * 10,
                                                                    backupCount=5)
        rotating_log_handler.setFormatter(formatter)
        print("File handler created")
        logger.addHandler(rotating_log_handler)

    def _add_loki_logger(logger):
        loki_handler = logging_loki.LokiQueueHandler(
            Queue(-1),
            url=f"{str(os.getenv('LOG_LOKI_URL'))}/loki/api/v1/push",
            version="1",
            auth=(str(os.getenv('LOG_LOKI_USERNAME')), str(os.getenv('LOG_LOKI_PASSWORD'))),
        )
        print("Loki handler created")
        logger.addHandler(loki_handler)

    if os.getenv("ENV_NAME") is None or os.getenv("APP_NAME") is None:
        raise ValueError(f"Missing required environment variables ENV_NAME and APP_NAME")
    root_logger = logging.getLogger(os.getenv("APP_NAME"))
    # clear old handlers
    root_logger.handlers.clear()
    try:
        # init file logging if its enabled by config
        if str(os.getenv("LOG_TO_FILE_ENABLED")) == "True":
            _add_file_logger(root_logger)
        else:
            print("File logging is disabled")
    except Exception as e:
        print(f"Exception while initializing file logging : {e}")

    try:
        if str(os.getenv("LOG_LOKI_LOGGING_ENABLED")) == "True":
            _add_loki_logger(root_logger)
        else:
            print("Grafana logging is disabled")
    except Exception as e:
        print(f"Exception while initializing file logging : {e}")

    log_level = 20
    try:
        log_level = int(os.getenv("LOG_LEVEL"))
    except:
        pass

    # set root logger level threshold (lower levels will not be logged)
    root_logger.setLevel(log_level)
    root_logger.info("Root logger initialized")
    print(
        f'Logging configured, file logging:{os.getenv("LOG_LOKI_LOGGING_ENABLED")} [{os.getenv("LOG_FOLDER")},{os.getenv("LOG_FILENAME")}], Grafana logging:{os.getenv("LOG_LOKI_LOGGING_ENABLED")} [{os.getenv("LOG_LOKI_URL")}], performance logging:{os.getenv("LOG_PERFORMANCE_MEASURE_ENABLED")}')


def measure_time(*args, **kwargs):
    """annotation to easily measure method execution time"""
    if 'message' not in kwargs:
        raise ValueError("Missing required parameter 'message' in @measure_time decorator")
    message = message = kwargs['message']

    # method = args[0]
    def deco(method):
        def timed(*args, **kw):
            if str(os.getenv("LOG_PERFORMANCE_MEASURE_ENABLED")) != "True":
                try:
                    return method(*args, **kw)
                except Exception as e:
                    print(f"Got An ERROR, error message:{e}")
                    raise e
            else:
                logger = Logger("performance")
                func_name = method.__name__
                ts = time.time()
                try:
                    result = method(*args, **kw)
                    te = time.time()
                    execution_time_millis = int((te - ts) * 1000)
                    labels = {
                        'func_name': func_name,
                        'execution_time': execution_time_millis
                    }
                    logger.info(
                        f"{message}, function:[{func_name}], execution time:[{execution_time_millis}]", labels)
                except Exception as e:
                    te = time.time()
                    execution_time_millis = int((te - ts) * 1000)
                    labels = {
                        'func_name': func_name,
                        'execution_time': execution_time_millis,
                        'error_occurred': True,
                        'exception': e
                    }
                    logger.error(
                        f"{message}, :[{func_name}], failed to measure execution time, exception:{e}, execution time:[{execution_time_millis}]",
                        labels)
                    raise e
                return result
            # return method(*args, **kw)

        return timed

    return deco


class Logger:
    """Logging logic wrapper. It can log to a file or grafana (if enabled)."""
    _is_configured = False
    _instances = dict()  # key = logger name, value=instance of Logger class

    def __new__(class_, logger_name):
        # if logger_name is None:
        #    logger_name = str(os.getenv("APP_NAME"))
        # else:
        if logger_name != str(os.getenv('APP_NAME')):
            logger_name = f"{str(os.getenv('APP_NAME'))}.{logger_name}"

        # print(f"Logger::__new__ {logger_name}")
        # we want to make sure only one instance of a logger with a logger_name exists (to avoid expensive initialization)
        if logger_name not in Logger._instances:
            Logger._instances[logger_name] = object.__new__(class_)
        return Logger._instances[logger_name]

    def __init__(self, logger_name):
        if logger_name != str(os.getenv('APP_NAME')):
            logger_name = f"{str(os.getenv('APP_NAME'))}.{logger_name}"
        self.logger = logging.getLogger(logger_name)

    def _get_request_context(self):
        context = {
            'request_start': time.time(),
            'correlationID': str(uuid.uuid4())
        }
        try:
            # check if we already have context initialized in flask 'g' (which is global dict for request scope duration)
            if "atarev_context" not in g:
                # it was not yet there - initialize it with current time and new correlationID
                flask.g.atarev_context = context
            else:
                context = flask.g.atarev_context
        except Exception as e:
            # do nothing here - this means we are not in flask context (e.g. standalone application and flask.g does not exist)
            pass
        return context

    def __log(self, message, level=logging.INFO, customlabels: dict = None):
        context = self._get_request_context()
        now = time.time()
        caller = getframeinfo(stack()[2][0], 1)
        labels = {
            "file_name": caller.filename.split(os.path.sep)[-1],  # get file name only (no path)
            "line_number": caller.lineno,
            "env": str(os.getenv("ENV_NAME")),
            "app": str(os.getenv("APP_NAME")),
            "request_start": context['request_start'],
            "request_incremental_duration": (now - int(context['request_start'])) * 1000,
            "correlationID": context['correlationID'],
            "hostname": hostname,
            "level": level}
        if customlabels is not None:
            labels = {**labels, **customlabels}
        labels["tags"] = labels.copy()  # this is needed for grafana
        try:
            self.logger.log(level=level, msg=message, extra=labels)
        except Exception as e:
            print(f"Exception on message:{message}")

    def debug(self, message, customlabels: dict = None):
        self.__log(message, logging.DEBUG, customlabels)

    def info(self, message, customlabels: dict = None):
        self.__log(message, logging.INFO, customlabels)

    def warning(self, message, customlabels: dict = None):
        self.__log(message, logging.WARN, customlabels)

    def error(self, message, customlabels: dict = None):
        self.__log(message, logging.ERROR, customlabels)


# create root logger
_configure_loggers()
Logger(str(os.getenv("APP_NAME")))
