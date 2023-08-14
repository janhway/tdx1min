import logging
import logging.handlers
import os
from functools import partial
from logging import Logger

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from tdx1min.tdx_cfg import WORK_DIR

SETTINGS: Dict[str, Any] = {
    "log.level": logging.DEBUG,
    "log.console": True,
    "log.file": True,
}


def get_logs_path():
    folder_path = Path(WORK_DIR)
    folder_path = folder_path.joinpath("logs")
    if not folder_path.exists():
        folder_path.mkdir()
    print("get_logs_path, current path={} log_path={}".format(Path.cwd(), folder_path))
    return folder_path


class LogEngine(object):
    """
    Processes log event and output with logging module.
    """

    def __init__(self, name="Trader", file_prefix='vt', file_name=None):
        """"""
        self.console_handler = None

        self.level: int = SETTINGS["log.level"]

        self.logger: Logger = logging.getLogger(name)
        self.logger.setLevel(self.level)

        self.formatter = logging.Formatter(
            "%(asctime)s  %(funcName)s %(lineno)d %(levelname)s: %(message)s"
        )

        self.add_null_handler()

        today_date = datetime.now().strftime("%Y-%m-%d")
        if not file_name:
            filename = f"{file_prefix}_{today_date}.log"
        else:
            filename = file_name
        log_path = get_logs_path()
        self.file_path = os.path.join(log_path, filename)

        if SETTINGS["log.console"]:
            self.add_console_handler()

        if SETTINGS["log.file"]:
            self.add_file_handler()

    def add_null_handler(self) -> None:
        """
        Add null handler for logger.
        """
        null_handler = logging.NullHandler()
        self.logger.addHandler(null_handler)

    def add_console_handler(self) -> None:
        """
        Add console output of log.
        """
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)
        self.console_handler = console_handler

    def remove_console_handler(self) -> None:
        if self.console_handler:
            self.logger.removeHandler(self.console_handler)
            self.console_handler = None

    def add_file_handler(self) -> None:
        """
        """
        # file_handler = logging.FileHandler(
        #     self.file_path, mode="a", encoding="utf8"
        # )

        # TimedRotatingFileHandler，并设置
        # when="midnight"，表示每天凌晨切换到新的日志文件。
        # interval=1 表示每天更换一次，
        # backupCount=7 表示保留最近的 7 个日志文件，以确保不会占用过多的磁盘空间。
        file_handler = logging.handlers.TimedRotatingFileHandler(
            self.file_path, when="midnight", interval=1, backupCount=7, encoding='utf-8'
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)


gLog = LogEngine(name="TdxMin", file_prefix='tdx_min')
logd = partial(gLog.logger.log, logging.DEBUG)
logi = partial(gLog.logger.log, logging.INFO)
logw = partial(gLog.logger.log, logging.WARN)
loge = partial(gLog.logger.log, logging.ERROR)
