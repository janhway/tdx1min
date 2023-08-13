import logging
import os
from functools import partial
from logging import Logger

from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from typing import Any, Dict

from tdx1min.tdx_cfg import WORK_DIR

SETTINGS: Dict[str, Any] = {
    "log.active": True,
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

    def __init__(self, file_name=None):
        """"""
        if not SETTINGS["log.active"]:
            return
        self.thread: Thread = Thread(target=self.run)
        self.queue: Queue = Queue()
        self.active: bool = False
        self.console_handler = None

        self.level: int = SETTINGS["log.level"]

        self.logger: Logger = logging.getLogger("Trader")
        self.logger.setLevel(self.level)

        self.formatter = logging.Formatter(
            "%(asctime)s  %(funcName)s %(lineno)d %(levelname)s: %(message)s"
        )

        self.add_null_handler()

        today_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not file_name:
            filename = f"vt_{today_date}.log"
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
        Add file output of log.
        """
        # today_date = datetime.now().strftime("%Y%m%d")
        # filename = f"vt_{today_date}.log"
        # log_path = get_logs_path()
        # file_path = os.path.join(log_path, filename)

        file_handler = logging.FileHandler(
            self.file_path, mode="a", encoding="utf8"
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def log(self, level: int, msg: str) -> None:  # 没有使用这个机制
        """"""
        # Start email engine when sending first email.
        if not self.active:
            self.start()

        self.queue.put((level, msg))

    def run(self) -> None:
        """"""
        while self.active:
            try:
                log = self.queue.get(block=True, timeout=1)
                # print(log)
                self.logger.log(log[0], log[1])
            except Empty:
                pass

    def start(self) -> None:
        """"""
        self.active = True
        self.thread.start()

    def close(self) -> None:
        """"""
        if not self.active:
            return

        self.active = False
        self.thread.join()


gLog = LogEngine()
logd = partial(gLog.logger.log, logging.DEBUG)
logi = partial(gLog.logger.log, logging.INFO)
logw = partial(gLog.logger.log, logging.WARN)
loge = partial(gLog.logger.log, logging.ERROR)
