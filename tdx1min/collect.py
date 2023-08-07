import os
import threading
import time

from datetime import datetime
from pathlib import Path
from threading import Thread
from typing import Any, Dict, List

from src_stg.tick_cfg import WORK_DIR


def get_path(path):
    folder_path = Path(WORK_DIR)
    folder_path = folder_path.joinpath(path)
    if not folder_path.exists():
        folder_path.mkdir()

    print("get_path, current path={} use_path={}".format(Path.cwd(), folder_path))
    return folder_path.__str__()


class CollectEngine(object):
    """
    Processes log event and output with logging module.
    """

    def __init__(self, filename: str):
        """"""
        self.thread: Thread = Thread(target=self.run)
        self.queue: List[str] = []
        self.lock = threading.Lock()
        self.active: bool = False

        self.filename = filename
        self.fp = None
        self.wait_num = 0

    def log(self, msg: str) -> None:
        """"""
        # Start email engine when sending first email.
        if not self.active:
            self.start()
        self.lock.acquire()
        try:
            self.queue.append(msg)
        finally:
            self.lock.release()

    def run(self) -> None:
        """"""
        while self.active:
            try:
                if self.queue:
                    self.lock.acquire()
                    try:
                        que = self.queue
                        self.queue = []
                    finally:
                        self.lock.release()

                    if not que:
                        self.fp.flush()
                        self.wait_num = 0

                    start = time.time()
                    for m in que:
                        self.fp.write(m + "\n")
                    self.wait_num += len(que)
                    if self.wait_num > 50:
                        self.fp.flush()
                        self.wait_num = 0
                    spent = time.time() - start
                    print("write num={} spent={}".format(len(que), round(spent, 3)))
                else:
                    self.fp.flush()
                    self.wait_num = 0
                    time.sleep(0.5)

            except Exception as e:
                print(e)

    def start(self) -> None:
        """"""
        filepath = os.path.join(get_path("log"), self.filename)
        self.fp = open(filepath, "a")
        self.active = True
        self.thread.start()

    def close(self) -> None:
        """"""
        if not self.active:
            return

        self.active = False
        self.fp.close()
        self.thread.join()
