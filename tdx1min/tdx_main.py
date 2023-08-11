import multiprocessing
import datetime
import sys
import time

from tdx1min.tdx_bars import check_run_period, tdx_bar_main
from tdx1min.vnlog import logi


def stg_main():
    """
    Running in the parent process.
    """
    logi("start father process")

    # 检查是否提供了足够的参数
    if len(sys.argv) < 2:
        logi("Usage: python script_name.py arg1 arg2 ...")
        sys.exit(1)

    # 获取命令行参数
    script_name = sys.argv[0]
    args = sys.argv[1:]

    # 打印脚本名
    logi("Script name:{}".format(script_name))

    # 打印传递的参数
    logi("Arguments:{}".format(args))

    stgtrd_cfg_path = sys.argv[1]
    output_path = None
    if len(sys.argv) >= 3:
        output_path = sys.argv[2]

    logi("stgtrd_cfg_path={} output_path={}".format(stgtrd_cfg_path, output_path))

    child_process = None

    try:
        while True:
            trading = check_run_period()

            # Start child process in trading period
            if trading and child_process is None:
                logi("start child")
                child_process = multiprocessing.Process(target=tdx_bar_main, args=(stgtrd_cfg_path, output_path))
                child_process.start()
                logi("start child ok")

            # 非记录时间则退出子进程
            if not trading and child_process is not None:
                if not child_process.is_alive():
                    child_process = None
                    logi("child stop ok")

            time.sleep(5)
    except KeyboardInterrupt:
        logi("receive KeyboardInterrupt")
    finally:
        if child_process is not None:
            child_process.terminate()
            child_process.join()
            logi("finish waiting child")
        logi("main process quit.")


if __name__ == "__main__":
    stg_main()
