import sys
import threading
import time
import traceback

from tdx1min.tdx_bars import check_run_period, tdx_bar_main, set_run_state
from tdx1min.vnlog import logi, loge


def stg_main():

    logi("start main thread.")

    # 检查是否提供了足够的参数
    # if len(sys.argv) < 2:
    #     logi("Usage: python script_name.py arg1 arg2 ...")
    #     sys.exit(1)

    # 获取命令行参数
    script_name = sys.argv[0]
    args = sys.argv[1:]

    logi("script name:{} args:{}".format(script_name, args))

    stgtrd_cfg_path = None
    output_path = None
    if len(sys.argv) >= 2:
        stgtrd_cfg_path = sys.argv[1]
    if len(sys.argv) >= 3:
        output_path = sys.argv[2]

    logi("stgtrd_cfg_path={} output_path={}".format(stgtrd_cfg_path, output_path))

    child_thread = None
    set_run_state(True)

    try:
        while True:
            trading = check_run_period()

            # Start child process in trading period
            if trading:
                if child_thread is None:
                    logi("start child")
                    # 创建线程并设置为守护线程
                    child_thread = threading.Thread(target=tdx_bar_main, args=(stgtrd_cfg_path, output_path))
                    child_thread.daemon = True
                    child_thread.start()
                    logi("start child ok")
                elif not child_thread.is_alive():
                    loge("child_thread {} quit unexpectedly.".format(child_thread.name))
                    child_thread = None
            elif not trading:
                if child_thread and not child_thread.is_alive():
                    child_thread = None
                    logi("child stop ok")

            time.sleep(5)
    except KeyboardInterrupt:
        logi("receive KeyboardInterrupt")
    except Exception as e:
        error_message = traceback.format_exc()
        loge("Exception {}".format(error_message))
    finally:
        if child_thread is not None:
            set_run_state(False)
            # child_thread.join()  daemon不需要join
            logi("finish waiting child")
        logi("main process quit.")


if __name__ == "__main__":
    stg_main()
