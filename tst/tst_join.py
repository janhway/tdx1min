import threading
import time


def thread_with_exception():
    print("Thread A started")
    time.sleep(2)
    print("Thread A raising exception")
    raise Exception("Exception in Thread A")
    print("Thread A finished")


# 创建一个线程并启动
thread_a = threading.Thread(target=thread_with_exception)
thread_a.start()

time.sleep(5)
print("Main thread waiting for Thread A")
thread_a.join()  # 在这里可能会抛出异常
print("Main thread finished")
