import datetime
import logging
import random
import time
import json
from functools import partial
from typing import List

from pytdx.hq import TdxHq_API
from tdx1min.api_pool import ApiPool
from tdx1min.collect import CollectEngine
from tdx1min.vnlog import LogEngine, loge

HOST = "110.41.147.114"


# 0 5分钟K线
# 1 15分钟K线
# 2 30分钟K线
# 3 1小时K线
# 4 日K线
# 5 周K线
# 6 月K线
# 7 1分钟
# 8 1分钟K线
# 9 日K线


def tst_collect_bars():
    api = TdxHq_API(auto_retry=True)
    mcodes = [(0, "000004"), (0, '300904'), (1, '600903')]
    dt = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    coll: CollectEngine = CollectEngine(f"barsx_{dt}.txt")

    today_date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"barx_{today_date}.log"
    # l:LogEngine = LogEngine(filename)
    # logd = partial(l.logger.log, logging.DEBUG)
    with api.connect(ip=HOST, port=7709, time_out=60):
        while 1:
            data = api.get_security_bars_x(0, mcodes, 0, 2)
            data['now'] = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            coll.log(json.dumps(data))
            # logd(json.dumps(data))
            time.sleep(10)


def tst_collect_bars2():
    api_pool: ApiPool = ApiPool(20)
    api_pool.start()

    mcodes = [(0, "000004"), (0, '300904'), (1, '600903')]
    dt = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    coll: CollectEngine = CollectEngine(f"barsx_{dt}.txt")

    while True:
        pool: List[TdxHq_API] = api_pool.alloc_api(5)
        try:
            for x in pool:
                data = x.get_security_bars_x(0, mcodes, 0, 2)
                if data:
                    data['now'] = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                    coll.log(json.dumps(data))
                time.sleep(5)
        except Exception as e:
            loge("exception {}".format(e))
        finally:
            api_pool.release_api()
        time.sleep(random.randint(5, 20))


if __name__ == '__main__':
    tst_collect_bars2()
