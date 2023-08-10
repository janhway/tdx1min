import datetime
import time
import json
from pytdx.hq import TdxHq_API
from tdx1min.collect import CollectEngine

HOST = "110.41.147.114"


def tst_collect_bars():
    api = TdxHq_API()
    mcodes = [(0, "000004"), (0, '300904'), (1, '600903')]
    coll: CollectEngine = CollectEngine("barsx.txt")
    with api.connect(ip=HOST, port=7709, time_out=60):
        while 1:
            data = api.get_security_bars_x(8, mcodes, 0, 2)
            data['now'] = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            coll.log(json.dumps(data))
            time.sleep(15)


if __name__ == '__main__':
    tst_collect_bars()
