import json
from typing import List


# 检查结论是：
# TDX用结束时间标识K线，在不同时间段查询5分钟K线时返回的情况：
# 09:35之前去查询K线，都会返回09:35标识的K线  "datetime": "2023-08-11 09:35"
# [0925-0930)之间的开盘价等于收盘价格 准确的是[0925,092959)之间
# [0925-0930)和[0930-0935) 的开盘价相等
# 0925之前查询时返回的开盘价等于昨天的收盘价格
def check_open_price():
    with open(r'E:\chenzhenwei\PycharmProjects\quant\tdx1min\.workdir\logs\barsx_20230811_085104.txt', 'r') as fp:
        lines = fp.readlines()
    dataslt0925 = {}
    datas0925 = {}
    datas0930 = {}
    for line in lines:
        l = line.strip()
        if not l:
            continue
        d: dict = json.loads(l)
        hm = d['now'][8:12]
        hms = d['now'][8:]
        if not (hm < '0935'):
            continue

        for code in d.keys():
            if code == 'now':
                continue
            code_it: List = d[code]
            found = None
            for k_it in code_it:
                if k_it["datetime"] == "2023-08-11 09:35":
                    found = k_it
                    break
            assert(found is not None)

            # 0925之前查询时返回的开盘价等于昨天的收盘价格
            if hm < '0925':
                found2 = None
                for k_it in code_it:
                    if k_it["datetime"] == "2023-08-10 15:00":
                        found2 = k_it
                        break
                assert found2
                assert found2['close'] == found['open']

            if hm < '0925':
                t = dataslt0925
            elif '0925' <= hm < '0930':
                t = datas0925
            else:
                assert '0930' <= hm < '0935'
                t = datas0930

            if code not in t:
                t[code] = found['open']
            else:
                # open price不会变化
                assert t[code] == found['open']
            if hms < '092959':
                # [0925-0930)之间的开盘价等于收盘价格 准确的是[0925,092959)之间
                assert found['open'] == found['close'], print(code, d)

    assert set(dataslt0925.keys()) == set(datas0925.keys())
    assert set(datas0925.keys()) == set(datas0930.keys())

    for code in datas0925:
        # [0925-0930)和[0930-0935) 的开盘价相等
        assert datas0925[code] == datas0930[code]

    print("dataslt0925 open=",dataslt0925)
    print("datas0925 open=",datas0925)
    print("datas0930 open=",datas0930)


if __name__ == '__main__':
    check_open_price()
