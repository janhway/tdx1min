import datetime
import sys


def read_his_dates(fn=None):
    his_dates = []
    if fn is None:
        fn = r"c:\ftp\params\Stgtrd_M5.csv"

    with open(fn, "r") as fp:
        lines = fp.readlines()
    for line in lines:
        l = line.strip()
        if not l:
            continue

        tmp = l.split(',')
        if l.startswith('code'):
            continue

        if tmp[6] not in his_dates:
            his_dates.append(tmp[6])
    return his_dates


def read_o_stg(dt, fn=None):
    if fn is None:
        fn = r"c:\ftp\params\Stgtrd_M5.csv"

    mp = {}
    with open(fn, "r") as fp:
        lines = fp.readlines()
        titles = None
    for line in lines:
        l = line.strip()
        if not l:
            continue

        tmp = l.split(',')
        if l.startswith('code'):
            titles = tmp
            continue

        slot = tmp[1]
        if slot[0:8] != dt:
            continue

        mp[tmp[1]] = {"op": tmp[2], "cp": tmp[5]}

    print("read_o_stg", mp)
    return mp


def compare_ovo_stg(dt, fn1, fn2):
    # dt = "20230815"
    d0815 = read_o_stg(dt, fn=fn1)
    d0816 = read_o_stg(dt, fn=fn2)
    return compare_stg_v(dt, d0815, d0816)


def read_m_stg(dt):
    fn = r"c:\ftp\stg\stg_" + dt + ".csv"

    mp = {}
    with open(fn, "r") as fp:
        lines = fp.readlines()
        titles = None
    for line in lines:
        l = line.strip()
        if not l:
            continue

        tmp = l.split(',')
        if l.startswith('Code'):
            titles = tmp
            continue

        mp[tmp[3]] = {"op": tmp[1], "cp": tmp[2]}

    print("read_m_stg", mp)
    return mp


def cdiff(f1, f2):
    diff = abs(f1 - f2) / f1
    # diff = round(diff, 6)
    # print(diff)
    return diff


def compare_stg(dt):
    m = read_m_stg(dt)
    o = read_o_stg(dt)
    compare_stg_v(dt, m, o)


def compare_stg_v(dt, one, another):
    # dt = '20230815'
    m = one
    o = another
    assert len(m.keys()) == len(o.keys())
    total_count = 2 * len(m.keys())
    count = 0
    least_diff = 0.0001

    max_diff = 0
    o_price = 0
    m_price = 0
    slot = None
    for k in m:
        diff = cdiff(float(m[k]['op']), float(o[k]['op']))
        if diff > least_diff:
            print("open price unequal slot={} my_op={} th_op={} diff={}".format(k, m[k]['op'], o[k]['op'], diff))
            count += 1
        if diff > max_diff:
            max_diff = diff
            o_price = float(o[k]['op'])
            m_price = float(m[k]['op'])
            slot = k

        diff = cdiff(float(m[k]['cp']), float(o[k]['cp']))
        if diff > least_diff:
            print("clos price unequal slot={} my_op={} th_op={} diff={}".format(k, m[k]['op'], o[k]['op'], diff))
            count += 1
        if diff > max_diff:
            max_diff = diff
            o_price = float(o[k]['cp'])
            m_price = float(m[k]['cp'])
            slot = k

    print("total_count={} mis_count={} max_diff={} one_price={} another_price={} slot={}"
          .format(total_count, count, max_diff, m_price, o_price, slot))
    return max_diff, slot, m_price, o_price


def compare_main():
    if len(sys.argv) >= 2:
        dt_str = sys.argv[1]
    else:
        dt = datetime.datetime.now()
        dt = dt - datetime.timedelta(days=1)
        dt_str = dt.strftime("%Y%m%d")
    print("compare datetime={}".format(dt_str))
    compare_stg(dt_str)


def compare_his_vs_his():
    fn1 = r"c:\ftp\params\Stgtrd_M5_20230816.csv"
    fn2 = r"c:\ftp\params\Stgtrd_M5_20230817.csv"
    his_dates = read_his_dates(fn1)

    max_diff, slot, m_price, o_price = 0, "", 0, 0
    for his_dt in his_dates:
        max_diff_tmp, slot_tmp, m_price_tmp, o_price_tmp = compare_ovo_stg(his_dt, fn1, fn2)
        if max_diff_tmp > max_diff:
            max_diff, slot, m_price, o_price = max_diff_tmp, slot_tmp, m_price_tmp, o_price_tmp

    print("max_diff={} one_price={} another_price={} slot={}"
          .format(max_diff, m_price, o_price, slot))


if __name__ == "__main__":
    # compare_main()
    compare_his_vs_his()
