import datetime
import sys


def read_o_stg(dt):
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
    # dt = '20230815'
    m = read_m_stg(dt)
    o = read_o_stg(dt)
    assert len(m.keys()) == len(o.keys())
    total_count = 2 * len(m.keys())
    count = 0
    least_diff = 0.0001

    max_diff = 0
    o_price = 0
    m_price = 0
    for k in m:
        diff = cdiff(float(m[k]['op']), float(o[k]['op']))
        if diff > least_diff:
            print("open price unequal slot={} my_op={} th_op={} diff={}".format(k, m[k]['op'], o[k]['op'], diff))
            count += 1
        if diff > max_diff:
            max_diff = diff
            o_price = float(o[k]['op'])
            m_price = float(m[k]['op'])

        diff = cdiff(float(m[k]['cp']), float(o[k]['cp']))
        if diff > least_diff:
            print("clos price unequal slot={} my_op={} th_op={} diff={}".format(k, m[k]['op'], o[k]['op'], diff))
            count += 1
        if diff > max_diff:
            max_diff = diff
            o_price = float(o[k]['cp'])
            m_price = float(m[k]['cp'])

    print("total_count={} mis_count={} max_diff={} o_price={} m_price={}"
          .format(total_count, count, max_diff, o_price, m_price))


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        dt_str = sys.argv[1]
    else:
        dt = datetime.datetime.now()
        dt = dt - datetime.timedelta(days=1)
        dt_str = dt.strftime("%Y%m%d")
    print("compare datetime={}".format(dt_str))
    compare_stg(dt_str)
