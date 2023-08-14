def read_o_stg(dt='20230811'):
    fn = r"c:\ftp\stg\Stgtrd_M5.csv"

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

    print(mp)
    return mp


def read_m_stg(dt='20230811'):
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

    print(mp)
    return mp


def compare_stg():
    dt = '20230814'
    m = read_m_stg(dt)
    o = read_o_stg(dt)
    assert len(m.keys()) == len(o.keys())
    total_count = 2 * len(m.keys())
    count = 0
    least_diff = 0.0001
    for k in m:
        diff = float(m[k]['op']) - float(o[k]['op'])
        diff = round(abs(diff), 6)
        if diff > least_diff:
            print("open price unequal slot={} my_op={} th_op={} diff={}".format(k, m[k]['op'], o[k]['op'], diff))
            count += 1

        diff = float(m[k]['cp']) - float(o[k]['cp'])
        diff = round(abs(diff), 6)
        if diff > least_diff:
            print("clos price unequal slot={} my_op={} th_op={} diff={}".format(k, m[k]['op'], o[k]['op'], diff))
            count += 1

    print("total_count={} count={}".format(total_count, count))


if __name__ == "__main__":
    compare_stg()
