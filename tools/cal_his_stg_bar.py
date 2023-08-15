import datetime
import json
import os.path
import time
from typing import List

from pytdx.hq import TdxHq_API
from tdx1min.api_pool import _tdx_hq_host, ApiPool
from tdx1min.tdx_bars import tdx_slot_equal
from tdx1min.tdx_cfg import WORK_DIR
from tdx1min.tdx_stg import read_cfg, day_bar_slots, read_cfg_file, cal_pre_tmap, cal_open_close_new, \
    write_stg_price_file
from tdx1min.vnlog import loge, logi

tmp_hosts = _tdx_hq_host(position='上海')
hosts = [(tmp_hosts[h]['HostName'], tmp_hosts[h]['IPAddress'], int(tmp_hosts[h]['Port'])) for h in tmp_hosts]
HOST = hosts[0][1]
PORT = hosts[0][2]
print(HOST, PORT)


def my_slot_to_tdx_slot(my_slot):
    slots = day_bar_slots()
    for i, slot in enumerate(slots):
        if slot == my_slot:
            if i != len(slots) - 1:
                return slots[i + 1]
            else:
                return "1500"
    return None


def tst_my_slot_to_tdx_slot():
    tdx_slots = [my_slot_to_tdx_slot(slot) for slot in day_bar_slots()]
    print(tdx_slots)


def dl_his_component_bar():
    today = '20230814'
    cfg_file = f'Stgtrd_cfg_{today}.csv'
    cfg_file = os.path.join(r"c:\ftp\params", cfg_file)
    mcodes, _ = read_cfg_file(cfg_file)
    # mcodes = [(1, '600216'), (0, '300757')]
    start = time.time()
    slots = day_bar_slots()
    mp_all = {}
    api_pool: ApiPool = ApiPool(10)
    api_pool.start()
    time.sleep(10)
    pool: List[TdxHq_API] = api_pool.alloc_api(7)
    api_idx = 0
    try:
        step = 10
        for i in range(0, len(mcodes), step):
            print("process {} ratio".format(round(i * 100 / len(mcodes), 2)))
            mcodes_tmp = mcodes[i:i + step]
            bars = None
            try_times = 10
            while not bars and try_times > 0:
                api = pool[api_idx % len(pool)]
                api_idx = (api_idx + 1) % len(pool)
                bars = api.get_security_bars_x(0, mcodes_tmp, 0, 80)
                # print(bars)
                if not bars:
                    api_pool.release_api()
                    loge("fail to query bars. host={} mcodes={}".format(api.ip, mcodes_tmp))
                    time.sleep(11 - try_times)
                    api_pool.alloc_api(7)
                    try_times -= 1

            for code in bars:
                for slot in slots:
                    if slot not in mp_all:
                        mp_all[slot] = {}
                    d: List[dict] = bars[code]
                    tdx_slot_std = my_slot_to_tdx_slot(slot)
                    found = False
                    for it in d:
                        # 'datetime', '2023-08-09 15:00'   2023-08-10 14:55
                        tmp_tdx_datetime = it['datetime'].replace("-", "").replace(":", "").replace(" ", "")
                        it_tdx_date = tmp_tdx_datetime[0:8]
                        it_tdx_slot = tmp_tdx_datetime[8:]
                        if it_tdx_date == today and tdx_slot_equal(tdx_slot_std, it_tdx_slot):
                            mp_all[slot][code] = it
                            found = True
                            break

                    if not found:
                        for j in range(len(d) - 1, -1, -1):
                            it = d[j]
                            tmp_tdx_datetime = it['datetime'].replace("-", "").replace(":", "").replace(" ", "")
                            it_tdx_date = tmp_tdx_datetime[0:8]
                            it_tdx_slot = tmp_tdx_datetime[8:]
                            if today > it_tdx_date or (today == it_tdx_date and tdx_slot_std > it_tdx_slot):
                                mp_all[slot][code] = it
                                mp_all[slot][code]['open'] = d[j]['close']  # open设置成跟close一样
                                found = True
                                break
                        assert found, loge("bar not found. code={} slot={} d={}".format(code, slot, d))
            time.sleep(1)
            # break
    except Exception as e:  # break
        loge("exception {}".format(e))
    finally:
        api_pool.stop()

    output_path = WORK_DIR

    mp_filename = os.path.join(output_path, f"dl_bar_{today}.txt")
    with open(mp_filename, 'w') as fp:
        json.dump(mp_all, fp)
    spent = time.time() - start
    logi("finished. spent={}".format(spent))


def cal_his_stg_bar():
    today = '20230814'

    cfg_file = f'Stgtrd_cfg_{today}.csv'
    cfg_file = os.path.join(r"c:\ftp\params", cfg_file)
    mcodes, cfg = read_cfg_file(cfg_file)

    output_path = WORK_DIR
    mp_filename = os.path.join(output_path, f"file_bar_{today}.txt")
    with open(mp_filename, 'r') as fp:
        mp_all = json.load(fp)
    # print(mp_all)

    pre_tmap = cal_pre_tmap(cfg)
    stg_output_path = os.path.join(output_path, "stg_his")
    stg_output_file = os.path.join(stg_output_path, f"his_stg_{today}_file.csv")

    slots = day_bar_slots()
    with open(stg_output_file, "w") as fp:
        title = 'Code,open,close,dt,CreateTime\n'
        fp.write(title)
        for slot in slots:
            open_price, close_price = cal_open_close_new(slot, pre_tmap, cfg, mp_all[slot])
            open_price = round(open_price, 5)
            close_price = round(close_price, 5)
            create_time = datetime.datetime.now().strftime("%H:%M:%S")
            tmp = ['Stg', str(open_price), str(close_price), today+slot, create_time]
            new_last_line = ','.join(tmp) + "\n"
            fp.write(new_last_line)


if __name__ == "__main__":
    # dl_his_component_bar()
    cal_his_stg_bar()
    # mcodes, _ = read_cfg()
