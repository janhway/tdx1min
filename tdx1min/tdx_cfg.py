import os
import random
from pathlib import Path

from typing import Tuple

BAR_PERIOD = 5


def _tdx_hq_host(position: str = None) -> dict:
    file = os.path.dirname(__file__)
    file = os.path.join(file, 'connect.cfg')
    tmp_path = Path(file)
    if not tmp_path.exists():
        file = Path.cwd()
        file = os.path.join(file, 'connect.cfg')

    hq_hosts: dict = {}

    with open(file, "r", encoding='utf8') as fp:
        lines = fp.readlines()

    hq_start = False
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if not hq_start:
            tmp_idx = line.find('[HQHOST]')
            if tmp_idx != -1:
                hq_start = True
            continue

        if line[0] == '[':
            break

        # HostName01 = 深圳双线主站1
        # IPAddress01 = 110.41.147.114
        # Port01 = 7709
        fields = ["HostName", "IPAddress", "Port"]
        for field in fields:
            if line.startswith(field):
                hid = line[len(field):len(field) + 2]
                if hid not in hq_hosts:
                    hq_hosts[hid]: dict = {}
                hq_hosts[hid][field] = line[len(field) + 3:]
                if field == 'Port' and hq_hosts[hid][field] != '7709':
                    del (hq_hosts[hid])

    if position:
        hq_hosts_spec = {}
        for k in hq_hosts:
            if hq_hosts[k]["HostName"].find(position) != -1:
                hq_hosts_spec[k] = hq_hosts[k]

        return hq_hosts_spec

    return hq_hosts


TDX_HQ_HOST = _tdx_hq_host()


def rand_hq_host(position: str = None) -> dict:
    hosts_dict = _tdx_hq_host(position)
    hosts = [hosts_dict[k] for k in hosts_dict]
    idx = random.randint(0, len(hosts[0:3]) - 1)
    return hosts[idx]


PROJECT_NAME = 'tdx1min'


def _crt_work_dir(work_dir_name: str) -> Tuple[Path, Path]:
    pth = Path.cwd()
    tmp = str(pth)
    idx = tmp.find(PROJECT_NAME)
    if idx < 0:
        fp = tmp
    else:
        fp = tmp[0:idx + len(PROJECT_NAME)]
    home_path = Path(fp)
    work_path: Path = home_path.joinpath(work_dir_name)

    # Create work_path folder under home path if not exist.
    if not work_path.exists():
        work_path.mkdir()

    return home_path, work_path


WORK_DIR = str(_crt_work_dir(".workdir")[1])

if __name__ == '__main__':
    print(rand_hq_host('深圳'))
