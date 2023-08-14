import datetime
import os
import socket
from pathlib import Path

from tst.ftp import ftphelper


def get_local_machine_info():
    try:
        # 获取本地主机名
        host_name = socket.gethostname()
        print("Host name:", host_name)  # Host name: janh-rtx

        # 获取本地主机的 IP 地址
        ip_address = socket.gethostbyname(host_name)
        print("IP address:", ip_address)

    except Exception as e:
        print("Error:", str(e))


def get_file_times(file_path):
    try:
        # 获取最近修改时间
        modification_time = os.path.getmtime(file_path)
        modification_datetime = datetime.datetime.fromtimestamp(modification_time)
        print("Last modification time:", modification_datetime)

        # 获取最近访问时间
        access_time = os.path.getatime(file_path)
        access_datetime = datetime.datetime.fromtimestamp(access_time)
        print("Last access time:", access_datetime)

    except Exception as e:
        print("Error:", str(e))


def get_file_times_x(file_path):
    try:
        # 创建Path对象
        file_path_obj = Path(file_path)

        # 获取最近修改时间
        modification_time = file_path_obj.stat().st_mtime
        modification_datetime = datetime.datetime.fromtimestamp(modification_time)
        print("Last modification time:", modification_datetime)

        # 获取最近访问时间
        access_time = file_path_obj.stat().st_atime
        access_datetime = datetime.datetime.fromtimestamp(access_time)
        print("Last access time:", access_datetime)

    except Exception as e:
        print("Error:", str(e))


def download_file(filename):
    ftp = ftphelper('106.14.134.228', 21)
    ftp.login('ftpuser', 'FTP+python')

    local_path = Path("c:/ftp/params")
    if not local_path.exists():
        local_path.mkdir(parents=True)
    local_path = local_path.joinpath(filename)
    if local_path.exists():
        local_path.unlink()

    remote_path = "/params/" + filename
    ftp.DownLoadFile(local_path.absolute(), remote_path)
    # print(local_path.absolute())
    if local_path.exists():
        return local_path.absolute()
    return ""


def dl_stg_cfg():
    print("=== before download")
    get_file_times(r"c:\ftp\params\Stgtrd_cfg.csv")
    print("=== downloading")
    download_file("Stgtrd_cfg.csv")
    print("=== after download")
    get_file_times(r"c:\ftp\params\Stgtrd_cfg.csv")


if __name__ == "__main__":
    # get_local_machine_info()
    # get_file_times(r"c:\ftp\params\Stgtrd_cfg.csv")
    # get_file_times_x(r"c:\ftp\params\Stgtrd_cfg.csv")
    dl_stg_cfg()