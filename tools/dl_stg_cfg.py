import datetime
import os
import socket
import time
from ftplib import FTP
from pathlib import Path

from tools.ftp import ftphelper


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


def get_remote_file_mtime(hostname, username, password, remote_path, filename):
    try:
        # 连接到远程FTP服务器
        with FTP(hostname) as ftp:
            # 登录
            ftp.login(username, password)

            # 转到指定目录
            ftp.cwd(remote_path)

            # 获取文件列表
            file_list = ftp.nlst()

            # 检查文件是否在列表中
            if filename in file_list:
                # 获取文件的修改时间
                mtime = ftp.sendcmd('MDTM ' + filename)
                mtime_datetime = datetime.datetime.strptime(mtime[4:], "%Y%m%d%H%M%S")
                print("Modification time of", filename, ":", mtime_datetime)
            else:
                print("File", filename, "not found in", remote_path)

    except Exception as e:
        print("Error:", str(e))


# 检查修改时间，如果一致，则不下载，否则下载
def download_file(local_path_str, remote_path_str, filename):
    ftp = ftphelper('106.14.134.228', 21)
    ftp.login('ftpuser', 'FTP+python')

    remote_file = remote_path_str + filename
    remote_mtime_datetime = ftp.modify_time(remote_file)
    remote_size = ftp.ftp.size(remote_file)
    print("remote file {} modify_time={} size={}".format(remote_file, remote_mtime_datetime, remote_size))

    local_path = Path(local_path_str)
    if not local_path.exists():
        local_path.mkdir(parents=True)
    local_path = local_path.joinpath(filename)
    if local_path.exists():
        # 本地文件修改时间
        current_mtime = local_path.stat().st_mtime
        local_mtime_datetime = datetime.datetime.fromtimestamp(current_mtime)
        local_size = local_path.stat().st_size
        print("local file {} modify_time={} size={}".format(local_path.__str__(), local_mtime_datetime, local_size))
        # 比较修改时间
        if remote_mtime_datetime == local_mtime_datetime and remote_size == local_size:
            print("modify time and size are the same, no need to download.")
            return local_path.__str__(), remote_mtime_datetime

        local_path.unlink()

    try_times = 3
    is_ok = False
    while try_times > 0:
        is_ok = ftp.download_file(local_path.__str__(), remote_file)
        if is_ok:
            break
        try_times -= 1
        time.sleep(3)

    if not is_ok:
        print("fail to download {}".format(local_path_str))
        return "", None

    # 设置新的修改时间
    os.utime(local_path.__str__(), (time.time(), remote_mtime_datetime.timestamp()))

    return local_path.__str__(), remote_mtime_datetime


def dl_stg_cfg():
    local_path_str = r"c:\ftp\params"
    filename = "Stgtrd_cfg.csv"
    local_file_str = os.path.join(local_path_str, filename)

    remote_path_str = "/params/"

    local_file_ret, mtime_datetime = download_file(local_path_str, remote_path_str, filename)
    print(local_file_ret, mtime_datetime)

    get_file_times(local_file_str)


if __name__ == "__main__":
    # get_local_machine_info()
    # get_file_times(r"c:\ftp\params\Stgtrd_cfg.csv")
    # get_file_times_x(r"c:\ftp\params\Stgtrd_cfg.csv")
    dl_stg_cfg()
