# coding=utf8

import os
import datetime
import ftplib
import traceback
from pathlib import Path

import pytz
# from tqdm import tqdm

from tdx1min.vnlog import logi, loge


class FtpHelper:
    ftp = ftplib.FTP()

    # ftp.set_debuglevel(1)
    # ftp.set_pasv(False)
    # q = Queue(7000)

    def __init__(self, host, port=9000):
        self.ftp.connect(host, port)

    def login(self, username='anonymous', password=''):
        self.ftp.login(username, password)
        # self.ftp.sock.ioctl(socket.SIO_KEEPALIVE_VALS, (10, 10000, 30000))
        logi(self.ftp.welcome)

    def convert_utc_to_local(self, utc_time_string, local_timezone):
        # 解析 UTC 时间字符串为 datetime 对象
        utc_time = datetime.datetime.strptime(utc_time_string, "%Y%m%d%H%M%S")

        # 设置 UTC 时区
        utc_timezone = pytz.timezone('UTC')
        utc_time = utc_timezone.localize(utc_time)

        # 转换为本地时区时间
        local_timezone = pytz.timezone(local_timezone)
        local_time = utc_time.astimezone(local_timezone)

        return local_time

    # 返回的是北京时间
    def modify_time(self, remote_file):
        try:
            mtime = self.ftp.sendcmd('MDTM ' + remote_file)
            utc_mtime_str = mtime[4:]
            mtime_datetime = self.convert_utc_to_local(utc_mtime_str, "Asia/Shanghai")
            logi("{} modification time={} datetime={}".format(remote_file, mtime, mtime_datetime))
            return mtime_datetime
        except Exception as e:
            error_message = traceback.format_exc()
            loge("Exception {}".format(error_message))
        return None

    def download_file(self, local_file_path, remote_file_path):  # 下载单个文件
        try:
            # 下载文件
            with open(local_file_path, 'wb') as fp:
                self.ftp.retrbinary('RETR ' + remote_file_path, fp.write)

            local_size = Path(local_file_path).stat().st_size
            remote_size = self.ftp.size(remote_file_path)
            if local_size != remote_size:
                loge("{} File downloaded, but file size verify failed.".format(local_file_path))
                return False

            logi("{} File downloaded successfully".format(local_file_path))
            return True
        except Exception as e:
            error_message = traceback.format_exc()
            loge("Exception {}".format(error_message))

        return False

    def download_file_partial(self, local_file_path, remote_file_path):  # 下载单个文件,断点续传

        if not os.path.isfile(local_file_path):
            loge('%s not exist' % local_file_path)
            return self.download_file(local_file_path, remote_file_path)

        try:
            start_byte = Path(local_file_path).stat().st_size

            # 下载文件的回调函数
            def download_callback(data):
                fp.write(data)

            # 打开本地文件并准备下载
            with open(local_file_path, 'ab') as fp:
                # 设置下载起始位置
                self.ftp.retrbinary('RETR ' + remote_file_path, callback=download_callback, rest=start_byte)

            local_size = Path(local_file_path).stat().st_size
            remote_size = self.ftp.size(remote_file_path)
            if local_size != remote_size:
                loge("{} Patial File downloaded, but file size verify failed.".format(local_file_path))
                return False

            logi("{} Partial File downloaded successfully".format(local_file_path))
            return True
        except Exception as e:
            error_message = traceback.format_exc()
            loge("Exception {}".format(error_message))

        return False

    def upload_file(self, local_file, remote_file):
        """从本地上传文件到ftp
           参数:
             local_path: 本地文件

             remote_path: 远程文件
        """
        if not os.path.isfile(local_file):
            loge('%s not exist' % local_file)
            return

        try:
            # 打开本地文件并准备上传
            with open(local_file, 'rb') as fp:
                # 上传文件
                self.ftp.storbinary('STOR ' + remote_file, fp, 1024)

            logi("{} File uploaded successfully".format(local_file))

            local_size = Path(local_file).stat().st_size
            remote_size = self.ftp.size(remote_file)
            if local_size != remote_size:
                loge("{} File upload, buf file size verify failed.".format(local_file))
                return False

            # # 计算本地文件的哈希值
            # local_hash = hashlib.md5()
            # with open(local_file, "rb") as f:
            #     for chunk in iter(lambda: f.read(4096), b""):
            #         local_hash.update(chunk)
            #
            # # 获取远程文件大小和哈希值
            # remote_hash = self.ftp.sendcmd("MD5 " + remote_file).split()[1]
            #
            # # 比较远程文件的大小和哈希值
            # if remote_hash != local_hash.hexdigest():
            #     loge("{} File upload, buf md5 verify failed.".format(local_file))
            #     return False

        except Exception as e:
            error_message = traceback.format_exc()
            loge("Exception {}".format(error_message))
            return False

        loge("{} File upload and verify ok.".format(local_file))
        return True

    def close(self):
        # self.q.close()
        self.ftp.quit()


if __name__ == "__main__":
    ftp = FtpHelper('127.0.0.1', 21)
    ftp.login('ftpuser', 'ftpuser123/')
    # ftp.upload_file(r"c:\ftp\stg\stg_20230817.csv", "/ftp/stg_xx.csv")
    ret = ftp.download_file(r"e:\chenzhenwei\abc.txt", "/ftp/stg/stg_20230817.csv")
    ftp.close()
    pass

