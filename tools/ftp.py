# coding=utf8
# FTP操作
import ftplib
import os
import threading
# !/usr/bin/python
# -*- coding: UTF-8 -*-
from tqdm import tqdm

from ftplib import FTP
import os
import sys
import time
import socket
from multiprocessing import Queue
import zipfile


class MyFTP:
    """
        ftp自动下载、自动上传脚本，可以递归目录操作
    """

    def __init__(self, host, port=21):
        """ 初始化 FTP 客户端
        参数:
                 host:ip地址

                 port:端口号
        """
        # print("__init__()---> host = %s ,port = %s" % (host, port))

        self.host = host
        self.port = port
        self.ftp = FTP()
        # 重新设置下编码方式
        self.ftp.encoding = 'gbk'
        self.log_file = open("log.txt", "a")
        self.file_list = []

    def login(self, username, password):
        """ 初始化 FTP 客户端
            参数:
                  username: 用户名

                 password: 密码
            """
        try:
            timeout = 60
            socket.setdefaulttimeout(timeout)
            # 0主动模式 1 #被动模式
            self.ftp.set_pasv(False)
            # 打开调试级别2，显示详细信息
            # self.ftp.set_debuglevel(2)

            self.debug_print('开始尝试连接到 %s' % self.host)
            self.ftp.connect(self.host, self.port)
            self.debug_print('成功连接到 %s' % self.host)

            self.debug_print('开始尝试登录到 %s' % self.host)
            self.ftp.login(username, password)
            self.debug_print('成功登录到 %s' % self.host)

            self.debug_print(self.ftp.welcome)
        except Exception as err:
            self.deal_error("FTP 连接或登录失败 ，错误描述为：%s" % err)
            pass

    def is_same_size(self, local_file, remote_file):
        """判断远程文件和本地文件大小是否一致

           参数:
             local_file: 本地文件

             remote_file: 远程文件
        """
        try:
            remote_file_size = self.ftp.size(remote_file)
        except Exception as err:
            # self.debug_print("is_same_size() 错误描述为：%s" % err)
            remote_file_size = -1

        try:
            local_file_size = os.path.getsize(local_file)
        except Exception as err:
            # self.debug_print("is_same_size() 错误描述为：%s" % err)
            local_file_size = -1

        self.debug_print('local_file_size:%d  , remote_file_size:%d' % (local_file_size, remote_file_size))
        if remote_file_size == local_file_size:
            return 1
        else:
            return 0

    def download_file(self, local_file, remote_file):
        """从ftp下载文件
            参数:
                local_file: 本地文件

                remote_file: 远程文件
        """
        self.debug_print("download_file()---> local_path = %s ,remote_path = %s" % (local_file, remote_file))

        if self.is_same_size(local_file, remote_file):
            self.debug_print('%s 文件大小相同，无需下载' % local_file)
            return
        else:
            try:
                self.debug_print('>>>>>>>>>>>>下载文件 %s ... ...' % local_file)
                buf_size = 1024
                file_handler = open(local_file, 'wb')
                self.ftp.retrbinary('RETR %s' % remote_file, file_handler.write, buf_size)
                file_handler.close()
            except Exception as err:
                self.debug_print('下载文件出错，出现异常：%s ' % err)
                return

    def download_file_tree(self, local_path, remote_path):
        """从远程目录下载多个文件到本地目录
                       参数:
                         local_path: 本地路径

                         remote_path: 远程路径
                """
        print("download_file_tree()--->  local_path = %s ,remote_path = %s" % (local_path, remote_path))
        try:
            self.ftp.cwd(remote_path)
        except Exception as err:
            self.debug_print('远程目录%s不存在，继续...' % remote_path + " ,具体错误描述为：%s" % err)
            return

        if not os.path.isdir(local_path):
            self.debug_print('本地目录%s不存在，先创建本地目录' % local_path)
            os.makedirs(local_path)

        self.debug_print('切换至目录: %s' % self.ftp.pwd())

        self.file_list = []
        # 方法回调
        self.ftp.dir(self.get_file_list)

        remote_names = self.file_list
        self.debug_print('远程目录 列表: %s' % remote_names)
        for item in remote_names:
            file_type = item[0]
            file_name = item[1]
            local = os.path.join(local_path, file_name)
            if file_type == 'd':
                print("download_file_tree()---> 下载目录： %s" % file_name)
                self.download_file_tree(local, file_name)
            elif file_type == '-':
                print("download_file()---> 下载文件： %s" % file_name)
                self.download_file(local, file_name)
            self.ftp.cwd("..")
            self.debug_print('返回上层目录 %s' % self.ftp.pwd())
        return True

    def upload_file(self, local_file, remote_file):
        """从本地上传文件到ftp

           参数:
             local_path: 本地文件

             remote_path: 远程文件
        """
        if not os.path.isfile(local_file):
            self.debug_print('%s 不存在' % local_file)
            return

        if self.is_same_size(local_file, remote_file):
            self.debug_print('跳过相等的文件: %s' % local_file)
            return

        buf_size = 1024
        file_handler = open(local_file, 'rb')
        self.ftp.storbinary('STOR %s' % remote_file, file_handler, buf_size)
        file_handler.close()
        self.debug_print('上传: %s' % local_file + "成功!")

    def upload_file_tree(self, local_path, remote_path):
        """从本地上传目录下多个文件到ftp
           参数:

             local_path: 本地路径

             remote_path: 远程路径
        """
        if not os.path.isdir(local_path):
            self.debug_print('本地目录 %s 不存在' % local_path)
            return
        """
        创建服务器目录
        """
        try:
            self.ftp.cwd(remote_path)  # 切换工作路径
        except Exception as e:
            base_dir, part_path = self.ftp.pwd(), remote_path.split('/')
            for p in part_path[1:-1]:
                base_dir = base_dir + p + '/'  # 拼接子目录
                try:
                    self.ftp.cwd(base_dir)  # 切换到子目录, 不存在则异常
                except Exception as e:
                    print('INFO:', e)
                    self.ftp.mkd(base_dir)  # 不存在创建当前子目录
        # self.ftp.cwd(remote_path)
        self.debug_print('切换至远程目录: %s' % self.ftp.pwd())

        local_name_list = os.listdir(local_path)
        self.debug_print('本地目录list: %s' % local_name_list)
        # self.debug_print('判断是否有服务器目录: %s' % os.path.isdir())

        for local_name in local_name_list:
            src = os.path.join(local_path, local_name)
            print("src路径==========" + src)
            if os.path.isdir(src):
                try:
                    self.ftp.mkd(local_name)
                except Exception as err:
                    self.debug_print("目录已存在 %s ,具体错误描述为：%s" % (local_name, err))
                self.debug_print("upload_file_tree()---> 上传目录： %s" % local_name)
                self.debug_print("upload_file_tree()---> 上传src目录： %s" % src)
                self.upload_file_tree(src, local_name)
            else:
                self.debug_print("upload_file_tree()---> 上传文件： %s" % local_name)
                self.upload_file(src, local_name)
        self.ftp.cwd("..")

    def close(self):
        """ 退出ftp
        """
        self.debug_print("close()---> FTP退出")
        self.ftp.quit()
        self.log_file.close()

    def debug_print(self, s):
        """ 打印日志
        """
        self.write_log(s)

    def deal_error(self, e):
        """ 处理错误异常
            参数：
                e：异常
        """
        log_str = '发生错误: %s' % e
        self.write_log(log_str)
        sys.exit()

    def write_log(self, log_str):
        """ 记录日志
            参数：
                log_str：日志
        """
        time_now = time.localtime()
        date_now = time.strftime('%Y-%m-%d', time_now)
        format_log_str = "%s ---> %s \n " % (date_now, log_str)
        print(format_log_str)
        self.log_file.write(format_log_str)

    def get_file_list(self, line):
        """ 获取文件列表
            参数：
                line：
        """
        file_arr = self.get_file_name(line)
        # 去除  . 和  ..
        if file_arr[1] not in ['.', '..']:
            self.file_list.append(file_arr)

    def get_file_name(self, line):
        """ 获取文件名
            参数：
                line：
        """
        pos = line.rfind(':')
        while (line[pos] != ' '):
            pos += 1
        while (line[pos] == ' '):
            pos += 1
        file_arr = [line[0], line[pos:]]
        return file_arr


'''
if __name__ == "__main__":
    my_ftp = MyFTP("192.168.169.3")
    #my_ftp.set_pasv(False)
    my_ftp.login("ftpuser", "123456")

    # 下载单个文件
    #my_ftp.download_file("/home/BG_2019_05_22_16_04_54_Camera6-0.mp4", "/BG_2019_05_22_16_04_54_Camera6-0.mp4") #FTP服务器目录   本地目录

    # 下载目录
    # my_ftp.download_file_tree("G:/ftp_test/", "App/AutoUpload/ouyangpeng/I12/")

    # 上传单个文件
    # my_ftp.upload_file("G:/ftp_test/Release/XTCLauncher.apk", "/App/AutoUpload/ouyangpeng/I12/Release/XTCLauncher.apk")

    # 上传目录
    my_ftp.upload_file_tree("/home/zoukaicai/java8", "/123/5/")

    my_ftp.close()
'''


# _sentinel = object()
class ftphelper:
    ftp = ftplib.FTP()

    # ftp.set_debuglevel(1)
    # ftp.set_pasv(False)
    # q = Queue(7000)

    def __init__(self, host, port=9000):
        self.ftp.connect(host, port)

    def login(self, username='anonymous', password=''):
        self.ftp.login(username, password)
        # self.ftp.sock.ioctl(socket.SIO_KEEPALIVE_VALS, (10, 10000, 30000))

        print(self.ftp.welcome)

    def DownLoadFile(self, LocalFile, RemoteFile):  # 下载当个文件
        file_handler = open(LocalFile, 'wb')
        try:

            # print(file_handler)
            # self.ftp.retrbinary("RETR %s" % (RemoteFile), file_handler.write)#接收服务器上文件并写入本地文件
            bufsize = 10240  # 设置缓冲块大小

            self.ftp.retrbinary('RETR ' + RemoteFile, file_handler.write)
            # self.ftp.retrlines('RETR ' + RemoteFile,file_handler.write)
        except Exception as e:
            print('download %s error:' % RemoteFile, e)
            self.DownLoadFile(LocalFile, RemoteFile)
        finally:
            file_handler.close()

    def DownLoadFileTree(self, LocalDir, RemoteDir):  # 下载整个目录下的文件
        # print("remoteDir:", RemoteDir)
        # print("localDir:",LocalDir)
        if not os.path.exists(LocalDir):
            os.makedirs(LocalDir)
        self.ftp.cwd(RemoteDir)
        RemoteNames = self.ftp.nlst()
        # print("RemoteNames", RemoteNames)

        bar = tqdm(RemoteNames)

        for file in bar:
            bar.set_description(file)
            Local = os.path.join(LocalDir, file)
            # print(self.ftp.nlst(file))
            if file.find(".") == -1:
                if not os.path.exists(Local):
                    os.makedirs(Local)
                self.DownLoadFileTree(Local, file)
            else:
                self.DownLoadFile(Local, file)
                # self.ftp.sendcmd('PWD')
                # self.ftp.voidcmd('NOOP')
                # self.ftp.voidresp()

        # self.ftp.cwd("..")
        # return

    def upload_file(self, local_file, remote_file):
        """从本地上传文件到ftp

           参数:
             local_path: 本地文件

             remote_path: 远程文件
        """
        if not os.path.isfile(local_file):
            print('%s not exist' % local_file)
            return
        buf_size = 1024
        f = open(local_file, 'rb')
        self.ftp.storbinary('STOR %s' % remote_file, f, buf_size)
        f.close()
        print('upload: %s' % local_file + "succeed !")

    def close(self):
        # self.q.close()
        self.ftp.quit()

    def retrbinary(self, cmd, callback, fsize=0, rest=0):
        blocksize = 1024
        cmpsize = rest
        self.voidcmd('TYPE I')
        conn = self.transfercmd(cmd, rest)  # 此命令实现从指定位置开始下载,以达到续传的目的
        while 1:
            if fsize:
                data = conn.recv(blocksize)
                if not data:
                    break
                callback(data)
                cmpsize += blocksize
            conn.close()
            return self.voidresp()

    '''    
    def create_tasks(self,LocalDir,RemoteDir):
        if not os.path.exists(LocalDir):
            os.makedirs(LocalDir)
        self.ftp.cwd(RemoteDir)
        RemoteNames = self.ftp.nlst()
        print("RemoteNames", RemoteNames)
        for file in RemoteNames:
            #print(file)
            #print(self.ftp.nlst(file))
            if file.find(".") == -1:
                self.DownLoadFileTree(file)
            else:
                self.q.put([LocalDir,file])
        self.q.put(_sentinel)

    def task(self):
        while True:
            t = self.q.get()
            if t is _sentinel:
                self.q.put(_sentinel)
                break            
            #print(t)
            Local = os.path.join(t[0], t[1])
            self.DownLoadFile(Local, t[1])

    def createThreads(self, threadNumber=20):
        try:
            threads = []
            for i in range(0, threadNumber - 1):
                subThread = threading.Thread(target=self.task)
                subThread.start()
                threads.append(subThread)
            for th in threads:
                th.join()
            return threads
        except Exception as e:
            print(e)
            return None
    '''


def unzip_file(path):
    filenames = os.listdir(path)  # 获取目录下所有文件名
    for filename in filenames:
        # print(filename[-4:])
        if filename[-4:] == '.zip':
            filepath = os.path.join(path, filename)
            zip_file = zipfile.ZipFile(filepath)  # 获取压缩文件
            # print(filename)
            newfilepath = filename.split(".", 1)[0]  # 获取压缩文件的文件名
            newfilepath = os.path.join(path, newfilepath)
            # print(newfilepath)
            if os.path.isdir(newfilepath):  # 根据获取的压缩文件的文件名建立相应的文件夹
                pass
            else:
                os.mkdir(newfilepath)
            for name in zip_file.namelist():  # 解压文件
                if filename == 'day.zip':
                    zip_file.extract(name, path)
                else:
                    zip_file.extract(name, newfilepath)
                # zip_file.extractall(path)
            zip_file.close()
            if os.path.exists(filepath):  # 删除原先压缩包
                os.remove(filepath)
            print("unzip{0}ok".format(filename))


import sys
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='ftp', usage='%(prog)s [options]')
    parser.add_argument('-o', default='D', help='option type,D is download ,U is upload')
    parser.add_argument('-l', default='c:/dl', help='local path or file')
    parser.add_argument('-r', default='/', help='remote path')
    # parser.print_help()
    args = parser.parse_args()

    ftp = ftphelper('106.14.134.228', 21)
    ftp.login('ftpuser', 'FTP+python')
    localpath = args.l
    remotepath = args.r
    option = args.o
    if option.upper() == 'D':
        if remotepath == '/':
            remotepath = '/day'
        print('DownLoad files begining......')
        ftp.DownLoadFileTree(localpath, remotepath)
        # ftp.create_tasks(localpath, remotepath)
        # thds = ftp.createThreads(2)
        print('DownLoad files finished.')
        unzip_file(localpath)

    if option.upper() == 'U':
        if remotepath == '/':
            remotepath = '/params/' + os.path.basename(localpath)
        print(remotepath)
        ftp.upload_file(localpath, remotepath)
    ftp.close()
    # import msvcrt

    # print ord(msvcrt.getch())
