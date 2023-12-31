#coding=utf-8
from __future__ import unicode_literals, division

import pandas as pd
import os

from pytdx.reader.base_reader import TdxFileNotFoundException, TdxNotAssignVipdocPathException
from pytdx.reader.base_reader import BaseReader
from collections import OrderedDict

"""
网传秘籍...

二、通达信5分钟线*.lc5文件和*.lc1文件 文件名即股票代码 每32个字节为一个5分钟数据，每字段内低字节在前 00 ~ 01 字节：日期，整型，
设其值为num，则日期计算方法为： year=floor(num/2048)+2004; month=floor(mod(num,2048)/100); day=mod(mod(num,2048),100);
02 ~ 03 字节： 从0点开始至目前的分钟数，整型 04 ~ 07 字节：开盘价，float型 08 ~ 11 字节：最高价，float型 12 ~ 15 字节：最低价，
float型 16 ~ 19 字节：收盘价，float型 20 ~ 23 字节：成交额，float型 24 ~ 27 字节：成交量（股），整型 28 ~ 31 字节：（保留）

"""


class TdxLCMinBarReader(BaseReader):
    """
    读取通达信分钟数据
    """

    def parse_data_by_file(self, fname):
        if not os.path.isfile(fname):
            raise TdxFileNotFoundException('no tdx kline data, pleaes check path %s', fname)
        with open(fname, 'rb') as f:
            content = f.read()
            raw_li = self.unpack_records("<HHfffffII", content)
            data = []
            for row in raw_li:
                year, month, day = self._parse_date(row[0])
                hour, minute = self._parse_time(row[1])

                data.append(OrderedDict([
                    ("date", "%04d-%02d-%02d %02d:%02d" % (year, month, day, hour, minute)),
                    ("year", year),
                    ('month', month),
                    ('day', day),
                    ('hour', hour),
                    ('minute', minute),
                    ('open', row[2]),
                    ('high', row[3]),
                    ('low', row[4]),
                    ('close', row[5]),
                    ('amount', row[6]),
                    ('volume', row[7]),
                    #('unknown', row[8])
                ]))
            return data
        return []

    def get_df(self, code_or_file, exchange=None):
        #if exchange == None:
            # 只传入了一个参数
        data = self.parse_data_by_file(code_or_file)
        #else:
        #    data = [self._df_convert(row) for row in self.get_kline_by_code(code_or_file, exchange)]
        df = pd.DataFrame(data=data)
        df.index = pd.to_datetime(df.date)
        return df[['open', 'high', 'low', 'close', 'amount', 'volume']]

    def _parse_date(self, num):
        year = num // 2048 + 2004
        month = (num % 2048) // 100
        day = (num % 2048) % 100

        return year, month, day

    def _parse_time(self, num):
        return (num // 60) , (num % 60)


if __name__ == '__main__':
    file = r'D:\new_tdx\vipdoc\sz\minline\sz000001.lc1'
    reader = TdxLCMinBarReader()
    df = reader.get_df(file)
    print(df)

    # print(df['2017-07-26'].sum())