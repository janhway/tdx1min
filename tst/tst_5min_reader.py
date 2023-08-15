from mootdx.reader import Reader
import pandas as pd
# tdx_dir = r'C:\zd_zsone'
tdx_dir = r'D:\new_tdx'
reader = Reader.factory(market='std', tdxdir=tdx_dir)

# 读取日线数据
# r = reader.daily(symbol='600036')
# print(r)

# 读取1分钟数据
# r: pd.DataFrame = reader.minute(symbol='600036')
# print(r.tail(5))

# 读取时间线数据
df: pd.DataFrame = reader.fzline(symbol='300757')
k = str(df.index[1])
print("k=",type(k), k)
# v = df.loc['2023-08-11 14:05:10']
# print(type(v), v)
# print("\n\n")
# print(df[['open','close']].tail(60))
#
# vipdoc = reader.find_path(symbol='300757', subdir='fzline', suffix='lc5')
# print(vipdoc)
