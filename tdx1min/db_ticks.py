import datetime
import time

from pathlib import Path

from sqlalchemy import create_engine, Column, Integer, Text, Float, Index
from sqlalchemy.orm import sessionmaker, declarative_base

from tdx1min.tdx_cfg import WORK_DIR
from tdx1min.trade_calendar import CHINA_TZ
from tdx1min.vnlog import logi


def cur_timestamp_ms():
    t = time.time()
    return int(round(t * 1000))


def get_db_tick_path():
    folder_path = Path(WORK_DIR)
    folder_path = folder_path.joinpath("db")
    if not folder_path.exists():
        folder_path.mkdir()

    folder_path = folder_path.joinpath("ticks.sqlite3")
    logi("get_db_tick_path, current path={} db_path={}".format(Path.cwd(), folder_path))
    return folder_path.__str__()


_db_path = get_db_tick_path()
_db_url = "sqlite:///" + _db_path
print("db_url={}".format(_db_url))
engine = create_engine(_db_url)
Session = sessionmaker(bind=engine)
Base = declarative_base()


def init_db():
    with Session() as session:
        # 在这里添加初始化数据的代码
        pass


def create_database():
    Base.metadata.create_all(engine)

    # 创建索引并添加到表中
    # idx_code_time = Index('idx_code_time', XtTick.code, XtTick.time)
    # idx_code_time.create(bind=engine)


def drop_database():
    Base.metadata.drop_all(engine)


class TdxTick(Base):
    __tablename__ = 'tdx_tick'

    id = Column(Integer, primary_key=True)
    code = Column(Text(8))
    time = Column(Text(8))  # 1689818859000  毫秒时间戳  1分钟线标识
    stime = Column(Text(16))
    price = Column(Text(10))
    created = Column(Integer, default=cur_timestamp_ms)

    index_time_code = Index('idx_time_code', time, code)


class Bar1Min(Base):
    __tablename__ = 'bar_1min'

    id = Column(Integer, primary_key=True)
    code = Column(Text(8))
    date = Column(Text(8))
    time = Column(Text(8))
    open = Column(Text(10))
    open_st = Column(Text(16)) # servertime
    close = Column(Text(10))
    close_st = Column(Text(16))  # servertime
    instead_date = Column(Text(8))
    created = Column(Integer, default=cur_timestamp_ms)

    def __repr__(self):
        return str(self.code) + "_" + str(self.date) + "_" + str(self.time)\
               + "_" + str(self.open_st) + "_" + str(self.open)\
               + "_" + str(self.close_st) + "_" + str(self.close)

    index_time_code = Index('idx_date_time_code', date, time, code)


# # 获取模型类的所有列
# _tick_columns = XtTick.__table__.columns
#
# # 获取字段名列表
# tick_field_names = [column.name for column in _tick_columns]


def crt_ticks(ticks):
    with Session() as session:
        session.add_all(ticks)
        session.commit()


def crt_bar1min(bars):
    with Session() as session:
        session.add_all(bars)
        session.commit()


create_database()

if __name__ == '__main__':
    pass
