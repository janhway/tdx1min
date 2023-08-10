import pytz
import datetime

CHINA_TZ = pytz.timezone("Asia/Shanghai")

IS_TEST_RUN = False


def _trading_time():
    china_tz = pytz.timezone("Asia/Shanghai")
    tt = {}
    if not IS_TEST_RUN:
        tt['APP_START'] = datetime.time(8, 30, 0, tzinfo=china_tz)
        tt['REAL_BID_START'] = datetime.time(9, 20, 0, tzinfo=china_tz)
        tt['REAL_BID_END'] = datetime.time(9, 25, 0, tzinfo=china_tz)
        # Chinese  market trading period
        tt['F_START'] = datetime.time(9, 30, 0, tzinfo=china_tz)  # 9:30 confirm it
        tt['F_END'] = datetime.time(11, 30, 0, tzinfo=china_tz)
        tt['S_START'] = datetime.time(13, 0, 0, tzinfo=china_tz)
        tt['S_END'] = datetime.time(15, 0, 0, tzinfo=china_tz)

        tt['GZNHG_START'] = datetime.time(15, 0, 5, tzinfo=china_tz)
        tt['GZNHG_END'] = datetime.time(15, 30, 0, tzinfo=china_tz)

        tt['APP_STOP'] = datetime.time(16, 0, 0, tzinfo=china_tz)

    else:
        tt['APP_START'] = datetime.time(8, 30, 0, tzinfo=china_tz)
        tt['REAL_BID_START'] = datetime.time(9, 20, 0, tzinfo=china_tz)
        tt['REAL_BID_END'] = datetime.time(9, 25, 0, tzinfo=china_tz)
        # Chinese  market trading period
        tt['F_START'] = datetime.time(9, 30, 0, tzinfo=china_tz)  # 9:30 confirm it
        tt['F_END'] = datetime.time(11, 30, 0, tzinfo=china_tz)
        tt['S_START'] = datetime.time(13, 0, 0, tzinfo=china_tz)
        tt['S_END'] = datetime.time(15, 0, 0, tzinfo=china_tz)

        tt['GZNHG_START'] = datetime.time(15, 0, 5, tzinfo=china_tz)
        tt['GZNHG_END'] = datetime.time(15, 30, 0, tzinfo=china_tz)

        tt['APP_STOP'] = datetime.time(16, 0, 0, tzinfo=china_tz)
    return tt


trd_time = _trading_time()


def if_weekend(day_str, separator=""):
    """
    if a day is weekend
    :param day_str: string of a day
    :param separator: separator of year, month and day, default is empty
    :return: True: is weekend; False: not weekend
    """
    spec = "%Y" + separator + "%m" + separator + "%d"
    day = datetime.datetime.strptime(day_str, spec).date()
    # Monday == 0 ... Sunday == 6
    if day.weekday() in [5, 6]:
        return True
    else:
        return False


HOLIDAY = ["20200501", "20200504", "20200505", "20200625", "20200626", "20201001", "20201002",
           "20201005", "20201006", "20201007", "20201008", ]


def if_holiday(datestr):
    if datestr in HOLIDAY:
        return True
    else:
        return False


def if_tradedate(datestr):
    if if_weekend(datestr):
        return False
    if if_holiday(datestr):
        return False
    return True


def now_is_tradedate():
    if IS_TEST_RUN:
        return True  # 测试时当作可交易日

    d = datetime.datetime.now()
    s = d.strftime("%Y%m%d")
    return if_tradedate(s)


def check_trading_period():
    """"""
    if not now_is_tradedate():
        return False

    current_time = datetime.datetime.now(tz=CHINA_TZ).time()
    # print("current_time=", current_time)
    trading = False
    if (trd_time['F_START'] <= current_time <= trd_time['F_END']) or (
            trd_time['S_START'] <= current_time <= trd_time['S_END']):
        trading = True

    return trading


def check_trading_period2():
    """"""
    if not now_is_tradedate():
        return False

    current_time = datetime.datetime.now(tz=CHINA_TZ).time()
    # print("current_time=", current_time)
    trading = False
    if (trd_time['REAL_BID_START'] <= current_time <= trd_time['F_END']) or (
            trd_time['S_START'] <= current_time <= trd_time['S_END']):
        trading = True

    return trading


def get_time_earlier_than_xminutes(loc_time, earlier_minutes):
    h = loc_time.hour
    m = loc_time.minute - earlier_minutes
    if m < 0:
        m += 60
        h -= 1  # 我们使用场景的h总是大于0

    return datetime.time(h, m, loc_time.second, loc_time.microsecond, tzinfo=CHINA_TZ)


def check_trading_period_ahead():
    """"""
    if not now_is_tradedate():
        return False

    f_ahead_start = get_time_earlier_than_xminutes(trd_time['F_START'], 10)
    s_ahead_start = get_time_earlier_than_xminutes(trd_time['S_START'], 10)

    current_time = datetime.datetime.now(tz=CHINA_TZ).time()

    return (f_ahead_start <= current_time <= trd_time['F_START']) or (
            s_ahead_start <= current_time <= trd_time['S_START'])


def check_noon_period():
    if not now_is_tradedate():
        return False

    current_time = datetime.datetime.now(tz=CHINA_TZ).time()
    return trd_time['F_END'] < current_time < trd_time['S_START']


def check_gznhg_period():
    if not now_is_tradedate():
        return False

    current_time = datetime.datetime.now(tz=CHINA_TZ).time()
    return trd_time['GZNHG_START'] < current_time < trd_time['GZNHG_END']


def check_gznhg_ahead_period():
    if not now_is_tradedate():
        return False

    current_time = datetime.datetime.now(tz=CHINA_TZ).time()
    ahead_start = get_time_earlier_than_xminutes(trd_time['S_END'], 3)
    return ahead_start < current_time <= trd_time['S_END']


# print(check_trading_period())

def check_market_close():
    current_time = datetime.datetime.now(tz=CHINA_TZ).time()
    return current_time >= trd_time['S_END']


def cur_date():
    n = datetime.datetime.now()
    return n.strftime("%Y%m%d")
