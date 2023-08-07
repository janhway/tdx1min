from datetime import datetime
import pytz

CHINA_TZ = pytz.timezone("Asia/Shanghai")
fstart = datetime(2023, 7, 20, 9, 25, 0, tzinfo=CHINA_TZ)
print(fstart.tzname())


CHINA_TZ = pytz.timezone("Asia/Shanghai")
fstart = CHINA_TZ.localize(datetime(2023, 7, 20, 9, 25, 0))
print(fstart.tzname())  # Output: Asia/Shanghai
