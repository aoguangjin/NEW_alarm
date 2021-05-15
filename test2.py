# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/15 16:29
# ☯ File  : test2.py

import datetime

nowTime = datetime.datetime.now() - datetime.timedelta(minutes=1)
e_time = datetime.datetime.strftime(nowTime, '%Y-%m-%d %H:%M:00')
minute_int = nowTime.strftime('%M')
eTime = e_time.split(':')[0] + ':' + minute_int[0] + '0:00'

# 离线结束时间
endtime_strp = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:00')
print(type(endtime_strp))

# 离线开始时间
startime_strp = endtime_strp - datetime.timedelta(seconds=int(30))
print(type(startime_strp))