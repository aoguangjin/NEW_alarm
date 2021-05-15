# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/15 13:31
# ☯ File  : test.py

import datetime
data=0.159
a = float(data)
print(a)


AlarmRule = "len(X) == 0"
Over_formula = AlarmRule.replace("X", "{}".format([]))
print(Over_formula)
p_result = eval(Over_formula)
print('我是p_result', p_result)



historydata = [(datetime.datetime(2021, 1, 28, 15, 0),)]

if historydata:
    history_stime = historydata[0][0]
    print(history_stime)


now_time = datetime.datetime.now()
startime_strp = now_time.strftime('%Y-%m-%d %H:%M:%S')
print((type(startime_strp)))