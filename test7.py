# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/23 16:00
# ☯ File  : test6.py

import pymssql
import pandas as pd
import  redis
from apscheduler.schedulers.blocking import BlockingScheduler


def B():
    r = redis.Redis(host='127.0.0.1', db=11, port=6379)
    conn = pymssql.connect(server='172.16.12.4', user="sa", password="@admin123", database="Grid_PositionPhoto",             charset="utf8")
    # 获取站点判断规则的信息(设备编号,因子,规则,报警类型,是否是设备,时间间隔,运行时间,是否推送,推送类型，推送对象，推送时长)
    AlarmInfo_sql = "SELECT  a.DGIMN, a.PollutantCode, a.AlarmRule, a.AlarmType, a.IsDevice, a.TimeInterval, a.RunTime, b.IsPush, b.PushType, b.PushObject, b.PushInterval FROM Base_AlarmRuleInfo a LEFT JOIN Base_PushRuleInfo b  on a.DGIMN = b.DGIMN and a.AlarmType = b.AlarmType"
    df = pd.read_sql(AlarmInfo_sql, con=conn)  # 172.16.12.4  Grid_PositionPhoto
    all_datas = []
    for name, group in df.groupby('RunTime'):
        # 变成列表镶嵌字典
        d = group.to_dict(orient='records')
        for i in range(0, len(d), 150):
            item = {}
            b = d[i:i + 150]
            item['jiange_time'] = name
            item['data'] = b
            all_datas.append(item)
    print(all_datas)
    r.sadd("test4", all_datas)
    print("save to redis successfully")

scheduler = BlockingScheduler()
scheduler.add_job(B, trigger='interval', seconds=20)
scheduler.start()