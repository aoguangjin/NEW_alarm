# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/15 9:39
# ☯ File  : run.py

import pandas as pd
import pymssql
import time
import schedule
from alarm_main_action import EarlyWarning


def run():
    conn = pymssql.connect(server='172.16.12.4', user="sa", password="@admin123", database="Grid_PositionPhoto", charset="utf8")
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
            # [{'time_interval': '1800', 'data': [{'DGIMN': 'point0001', 'PollutantCode': '', 'AlarmRule': 'len(X) == 0', 'AlarmType': '1', 'IsDevice': '1', 'TimeInterval': '1800', 'RunTime': '1800', 'IsPush': '1', 'PushType': 'AuroraPush', 'PushObject': '', 'PushInterval': '0'}]}]
            # print(all_datas)
    for ruledatas in all_datas:
        jiange_time = ruledatas['jiange_time']
        schedule.every(int(jiange_time)).seconds.do(EarlyWarning.TimerWays, EarlyWarning.main, ruledatas)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run()


