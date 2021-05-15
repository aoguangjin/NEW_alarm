# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/17 15:26
# ☯ File  : alarm_4_action.py

import datetime
from alarm_1_comon_action import result_insertDB

# 恒定值报警
def SteadyValue(eTime, point_id, PollutantCode, single_data_list, AlarmRule, AlarmType, PushInterval):
    if single_data_list:
        p_result = list(set(single_data_list))
        # print('ooooo', p_result)
        if p_result:
            if len(p_result) == 1:
                # print('恒定值报警')
                # print(p_result[0])

                except_info = '恒定值为{}'.format(p_result[0])

                ExceptType = "OnlyAlarm"

                eTime_utc = eTime - datetime.timedelta(hours=8)

                # 当前报警时间
                storage_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                result_insertDB(point_id, eTime_utc, except_info, ExceptType, storage_time, AlarmType,
                                             PollutantCode, PushInterval)