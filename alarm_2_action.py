# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/17 15:34
# ☯ File  : alarm_2_action.py
import datetime
from alarm_1_comon_action import result_insertDB


# 超指标报警
def OverTarget(eTime, DataGatherCode, single_data_list, AlarmRule, AlarmType, PollutantCode, PushInterval):
    # single_data_list = [-1,-1,-1,-1,-1]
    # print('超指标列表',single_data_list)
    if single_data_list:
        # 最低指标
        lower_rule = AlarmRule[0]
        lower_result = judge_way(single_data_list, lower_rule)

        # 最高指标
        high_rule = AlarmRule[1]
        over_result = judge_way(single_data_list, high_rule)

        if lower_result == True:
            # print('超指标报警-低值报警')
            # 最低值
            min_value = min(single_data_list)
            # print(min(single_data_list))
            except_info = '低值报警，浓度值：{}'.format(min_value)
            ExceptType = "LowerAlarm"
            OverAlarmInfo(eTime, DataGatherCode, except_info, ExceptType, AlarmType, PollutantCode, PushInterval)

        if over_result == True:
            # print('超指标报警-高值报警')
            #  将报警信息存库并判断推送方式
            # 最高值
            max_value = max(single_data_list)
            # print(max(single_data_list))
            except_info = '超指标报警，浓度值：{}'.format(max_value)
            ExceptType = "OverAlarm"
            OverAlarmInfo(eTime, DataGatherCode, except_info, ExceptType, AlarmType, PollutantCode, PushInterval)


# 判断是否超标的函数
def judge_way(single_data_list, AlarmRule):
    judge_value = True
    # ValueRang<70   # single_data_list = [-1,-1,-1,-1,-1]
    for i in single_data_list:
        Over_formula = AlarmRule.replace("ValueRang", "{}".format(i))
        p_result = eval(Over_formula)
        if p_result == True:
            judge_value = True
        else:
            judge_value = False
            break
    return judge_value


# 判断是否是超标的函数 加 存储
def OverAlarmInfo(eTime, DataGatherCode, except_info, ExceptType, AlarmType, PollutantCode, PushInterval):
    eTime_utc = eTime - datetime.timedelta(hours=8)
    # 当前报警时间
    storage_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #  将报警信息存库并判断推送方式
    result_insertDB(DataGatherCode, eTime_utc, except_info, ExceptType, storage_time, AlarmType,
                                 PollutantCode, PushInterval)