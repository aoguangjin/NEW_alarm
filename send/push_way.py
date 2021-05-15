# -*- coding: utf-8 -*-
# @Time    : 2020/9/10 0010 下午 5:24
# @Author  : WuHuiMin
# @File    : push_way.py
# @Software: PyCharm
import os
import re
import datetime
import requests
from dbPart.MSSql_SqlHelp import MSSQL
from configparser import ConfigParser
from logflile import funLog
logger = funLog()


# 初始化类
cp = ConfigParser()
host_path = os.path.abspath(os.path.abspath('.'))
config_file_name = '\config.cfg'
config_path = host_path + config_file_name
# print('配置文件位置',config_path)
# 读取带中文的配置文件需要加encoding='utf-8' 或者 "utf8"
cp.read(config_path,encoding='utf-8-sig')
server1=cp.get('sql_info','server1')
user1=cp.get('sql_info','user1')
pwd1=cp.get('sql_info','pwd1')

server2=cp.get('sql_info','server2')
user2=cp.get('sql_info','user2')
pwd2=cp.get('sql_info','pwd2')
db2=cp.get('db_info','db2')


# MS Sql Server 链接字符串
# ms = MSSQL(server="172.16.12.4", user="sa", pwd="@admin123", db="Grid_PositionPhoto")
ms = MSSQL(server=server2, user=user2, pwd=pwd2, db=db2)



# 对应因子限值
def factor_value(factor):
    factor_info = [{'factor': 'a05024', 'factor_name': 'O3'},
                   {'factor': 'a21004', 'factor_name': 'NO2'},
                   {'factor': 'a21005', 'factor_name': 'CO'},
                   {'factor': 'a21026', 'factor_name': 'SO2'},
                   {'factor': 'a34002', 'factor_name': 'PM10'},
                   {'factor': 'a34004', 'factor_name': "PM2.5"},
                   {'factor': 'a99054', 'factor_name': "TVOC"},
                   {'factor': 'a21001', 'factor_name': "NH3"},
                   {'factor': 'a21028', 'factor_name': "H2S"},
                   {'factor': 'ou', 'factor_name': "OU"}
                   ]
    factor_data = []
    for f in factor_info:
        if f['factor'] == factor:
            factor_data.append({'factor_name': f['factor_name']})
            break
    return factor_data


def wx_push(group_name, info):
    url = 'http://172.16.9.15:8675//WX_send'
    data = {
        'group_name': group_name,
        'push_info': info
    }
    r = requests.post(url, data=data).text
    print(r)

def SMSSend(sms_info):
    url = "http://172.16.9.15:8000/SendSMS"
    data = sms_info
    print('短信data',data)
    print(data['item_site_name'],data['item_name'],data)
    log_info = '站点：{}，报警类型：{}，传入参数：{}'.format(data['item_site_name'],data['item_name'],data)
    logger.info(log_info)
    res = requests.post(url,json=data).json()
    print(res)


# 极光推送调用接口
def jg_get_param(dgimn, alarmid, localtime, devicename):
    # 获取接口数据
    try:
        url = 'http://172.16.12.16:4065/GridWebApi/WebGrid_Api2.0/rest/AtmosphereApi/NewAlarmGridData/AlipushAlarm'
        garams = {
            # "alias": ['1'],
            # "alias": alias_list,
            # "TagName": TagName_value,
            "sendName": "系统",
            "DGIMN": dgimn,
            "alarmid": alarmid,
            "localtime": localtime,
            "devicename": devicename
        }
        resp = requests.post(url, data=garams, timeout=(3, 7)).text
        print(resp)
    except Exception as e:
        print('极光推送异常', e)

# 极光推送参数准备
def jg(utc_time,device_id,device_name):
    m_localtime_jiguang = utc_time + datetime.timedelta(hours=8)
    m_jiguangtui_time = utc_time.strftime('%Y-%m-%d %H:%M:%S')
    # 极光推送传入参数
    # 查询22数据库信息  后将参数传给极光
    jiguang_sql = "SELECT ID FROM T_Grid_PointDataExcept WHERE device_id = '%s' and utc_time ='%s' " % (
        device_id, m_jiguangtui_time)
    # mianyang_jiguang = ms4.ExecQuery(jiguang_sql.encode('utf-8'))
    mianyang_jiguang = ms.ExecQuery(jiguang_sql)
    if mianyang_jiguang:
        idd = list(mianyang_jiguang)[0][0]
        m_localtime = m_localtime_jiguang.strftime('%Y-%m-%d %H:%M:%S')
        t1 = m_localtime.split("-")
        m_jiguang_time = t1[1] + "-" + t1[2]
        # 调用极光
        jg_get_param(device_id, idd, m_jiguang_time, device_name)


def push_way(device_id, AlarmType, utc_time, except_info, push_info):
    print('hihihihihih',except_info)
    # 因子对应名字
    factor_name = factor_value(push_info[6])
    # factor_name = factor_value('a34004')
    print('因子name', factor_name)
    # 获取站点的名字
    # 根据设备号，去找站点的基本信息
    # DG_info_sql = "SELECT PointName,DGIMN FROM DB_Base_Product.dbo.T_Bas_CommonPoint  WHERE DGIMN = (SELECT PointMN FROM DB_Base_Product.dbo.T_Bas_PointDevice WHERE DeviceMN = '" + device_id + "')"
    DG_info_sql = "SELECT PointName,DGIMN FROM DB_Base_Product.dbo.T_Bas_CommonPoint  WHERE DGIMN = '" + device_id + "'"
    print('DG_info_sql',DG_info_sql)
    DG_info_data = ms.ExecQuery(DG_info_sql)
    mn_name = DG_info_data[0][0]

    # 推送信息格式整理
    # 离线报警1
    # 超指标报警2
    # 数据智能报警3
    # 恒定值报警4
    push_content = ''
    sms_info = ''
    if AlarmType == '1':
        # XXX站点已离线，请检查。
        # XXX站点已离线XXX分钟/小时，请尽快检查
        push_content = '离线报警：{}站点已离线，请检查'.format(mn_name)
        # push_content = '离线报警：{}站点已离线XXX分钟，请尽快检查。'.format(mn_name)
        sms_info = {'item_name': 'Offline','itme_send_number': '','item_site_name': mn_name}
    if AlarmType == '2':
        print('push_info----',push_info)
        alarm_gs =push_info[7]
        alarm_gs_split = alarm_gs.split(',')
        print('读取超指标公式分来',alarm_gs_split)
        lower_line = re.findall(r"\d+\.?\d*", alarm_gs_split[0])[0]
        high_line = re.findall(r"\d+\.?\d*", alarm_gs_split[1])[0]
        name_chaobiao = except_info.split('，')[0]
        value_data =except_info.split('，')[1]
        item_pollutant_value = except_info.split('：')[1] #只有浓度数值
        print('rrr',except_info)
        if name_chaobiao == '超指标报警':
            # push_content = '超标报警：站点名：{}，污染物：{}，超过浓度值：{}，请检查。'.format(mn_name, factor_name[0]['factor_name'], high_line)
            push_content = '超标报警：站点名：{}，污染物：{}，{}，超过浓度值：{}，请检查。'.format(mn_name, factor_name[0]['factor_name'],value_data,high_line)
            # 报警浓度值
            item_pollutant_value =except_info.split('：')[1]
            sms_info ={
                "item_name": "UpperLimit",
                "itme_send_number": "",
                "item_site_name": mn_name,
                "item_pollutant": factor_name[0]['factor_name'],
                "item_pollutant_value": item_pollutant_value,
                "item_pollutant_value_limit":high_line
            }
        if name_chaobiao == '低值报警':
            # push_content = '低值报警：站点名：{}，污染物：{}，低于浓度值：{}，请检查。'.format(mn_name, factor_name[0]['factor_name'],lower_line)
            push_content = '低值报警：站点名：{}，污染物：{}，{}，低于浓度值：{}，请检查。'.format(mn_name, factor_name[0]['factor_name'],value_data,lower_line)
            # 报警浓度值
            sms_info ={
                "item_name": "LowerLimit",
                "itme_send_number": "",
                "item_site_name": mn_name,
                "item_pollutant":  factor_name[0]['factor_name'],
                "item_pollutant_value": item_pollutant_value,
                "item_pollutant_value_limit": lower_line
            }

    if AlarmType == '3':
        # push_content = '综合数据报警：站点名：通航产业园，污染物：CO，浓度值：8.1'
        # push_content = '综合数据报警：站点名：{}，污染物：{}'.format(mn_name,except_info)
        alarm_time = (utc_time + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
        push_content = '综合数据报警：站点名：{}，污染物：{}，时间：{}'.format(mn_name,except_info,alarm_time)
        print('智能报警推送信息：', push_content)
        item_pollutant =  '，'.join(except_info.split('，')[:-1])
        item_pollutant_value = (except_info.split('，')[-1:])[0].split('：')[1]
        sms_info ={
            "item_name": "Synthesize",
            "itme_send_number": "",
            "item_site_name": mn_name,
            "item_pollutant": item_pollutant,
            "item_pollutant_value": item_pollutant_value,
            "item_time": alarm_time
        }

    if AlarmType == '4':
        only_value = re.findall(r"\d+\.?\d*", except_info)[0]
        print('only_value', only_value)
        only_time = round(int(push_info[8]) / 60)
        push_content = '恒定值报警：站点名：{}，污染物：{}，浓度值：{}，已连续{}分钟恒定值，请检查。'.format(mn_name, factor_name[0]['factor_name'],only_value, only_time)
        sms_info ={
            "item_name": "ConstantValue_MM",
            "itme_send_number": "",
            "item_site_name": mn_name,
            "item_pollutant": factor_name[0]['factor_name'],
            "item_pollutant_value": only_value,
            "item_time": only_time
        }
    push_info_list = push_info[3].split(',')
    # if push_info[3] == 'WeChatPush':
    if 'WeChatPush' in push_info_list:
        # 微信推送对象
        push_object = push_info[4].split(',')
        for p in push_object:
            print('p', p)
            if push_content:
                print('推送信息', push_content)
                wx_push(p, push_content)
                now_time = datetime.datetime.now()
                # logger.info('当前报警时间%s，推送群名：%s，站点mn：%s，推送信息：%s'%(now_time,p,device_id,push_content))
    if 'AuroraPush' in push_info_list:
        print('app推送')
        jg(utc_time, device_id, mn_name)

    if 'SMSPush' in push_info_list:
        print('短信推送')
        SMSSend(sms_info)

# SMSSend(
# {
#     "item_name": "UpperLimit",
#     "itme_send_number": "",
#     "item_site_name": '沙市区烟草',
#     "item_pollutant": 'Pm2.5',
#     "item_pollutant_value": '172',
#     "item_pollutant_value_limit": '75'
# }
# )
#
