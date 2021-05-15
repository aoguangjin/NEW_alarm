# -*- coding: utf-8 -*-
# @Time    : 2019/10/16 0016 上午 9:28
# @Author  : WuHuiMin
# @File    : Info_send.py
# @Software: PyCharm
# 日志


# from wxpy import *
# bot = Bot(cache_path=True)
#
# from logflile import funLog
# logger = funLog()
#
# from config.settings import *
import requests
#
# def WX_send(region_name,push_info):
#     group_list = []
#     for single_city in city_group:
#         if single_city['city_name'] == region_name:
#             group_list.append(single_city['group_name'])
#     # for single_group in group_list:
#     # print('群群群去海南',single_group)
#     print('群群群',push_info)
#     # group_receiver = ensure_one(bot.groups().search(u"%s"%(single_group)))
#     group_receiver = ensure_one(bot.groups().search(u"%s"%("啦啦")))
#     group_receiver.send(u"%s" %(push_info))
#     # print('%s 群发送成功'%(single_group))
#
#     # my_friend = bot.friends().search('呜啦啦')[0]
#     # my_friend.send(u"%s"%(push_info))



def get_param(dgimn, TagName_value, alarmid, localtime, devicename, alias_list):
    # 获取接口数据
    # t3 = time.time()
    try:
        url = 'http://172.16.12.65:4064/WebGrid_Api/rest/AtmosphereApi/NewAlarmGridData/JPpushAlarm?authorCode=48f3889c-af8d-401f-ada2-c383031af92d'
        garams = {
            # "alias": ['1'],
            "alias": alias_list,
            "TagName": TagName_value,
            "sendName": "系统",
            "DGIMN": dgimn,
            "alarmid": alarmid,
            "localtime": localtime,
            "devicename": devicename
        }
        print(garams)
        resp = requests.post(url, data=garams,timeout=(3,7)).text
        print('成功',resp)
    except Exception as e:
        print('极光推送异常',e)

