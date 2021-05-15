# -*- coding: utf-8 -*-
# @Time    : 2019/10/14 0014 下午 5:24
# @Author  : WuHuiMin
# @File    : get_mongo_data.py
# @Software: PyCharm
import pymongo
client = pymongo.MongoClient('mongodb://172.16.12.19:27017,172.16.12.20:27017,172.16.12.21:27017')
db = client.OriginalData
from config.settings import *


def data_mongo(table_name,point_list,time_data,factor):
    # print(table_name)
    ddd = db[table_name].aggregate([
        {'$match': {"DataGatherCode": {"$in": point_list},
                    "MonitorTime": time_data
                    },
         },
        {"$unwind": "$RealDataList"},
        {
            "$match": {"RealDataList.PollutantCode": factor}
        },

        {
            "$group": {"_id": {"DataGatherCode": '$DataGatherCode', 'MonitorTime': '$MonitorTime'}
                , "DataGatherCode": {'$first': '$DataGatherCode'},
                       "MonitorTime": {'$first': '$MonitorTime'},
                       "MonitorFactor": {"$first": "$RealDataList.PollutantCode"},
                       "MonitorValue": {"$first": "$RealDataList.MonitorValue"}}

        },
        {
            "$sort": {"MonitorTime": -1}
        },
    ])
    return ddd

def mongo_all_data(temp_point_list, eTime,odor_time, factor):
    point_para_id = []
    point_pm_id = []
    point_odor_id = []
    for p in temp_point_list:
        if p[4] == '颗粒物':
            point_pm_id.append(p[0])
        if p[4] == '恶臭监测站':
            point_odor_id.append(p[0])
        else:
            point_para_id.append(p[0])

    # 六参数或者其他取数据
    six_data= data_mongo('OriginalRealTimeData', point_para_id, eTime, factor)
    # print('除颗粒物取数据===》',len(list(six_data)))
    # 颗粒物
    PM_data = data_mongo('RealTimeData', point_pm_id, eTime, factor)

    # 恶臭取数据
    odor_data = data_mongo('RealTimeData', point_odor_id, odor_time, factor)




    # print('颗粒物恶臭取数据===》',list(PM_data))
    # 得到momgo数据后  将其重复数据去重 同一时刻有不为0的多值  随便取一个值
    last_result = []
    temp_data_l = list(six_data) + list(PM_data) + list(odor_data)
    if temp_data_l:
        last_result.append(temp_data_l[0])
        for dict in temp_data_l:
            k = 0
            for item in last_result:
                if dict['DataGatherCode'] != item['DataGatherCode'] or dict['MonitorTime'] != item['MonitorTime']:
                    if float(dict['MonitorValue']) != 0:
                        k = k + 1
                else:
                    break
                if k == len(last_result):
                    last_result.append(dict)
    # print('iiiii',last_result)
    return last_result



