# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/15 10:12
# ☯ File  : warning.py

# -*- coding: utf-8 -*-
# @Time    : 2020/7/14 0014 上午 10:39
# @Author  : WuHuiMin
# @File    : alarm_file.py
# @Software: PyCharm


# 使用老版本的定时方法，进行定时
import os
import datetime
import pymssql
import pandas as pd
import schedule
import time
import threading
import numpy as np

# mongo 服务
from dbPart.Mongo_Connect import MongoConnect
# sqlserver 服务
from dbPart.MSSql_SqlHelp_copy import MSSQL
from send.push_way import push_way
# ConfigParser 是用来读取配置文件的包。
from configparser import ConfigParser


# 初始化类
cp = ConfigParser()
host_path = os.path.abspath(os.path.abspath('.'))
config_file_name = '\config.cfg'
config_path = host_path + config_file_name

# 读取带中文的配置文件需要加encoding='utf-8' 或者 "utf8"
cp.read(config_path,encoding='utf-8-sig')
server1=cp.get('sql_info','server1')
user1=cp.get('sql_info','user1')
pwd1=cp.get('sql_info','pwd1')

server2=cp.get('sql_info','server2')
user2=cp.get('sql_info','user2')
pwd2=cp.get('sql_info','pwd2')

db1=cp.get('db_info','db1')
db2=cp.get('db_info','db2')

# mongodb连接信息
mongodb_port=cp.get('mongodb_info','host_port')
mongodb_db=cp.get('mongodb_info','db')
# print(' mongodb连接信息---')
# print(mongodb_port)
# print(mongodb_db)

# 各个报警的用到的表信息
alarm_1=cp.get('mongodbtable_info','alarm_1')
alarm_2=cp.get('mongodbtable_info','alarm_2')
alarm_3=cp.get('mongodbtable_info','alarm_3')
alarm_4=cp.get('mongodbtable_info','alarm_4')
# print('各个报警的用到的表信息===')
# print(alarm_1)
# print(alarm_2)
# print(alarm_3)
# print(alarm_4)


# 智能报警系数
time_anomaly=float(cp.get('coeff_info','time_anomaly'))
# print('系数',time_anomaly)
# print(server2, user2, pwd2, db2)

# MS Sql Server 链接字符串
ms = MSSQL(server=server2, user=user2, pwd=pwd2, db=db2)
ms2 = MSSQL(server=server1, user=user1, pwd=pwd1, db=db1)
conn = pymssql.connect(server=server2,user=user2,password=pwd2,database=db2, charset="utf8")
# MongoDB 数据库链接
client1 = MongoConnect(host_port=mongodb_port,db=mongodb_db)

# db1 = AlarmInfo_test
# db2 = Grid_PositionPhoto
# db3 = EnergyData


class EarlyWarning:

    # 插入数据
    def result_insertDB(device_id, utc_time, except_info, ExceptType,storage_time,AlarmType,PollutantCode,PushInterval):
        '''
        :param device_id: 站点mn
        :param utc_time: utc时间
        :param except_info: 报警信息
        :param ExceptType: 报警类型（网格化要字段）
        :param storage_time: 当前时间
        :param AlarmType: 报警类型编码（eg：1,2,3，4）
        :param PollutantCode: 报警因子code
        :return: 存库
        '''
        # try:
        sql = " select count(ID) from T_Grid_PointDataExcept where device_id = '%s' and utc_time = '%s' and pollutant_name = '%s' and AlarmType = '%s'" % (
            device_id, utc_time,PollutantCode,AlarmType)
        isRepeat = ms.ExecQuery(sql)
        # print('iiiiiiiiii',isRepeat)
        if isRepeat[0][0] == 0:
            push_judge_info = EarlyWarning.push_judge(device_id, AlarmType, PollutantCode, utc_time,PushInterval)
            if push_judge_info == True:
                except_state = '0'
                sql = " insert into T_Grid_PointDataExcept(device_id,utc_time,except_state,except_info ,ExceptType,storage_time,AlarmType,pollutant_name) values('%s','%s','%s','%s','%s','%s','%s','%s') " % (device_id, utc_time, except_state, except_info, ExceptType, storage_time,AlarmType,PollutantCode)
                # print('存库sql（插入）',sql)
                ms.ExecNonQuery(sql)

                #存库后然后推送（微信/极光）[应该判断是否有推送：]
                # push_sql = "SELECT a.DGIMN,a.AlarmType,a.IsPush,a.PushType,a.PushObject,a.PushInterval,b.PollutantCode,b.AlarmRule,b.TimeInterval FROM Base_PushRuleInfo a INNER JOIN Base_AlarmRuleInfo b on a.DGIMN = b.DGIMN  WHERE a.DGIMN = 'f1-l6-0115' and  a.AlarmType = '3'"
                push_sql = "SELECT a.DGIMN,a.AlarmType,a.IsPush,a.PushType,a.PushObject,a.PushInterval,b.PollutantCode,b.AlarmRule,b.TimeInterval FROM Base_PushRuleInfo a INNER JOIN Base_AlarmRuleInfo b on  a.DGIMN = b.DGIMN and a.AlarmType = b.AlarmType  WHERE a.DGIMN = '" + device_id + "' and  a.AlarmType = '"+AlarmType+"' AND b.PollutantCode = '" + PollutantCode + "'"
                # print(push_sql)
                push_info = ms.ExecQuery(push_sql)
                # print('ppp', push_info)
                if push_info:
                    push_infos = push_info[0]
                    if push_infos[2] == '1':
                        # device_id + AlarmType ==> sql报警站点信息
                        # print('推送部分：',device_id, AlarmType,utc_time,except_info,push_infos)
                        push_way(device_id, AlarmType,utc_time,except_info,push_infos)
        #
        # except Exception as e:
        #     print("数据插入异常", e)

    # 智能报警（时间异常空间异常存库）
    def insertDB(point_id, datatime, except_type, except_tag, except_info, except_factor, except_value):
        '''
        :param point_id: 站点mn
        :param datatime: 时间
        :param except_type: 异常类型
        :param except_tag: 异常标签(eg：空间 max)
        :param except_info: 异常详情信息
        :param except_factor: 异常因子
        :param except_value:
        :return:
        '''
        sql = " select count(ID) from T_Grid_PointDataExcept_test where device_id = '%s' and utc_time = '%s' and except_type = '%s' and except_tag = '%s' and pollutant_name = '%s'" % (
            point_id, datatime, except_type, except_tag, except_factor)
        isRepeat = ms2.ExecQuery(sql)

        if isRepeat[0][0] == 0:
            sql = " insert into T_Grid_PointDataExcept_test(device_id,utc_time,except_type,except_tag,except_info ,except_diff,pollutant_name,except_value) values('%s','%s','%s','%s','%s','%s','%s','%s') " % (
                point_id, datatime, except_type, except_tag, except_info, '', except_factor, except_value)
            ms2.ExecNonQuery(sql)
            # print('存库结束')
        # else:
        #     print('已经存在')




    # 读取数据
    # 读mongo原始数据
    def mongo_data(sTime, eTime, point_list,tablename,limit):
        '''
        :param sTime: 开始时间
        :param eTime:  结束时间
        :param point_list: 站点列表
        :param tablename: 表名
        :param limit: 个数
        :return: mongo数据
        '''
        # print(sTime, eTime, point_list,tablename,limit)
        # 查询的表名 RealTimeData = tablename  链接数据库client = self.conn.OriginalData

        # sort传[],limit传0 则表示不排序，全部输出
        sort = []

        # 往前推的时间
        sTime = datetime.datetime.utcfromtimestamp(sTime.timestamp())

        # 现在的时间
        eTime = datetime.datetime.utcfromtimestamp(eTime.timestamp())

        all_data_sql = {"MonitorTime": {"$gt": sTime, "$lte": eTime}, "DataGatherCode": {"$in":point_list}}
        ddd = client1.FindData(all_data_sql,tablename,sort,limit)
        return ddd

    # 获取关联点位列表
    def Get_PointRelation_List(point_list, distance):
        '''
        :param point_list: 站点列表
        :param distance: 公里数
        :return: 关联站点代码列表（含自身）
        '''
        print(point_list)
        point_list_id = str(point_list).replace('[', '').replace(']', '') if point_list else "''"
        print(point_list_id)
        sql1 = " select device_id,association_device_id from T_Grid_PointRelation where distance <= '3000' and device_id in (" + point_list_id + ") and association_device_id in (SELECT device_id FROM T_Grid_PointInfo WHERE  device_type != '国控')"
        print('3000米',sql1)
        temp_point_list = ms2.ExecQuery(sql1)
        print('获取完成3000米站点')
        return temp_point_list


    def Range_data(PollutantCode,DataGatherCode, point_diatance, all_stationdata):
        '''
        :param PollutantCode: 因子
        :param DataGatherCode: 站点mn
        :param point_diatance: 站点距离
        :param all_stationdata: mongo数据
        :return: 关联站点得mongo数据
        '''
        # print('空间异常：获取周边3公里....')
        mongo_data = []
        for every_data in all_stationdata:
            for data in every_data['RealDataList']:
                if data['PollutantCode'] == PollutantCode:
                    for p_d in point_diatance:
                        if p_d[0] == DataGatherCode and p_d[1] == every_data['DataGatherCode']:
                            if float(data['MonitorValue']) != 0:
                                up_MonitorValue = float(data['MonitorValue'])
                                mongo_data.append(up_MonitorValue)
        return mongo_data

    #2、时间异常（计算加存库）
    def time_warning(sTime,DataGatherCode,MonitorValue,up_MonitorValue,PollutantCode):
        '''
        :param sTime: 时间
        :param DataGatherCode: mn号
        :param MonitorValue: 当前时刻值
        :param up_MonitorValue: 上一时刻值
        :param PollutantCode: 监测因子
        :return: 生成时间异常后存库
        '''
        # try:
        # sql = "select time_anomaly_all from T_Grid_PointInfo where device_id = '%s' " % (point_id)
        # print('sql系数',sql)
        # time_anomaly = ms.ExecQuery(sql.encode('utf-8'))
        # 存库时间
        sTime_utc = sTime - datetime.timedelta(hours=8)
        # print('获取时间异常时间---',)
        # 系数
        if time_anomaly != None and time_anomaly > 0:
            if up_MonitorValue < 0:
                temp = MonitorValue * (-time_anomaly)
            else:
                temp = MonitorValue * time_anomaly

            if MonitorValue - up_MonitorValue > temp:
                # print('时间异常咯')
                # except_info = "当前值：" + str(all_data[0]) + "-----前一个值：" + str(all_data[1]) + "-----系数：" + str(temp)
                except_info = "当前值：" + str(MonitorValue) + " ,前一个值：" + str(up_MonitorValue) + " ,超过上一个浓度值：" + str(round(MonitorValue - up_MonitorValue, 1))
                print("时间》》存库信息", DataGatherCode, sTime_utc.strftime('%Y-%m-%d %H:%M:%S'), 'TimeAnomaly', '', except_info,
                      '', str(MonitorValue), PollutantCode)
                EarlyWarning.insertDB(DataGatherCode, sTime_utc.strftime('%Y-%m-%d %H:%M:%S'), 'TimeAnomaly', '',except_info=except_info, except_factor=PollutantCode,except_value=str(MonitorValue))
                # print('时间异常存库结束...')
        # print('时间异常结束...')


    # 2、空间异常（计算加存库）
    def Spatial_Outlier(sTime,DataGatherCode,PollutantCode,value,MonitorValue):
        # print('空间异常：计算开始。。。。')
        sTime_utc = sTime - datetime.timedelta(hours=8)
        all_data = [all_d for all_d in value if all_d != 0]
        # print('周边3公里站点的值,...',all_data)
        # print('allllll=======',all_data,MonitorValue)
        if MonitorValue <= 0:
            # print('比较站点此刻缺少数据')
            return False
        elif len(all_data) >2  and MonitorValue > 0:
            # 在python中计算一个多维数组的任意百分比分位数，此处的百分位是从小到大排列
            quantile_25 = np.percentile(all_data, 25)
            quantile_75 = np.percentile(all_data, 75)
            if MonitorValue >= quantile_75 + 1.5 * (quantile_75 - quantile_25):
                # print('判断出空间异常')
                EarlyWarning.insertDB(DataGatherCode, sTime_utc.strftime('%Y-%m-%d %H:%M:%S'), 'SpatialAnomaly', 'max',
                                      '', PollutantCode, '')
                # print('空间异常存库结束。。。')
                # point_id, datatime, except_type, except_tag, except_info, except_factor, except_value
                print('空间异常存库信息',DataGatherCode,PollutantCode, sTime_utc.strftime('%Y-%m-%d %H:%M:%S'), 'SpatialAnomaly', 'max',
                                      '', PollutantCode, '')

        # print('空间异常结束....')


    # 1、时间异常
    def time_warning_MoreTime(sTime,DataGatherCode,MonitorValue,PollutantCode,up_all_stationdata):
        # print('进入时间异常')
        # 时间异常取上一时刻报警
        value_data = EarlyWarning.get_value(up_all_stationdata, DataGatherCode, PollutantCode)
        print(DataGatherCode)
        # print('时间异常取上一时刻报警',value_data)
        if value_data:
            # print('开始时间异常')
            EarlyWarning.time_warning(sTime, DataGatherCode, MonitorValue, value_data['MonitorValue'], PollutantCode)


    #1、空间异常
    def Spatial_Outlier_MoreTime(sTime,DataGatherCode,MonitorValue,PollutantCode,point_diatance,all_stationdata):
        # print('进入空间异常')
        # 取监测中心点和周边3公里站点的值
        value = EarlyWarning.Range_data(PollutantCode,DataGatherCode, point_diatance, all_stationdata)
        # print('空间异常;l获取3公里结束。。。')
        # print('周边站点的数据',value)
        value.append(MonitorValue)
        #空间异常计算
        o = EarlyWarning.Spatial_Outlier(sTime,DataGatherCode,PollutantCode,value,MonitorValue)
        # print('空间异常返回值',o)


    # 智能报警
    def SmartAlarm(sTime,DataGatherCode,PollutantCode,MonitorValue,point_diatance,all_stationdata,up_all_stationdata):
        # 时间异常
        EarlyWarning.time_warning_MoreTime(sTime,DataGatherCode,MonitorValue,PollutantCode,up_all_stationdata)
        # 空间异常
        EarlyWarning.Spatial_Outlier_MoreTime(sTime,DataGatherCode,MonitorValue,PollutantCode,point_diatance,all_stationdata)
        # print('时间-空间 都计算结束....')

    # 设备离线/恒定值报警 合并站点
    def append_data(every_point, mongodata, PollutantCode):
        single_point_list = []
        for point_data in mongodata:
            if every_point == point_data['DataGatherCode']:  #站点信息相等
                if PollutantCode:
                    for data in point_data['RealDataList']:
                        if data['PollutantCode'] == PollutantCode:
                            if float(data['MonitorValue']) != 0:
                                up_MonitorValue = float(data['MonitorValue'])
                                single_point_list.append(up_MonitorValue)
                else:
                    single_point_list.append(point_data['DataGatherCode'])
        return single_point_list

    def OverAlarmInfo(eTime,DataGatherCode,except_info,ExceptType,AlarmType,PollutantCode,PushInterval):

        eTime_utc = eTime - datetime.timedelta(hours=8)
        # 当前报警时间
        storage_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #  将报警信息存库并判断推送方式
        EarlyWarning.result_insertDB(DataGatherCode, eTime_utc, except_info, ExceptType, storage_time, AlarmType,
                                     PollutantCode, PushInterval)

    # 超指标报警
    def OverTarget(eTime,DataGatherCode,single_data_list,AlarmRule,AlarmType,PollutantCode,PushInterval):
        # single_data_list = [-1,-1,-1,-1,-1]
        # print('超指标列表',single_data_list)
        if single_data_list:

            # 最低指标
            lower_rule = AlarmRule[0]
            lower_result = EarlyWarning.judge_way(single_data_list, lower_rule)

            # 最高指标
            high_rule = AlarmRule[1]
            over_result = EarlyWarning.judge_way(single_data_list, high_rule)
            if lower_result == True:
                # print('超指标报警-低值报警')
                # 最低值
                min_value = min(single_data_list)
                print(min(single_data_list))
                except_info = '低值报警，浓度值：{}'.format(min_value)
                ExceptType = "LowerAlarm"
                EarlyWarning.OverAlarmInfo(eTime, DataGatherCode,except_info,ExceptType,AlarmType, PollutantCode, PushInterval)
            if over_result == True:
                # print('超指标报警-高值报警')
                #  将报警信息存库并判断推送方式
                # 最高值
                max_value = max(single_data_list)
                print(max(single_data_list))
                except_info = '超指标报警，浓度值：{}'.format(max_value)
                ExceptType = "OverAlarm"
                EarlyWarning.OverAlarmInfo(eTime, DataGatherCode, except_info, ExceptType, AlarmType, PollutantCode, PushInterval)


                # except_info = '超指标报警'
                # ExceptType = "OverAlarm"
                # eTime_utc = eTime-datetime.timedelta(hours=8)
                # # 当前报警时间
                # storage_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # #  将报警信息存库并判断推送方式
                #
                # # push_judge_info = EarlyWarning.push_judge(DataGatherCode, AlarmType, PollutantCode, eTime_utc, PushInterval)
                # # if push_judge_info == True:
                # EarlyWarning.result_insertDB(DataGatherCode, eTime_utc, except_info, ExceptType,storage_time,AlarmType,PollutantCode,PushInterval)
                #

    def judge_way(single_data_list,AlarmRule):
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





    # 恒定值报警
    def SteadyValue(eTime,point_id,PollutantCode,single_data_list,AlarmRule,AlarmType,PushInterval):
        # p_result = EarlyWarning.judge_way(single_data_list, AlarmRule)
        # single_data_list = [2,2,2,2,2,2,2,2]
        if single_data_list:
            p_result = list(set(single_data_list))
            # print('ooooo',p_result)
            if p_result:
                if len(p_result) == 1:
                    # print('恒定值报警')
                    except_info = '恒定值为{}'.format(p_result[0])
                    ExceptType = "OnlyAlarm"
                    eTime_utc = eTime - datetime.timedelta(hours=8)
                    # 当前报警时间
                    storage_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    #
                    # push_judge_info = EarlyWarning.push_judge(point_id, AlarmType, PollutantCode, eTime_utc,PushInterval)
                    # if push_judge_info == True:
                    #  将报警信息存库并判断推送方式
                    EarlyWarning.result_insertDB(point_id, eTime_utc, except_info, ExceptType,storage_time,AlarmType,PollutantCode,PushInterval)
                # else:
                #     print('正常')

    def get_value(all_stationdata, DataGatherCode,factor):
        # print('获取mongo的当前值或者上一个值')
        data_item  = {}
        for every_station in all_stationdata:
            try:
                if every_station['DataGatherCode'] == DataGatherCode:
                    for data in every_station['RealDataList']:
                        if data['PollutantCode'] == factor:
                            if float(data['MonitorValue']) != 0:
                                data_item['PollutantCode'] = data['PollutantCode']
                                data_item['MonitorValue'] = float(data['MonitorValue'])
                                break
            except Exception as e:
                print('循环mongo取值',e)
        # print('已整理好mongo值')
        return data_item


    # 存库前需要判断 推送间隔
    def push_judge(device_id,AlarmType,factor,utc_time,pushInterval):
        # 查数据库中上个报警的时间点对比
        uptime_sql = "SELECT top 1 utc_time FROM T_Grid_PointDataExcept WHERE device_id =  '" + device_id + "' and  AlarmType = '" + AlarmType + "' and pollutant_name = '" + factor + "' ORDER BY utc_time DESC "
        # print('查数据库中上个报警的时间sql',uptime_sql)
        uptime_data = ms.ExecQuery(uptime_sql)
        # print('上个报警的时间',uptime_data)
        if pushInterval !=None:
            if uptime_data:
                # time_inter = single_point[9]
                # 推送间隔
                time_inter = pushInterval
                # print('time_inter', uptime_data[0][0], time_inter)
                add_uptime = uptime_data[0][0] + datetime.timedelta(seconds=int(time_inter))
                # print('对比结果：',utc_time, add_uptime)
                if utc_time > add_uptime:
                    # print('推送存库')
                    return True
            else:
                return True


    def main(ruledatas):
        # 离线报警1
        # 超指标报警2
        # 数据智能报警3
        # 恒定值报警4
        # 按照报警类型分类
        # 规则信息:ruledatas

        AlarmType_list = list(set(i['AlarmType'] for i in ruledatas['data'] if i['AlarmType']))
        print(AlarmType_list)
        # 读数据之获取时间
        if '1' in AlarmType_list:
            guize_list = list([i for i in ruledatas['data'] if i['AlarmType'] == "1"])  #['2','3']
            print(guize_list)

            # 所有站点mn 【用这个站点去查mongo数据】
            all_station_list = list(set([i['DGIMN'] for i in ruledatas['data'] if i['AlarmType'] == "1"]))   # 返回站点信息
            TimeInterval_list = list(set([i['TimeInterval'] for i in ruledatas['data'] if i['AlarmType'] == "1"])) # 返回时间间隔
            TimeInterval = TimeInterval_list[0]
            # print('往前推的时间间隔',TimeInterval)

            # 往前推一分钟
            nowTime = datetime.datetime.now() - datetime.timedelta(minutes=1)
            e_time = datetime.datetime.strftime(nowTime, '%Y-%m-%d %H:%M:00')
            minute_int = nowTime.strftime('%M')
            eTime = e_time.split(':')[0] + ':' + minute_int[0] + '0:00'

            # 离线结束时间
            endtime_strp = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:00')

            # 离线开始时间
            starttime_strp = endtime_strp - datetime.timedelta(seconds=int(TimeInterval))

            # print('离线开始时间utc',sTime)
            # print('离线结束时间utc',eTime_strp)
            # sTime = datetime.datetime.strptime('2020-09-30 07:10:00','%Y-%m-%d %H:%M:%S')
            # eTime_strp = datetime.datetime.strptime('2020-09-30 07:20:00','%Y-%m-%d %H:%M:%S')

            # 获取学习后的mongo数据
            tablename = alarm_1  # RealTimeData

            # 根据 sqlserver 里面的站点信息   去查询 Mongo里面的信息
            all_stationdata = EarlyWarning.mongo_data(sTime, eTime_strp, all_station_list, tablename, 0)  # RealTimeData = tablename


            except_info = '站点离线'
            ExceptType = "Device"
            PollutantCode = ''

            for every_rule in guize_list:
                time.sleep(5)
                # print("我是all_stationdata", all_stationdata)
                # print('every_rule11', every_rule)      # sqlserver 返回的数据
                # {'DGIMN': 'AQMS-1100-2020096571', 'PollutantCode': '', 'AlarmRule': 'len(X) == 0', 'AlarmType': '1', 'IsDevice': '1', 'TimeInterval': '600', 'RunTime': '600', 'IsPush': '1', 'PushType': 'WeChatPush', 'PushObject': '没问题', 'PushInterval': '1800'}
                point_id = every_rule['DGIMN']  # 返回sqlserver 站点

                # 返回 列表
                single_data_list = EarlyWarning.append_data(point_id, all_stationdata, '')
                print('我是single_data_list',single_data_list)
                if len(single_data_list) == 0:
                    # # 获取报警规则公式
                    AlarmRule = every_rule['AlarmRule']
                    # AlarmRule = 'len(X)==0'
                    AlarmType = every_rule['AlarmType']

                    Over_formula = AlarmRule.replace("X", "{}".format(single_data_list))
                    p_result = eval(Over_formula)
                    print('我是p_result',p_result)
                    #
                    # # 获取报警规则公式
                    if p_result == True:
                        # 存储时间
                        now_time = datetime.datetime.now()
                        storage_time = now_time.strftime('%Y-%m-%d %H:%M:%S')
                        data_time_utc = eTime_strp - datetime.timedelta(hours=8)  # 现在的时间往后推迟8小时的时间
                        print('我是ata_time_utc',data_time_utc)

                        # 连续报警的站点不推送(获取该最新一条的报警时间，拿最新的报警时间和现在报警时间的间隔，判断是否有数据，如果有数据就报警，没数据就不报)
                        historysql = "SELECT top 1 local_time FROM T_Grid_PointDataExcept WHERE device_id = '" + point_id + "' ORDER BY local_time DESC  "
                        historydata = ms.ExecQuery(historysql)
                        print('我是historydata',historydata)
                        if historydata:
                            history_stime = historydata[0][0]
                            point_single_list = [point_id]
                            single_point_data = EarlyWarning.mongo_data(history_stime, eTime_strp, point_single_list,tablename, 200)
                            print('我是single_point_data',single_point_data)
                            history_point_info = len(list(single_point_data))
                            print('lllllllo0000',history_point_info)
                            if history_point_info > 0:
                                # print('当前时间utc', (datetime.datetime.now() - datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:00'))
                                # 推送的时间
                                # print('推送的时间', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:00'))
                                # print("站点id", point_id)
                                # print("报警信息", '站点离线')
                                # print(point_id, data_time_utc, except_info, ExceptType, storage_time)

                                # push_judge_info = EarlyWarning.push_judge(point_id, AlarmType, '', data_time_utc, every_rule['PushInterval'])
                                # if push_judge_info == True:
                                EarlyWarning.result_insertDB(point_id, data_time_utc, except_info, ExceptType, storage_time,AlarmType,PollutantCode,every_rule['PushInterval'])
                        else:
                            # print(point_id, data_time_utc, except_info, ExceptType, storage_time)
                            EarlyWarning.result_insertDB(point_id, data_time_utc, except_info, ExceptType, storage_time,AlarmType,PollutantCode,every_rule['PushInterval'])

        if '2' in AlarmType_list:
            print('开始超指标报警2')
            # 超指标报警时间＋取mongo数据
            # 规则信息：ruledata 就是传进来a、
            print(ruledata['data'])
            guize_list = list([i for i in ruledata['data'] if i['AlarmType'] == "2"])
            station_list = list(set([i['DGIMN'] for i in ruledata['data'] if i['AlarmType'] == "2"]))
            TimeInterval_list = list(set([i['TimeInterval'] for i in ruledata['data'] if i['AlarmType'] == "2"]))
            print('我是TimeInterval_list',TimeInterval_list)
            TimeInterval = TimeInterval_list[0]
            print('超指标站点：',guize_list)
            new_time = (datetime.datetime.now() - datetime.timedelta(minutes=1))
            second_int = int(new_time.strftime('%S'))
            s_header = new_time.strftime('%Y-%m-%d %H:%M')
            r = 30 if second_int >= 30 else '00'
            eTime = s_header + ":" + str(r)

            # 目前的时间
            eTime = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:%S')
            over_eTime =  eTime

            if TimeInterval == 0:
                over_sTime = over_eTime - datetime.timedelta(seconds=int(30))
            else:
                over_sTime = over_eTime - datetime.timedelta(seconds=int(TimeInterval))
            print("超标开始时间:", over_sTime)
            print('超标结束时间', over_eTime)

            tablename = alarm_2
            over_all_stationdata = EarlyWarning.mongo_data(over_sTime, over_eTime, station_list, tablename, 0)
            print(over_all_stationdata)
            for every_rule in guize_list:
                # print('ee',every_rule)
                point_id = every_rule['DGIMN']
                PollutantCode = every_rule['PollutantCode']
                single_data_list = EarlyWarning.append_data(point_id, over_all_stationdata, PollutantCode)
                A = every_rule['AlarmRule']
                gongshi = A.split(',')
                # print('gongshi', gongshi)
                AlarmRule = gongshi
                AlarmType = every_rule['AlarmType']
                PushInterval = every_rule['PushInterval']
                # print('推送间隔',PushInterval)
                EarlyWarning.OverTarget(eTime, point_id, single_data_list, AlarmRule,AlarmType,PollutantCode,PushInterval)


        # if '3' in AlarmType_list:
        #     print('开始数据智能报警3')
        #     # 数据智能报警3
        #     # 规则信息：  ruledata 就是传进来a
        #     guize_list = list([i for i in ruledata['data'] if i['AlarmType'] == "3"])
        #     # 所有站点mn【用这个站点去查mongo数据】
        #     station_list = list(set([i['DGIMN'] for i in ruledata['data'] if i['AlarmType'] == "3"]))
        #     # TimeInterval_list = list(set([i['TimeInterval'] for i in ruledata['data'] if i['AlarmType'] == "3"]))
        #     # TimeInterval = TimeInterval_list[0]
        #     # 对应所有3公里站点
        #     distance = 3000
        #     point_diatance = EarlyWarning.Get_PointRelation_List(station_list,distance)
        #     print('对应所有3公里站点', point_diatance)
        #     all_station_data = []
        #     for point_mn in point_diatance:
        #         all_station_data.append(point_mn[0])
        #         all_station_data.append(point_mn[1])
        #     # print(list(set(all_station_data)))
        #     # 智能报警需要取mongo数据得站点
        #     station_data = list(set(all_station_data))
        #
        #     new_time = (datetime.datetime.now() - datetime.timedelta(minutes=1))
        #     # 时间
        #     second_int = int(new_time.strftime('%S'))
        #     s_header = new_time.strftime('%Y-%m-%d %H:%M')
        #     r = 30 if second_int >= 30 else '00'
        #     eTime = s_header + ":" + str(r)
        #     eTime = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:%S')
        #     sTime = eTime - datetime.timedelta(seconds=30)
        #     # print("当前时间:", datetime.datetime.now())
        #     # print('开始时间1',sTime)
        #     # print('结束时间1',eTime)
        #
        #     tablename = alarm_3
        #     all_stationdata = EarlyWarning.mongo_data(sTime, eTime, station_data, tablename, 0)
        #
        #     # print('11111111111111111',all_stationdata)
        #     # 获取前一时刻mongo数据
        #     up_sTime = eTime -datetime.timedelta(seconds=60)
        #     up_eTime = eTime -datetime.timedelta(seconds=30)
        #     # print('前一时刻开始时间',up_sTime)
        #     # print('前一时刻结束时间',up_eTime)
        #
        #
        #     up_all_stationdata = EarlyWarning.mongo_data(up_sTime, up_eTime, station_data, tablename, 0)
        #     for every_rule in guize_list:
        #         for every_station in all_stationdata:
        #             if every_station['DataGatherCode'] == every_rule['DGIMN']:
        #                 for data in every_station['RealDataList']:
        #                     if data['PollutantCode'] == every_rule['PollutantCode']:
        #                         DataGatherCode = every_rule['DGIMN']
        #                         PollutantCode = data['PollutantCode']
        #                         MonitorValue = float(data['MonitorValue'])
        #                         # 智能报警
        #                         EarlyWarning.SmartAlarm(eTime, DataGatherCode, PollutantCode, MonitorValue,
        #                                                 point_diatance, all_stationdata, up_all_stationdata)
        #


        # if '4' in AlarmType_list:
        #     print('开始恒定值判断==》')
        #     # 恒定值报警4
        #     # 规则信息：ruledata 就是传进来a
        #     guize_list = list([i for i in ruledata['data'] if i['AlarmType'] == "4"])
        #     # 所有站点mn【用这个站点去查mongo数据】
        #     station_list = list(set([i['DGIMN'] for i in ruledata['data'] if i['AlarmType'] == "4"]))
        #     TimeInterval_list = list(set([i['TimeInterval'] for i in ruledata['data'] if i['AlarmType'] == "4"]))
        #     TimeInterval = TimeInterval_list[0]
        #
        #     # 时间
        #     new_time = (datetime.datetime.now() - datetime.timedelta(minutes=1))
        #     second_int = int(new_time.strftime('%S'))
        #     s_header = new_time.strftime('%Y-%m-%d %H:%M')
        #     r = 30 if second_int >= 30 else '00'
        #     eTime = s_header + ":" + str(r)
        #     eTime = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:%S')
        #     sTime = eTime - datetime.timedelta(seconds=int(TimeInterval))
        #
        #
        #     print('恒定值开始时间',sTime)
        #     print('恒定值结束时间',eTime)
        #
        #
        #     # 获取mongo数据
        #     tablename = alarm_4
        #     all_stationdata = EarlyWarning.mongo_data(sTime, eTime, station_list, tablename, 0)
        #     print(all_stationdata)
        #
        #     for every_rule in guize_list:
        #         # print('every_rule', every_rule)
        #         point_id = every_rule['DGIMN']
        #         PollutantCode = every_rule['PollutantCode']
        #         PushInterval = every_rule['PushInterval']
        #
        #         AlarmType = every_rule['AlarmType']
        #
        #         # 规则
        #         AlarmRule = every_rule['AlarmRule']
        #         if AlarmRule !=None:
        #             A = every_rule['AlarmRule']
        #             AlarmRule = A.split(',')
        #         else:
        #             AlarmRule = ""
        #         single_data_list = EarlyWarning.append_data(point_id, all_stationdata, PollutantCode)
        #         EarlyWarning.SteadyValue(eTime, point_id, PollutantCode, single_data_list, AlarmRule,AlarmType,PushInterval)
        # t2 = time.time()
        # print('用时=====',t2-t1)

    # 定时器
    def TimerWays(job_func,data_list):
        job_thread = threading.Thread(target=job_func,args=[data_list])
        job_thread.start()