# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/15 10:34
# ☯ File  : alarm_main_action.py

import datetime
import threading
from dbPart.Mongo_Connect import MongoConnect
from dbPart.MSSql_SqlHelp_copy import MSSQL

from alarm_2_action import OverTarget
from alarm_3_action import Get_PointRelation_List, SmartAlarm
from alarm_4_action import SteadyValue
from alarm_1_comon_action import mongo_data, append_data, result_insertDB
from apscheduler.schedulers.blocking import BlockingScheduler


client1 = MongoConnect(host_port="172.16.12.170:27017,172.16.12.110:27017,172.16.12.161:27017", db="OriginalData")
ms = MSSQL(server="172.16.12.4", user="sa", pwd="@admin123", db="Grid_PositionPhoto")
ms2 = MSSQL(server="172.16.12.3", user="sa", pwd="@admin123", db="AlarmInfo_test")
time_anomaly = float(0.2)
# print('系数',time_anomaly)


class EarlyWarning:

    def main(ruledatas):
        """
        按照报警类型分类 :离线报警 超指标报警 数据智能报警 恒定值报警  规则信息:ruledatas
        """
        AlarmType_list = list(set(i['AlarmType'] for i in ruledatas['data'] if i['AlarmType']))
        print(AlarmType_list)

        # （1） 离线报警
        if '1' in AlarmType_list:
            # 获取data信息的列表
            rule_list = list([i for i in ruledatas['data'] if i['AlarmType'] == "1"])  # ['2','3']
            print(rule_list)

            # 获取站点信息的列表 所有站点mn【用这个站点去查mongo数据】
            all_station_list = list(set([i['DGIMN'] for i in ruledatas['data'] if i['AlarmType'] == "1"]))
            print(all_station_list)

            TimeInterval_list = list(set([i['TimeInterval'] for i in ruledatas['data'] if i['AlarmType'] == "1"]))  # 返回时间间隔
            print(TimeInterval_list)
            TimeInterval = TimeInterval_list[0]
            # print('往前推得时间间隔',TimeInterval)

            # 往前推一分钟
            nowTime = datetime.datetime.now() - datetime.timedelta(minutes=1)
            e_time = datetime.datetime.strftime(nowTime, '%Y-%m-%d %H:%M:00')
            minute_int = nowTime.strftime('%M')
            eTime = e_time.split(':')[0] + ':' + minute_int[0] + '0:00'

            # 离线结束时间
            endtime_strp = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:00')

            # 离线开始时间
            startime_strp = endtime_strp - datetime.timedelta(seconds=int(TimeInterval))

            # 获取学习后的mongo数据
            tablename = 'RealTimeData'  #mongo数据库

            # 根据 sqlserver 里面的站点信息   去查询 Mongo里面的信息
            all_stations_data = mongo_data(startime_strp, endtime_strp, all_station_list, tablename, 0)
            print("我是all_stations_data", all_stations_data)

            # 报错信息
            except_info = '站点离线'
            # 报错类型
            ExceptType = "Device"
            # 因子
            PollutantCode = ''

            for every_rule in rule_list:
                print('every_rule11', every_rule)      # sqlserver 返回的数据
                # {'DGIMN': 'AQMS-1100-2020096571', 'PollutantCode': '', 'AlarmRule': 'len(X) == 0', 'AlarmType': '1', 'IsDevice': '1', 'TimeInterval': '600', 'RunTime': '600', 'IsPush': '1', 'PushType': 'WeChatPush', 'PushObject': '没问题', 'PushInterval': '1800'}
                point_id = every_rule['DGIMN']  # 返回sqlserver 站点

                # point_id 是规则库返回的数据，all_stations_data 是mongo库返回的数据
                single_point_list = append_data(point_id, all_stations_data, '')
                # print('我是single_data_list', single_point_list)

                if len(single_point_list) == 0:
                    # # 获取报警规则公式
                    AlarmRule = every_rule['AlarmRule']
                    AlarmType = every_rule['AlarmType']
                    new_AlarmRule = AlarmRule.replace("X", "{}".format(single_point_list))
                    # print('我是Over_formula', new_AlarmRule)
                    p_result = eval(new_AlarmRule)
                    # print('我是p_result', p_result)
                    if p_result == True:
                        # 存储时间
                        now_time = datetime.datetime.now()
                        storage_time = now_time.strftime('%Y-%m-%d %H:%M:%S')
                        # utc时间
                        data_time_utc = endtime_strp - datetime.timedelta(hours=8)  # 现在的时间往后推迟8小时的时间 也就是UTC时间
                        # print('我是ata_time_utc', data_time_utc)
                        # (server="172.16.12.4", user="sa", pwd="@admin123", db="Grid_PositionPhoto")
                        # 连续报警的站点不推送(获取该站点最新一条的报警时间，拿最新的报警时间和现在报警时间的间隔，判断是否有数据，如果有数据就报警，没数据就不报)
                        historysql = "SELECT top 1 local_time FROM T_Grid_PointDataExcept WHERE device_id = '" + point_id + "' ORDER BY local_time DESC  "  #降序排列
                        # print("我是historysql",historysql)

                        historydata = ms.ExecQuery(historysql)
                        # print('我是historydata', historydata)

                        if historydata:
                            history_stime = historydata[0][0]
                            point_id_list = [point_id]
                            # print("我是point_id_list", point_id_list)

                            single_point_data = mongo_data(history_stime, endtime_strp, point_id_list, tablename, 200)
                            # print('我是single_point_data', single_point_data)

                            history_point_info = len(list(single_point_data))
                            # print('我是history_point_info', history_point_info)

                            if history_point_info > 0:
                                # print('当前时间utc', (datetime.datetime.now() - datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:00'))
                                # print('推送的时间', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:00'))
                                # print("站点", point_id)
                                # print("报警信息", '站点离线')
                                result_insertDB(point_id, data_time_utc, except_info, ExceptType, storage_time, AlarmType, PollutantCode, every_rule['PushInterval'])
                                print("type (1) save to successfully")
                        else:
                            #没有历史数据直接报警
                            # print(point_id, data_time_utc, except_info, ExceptType, storage_time)
                            result_insertDB(point_id, data_time_utc, except_info, ExceptType, startime_strp, AlarmType, PollutantCode, every_rule['PushInterval'])
                            print("haha type(1) save to successfully")


        # （2） 超指标报警2
        if '2' in AlarmType_list:
            print('开始超指标报警2')
            # 超指标报警时间＋取mongo数据
            # 规则信息：ruledata 就是传进来a、

            # 规则信息
            rule_list = list([i for i in ruledatas['data'] if i['AlarmType'] == "2"])

            # 站点信息
            stations_list = list(set([i['DGIMN'] for i in ruledatas['data'] if i['AlarmType'] == "2"]))

            # 往前推的时间
            TimeInterval_list = list(set([i['TimeInterval'] for i in ruledatas['data'] if i['AlarmType'] == "2"]))
            TimeInterval = TimeInterval_list[0]

            now_time = (datetime.datetime.now() - datetime.timedelta(minutes=1))
            second_int = int(now_time.strftime('%S'))
            s_header = now_time.strftime('%Y-%m-%d %H:%M')
            r = 30 if second_int >= 30 else '00'
            eTime = s_header + ":" + str(r)
            eTime = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:%S')

            # '超标结束时间'，往前推30秒，或推 TimeInterval 秒
            over_endTime = eTime

            # 超标开始时间
            if TimeInterval == 0:
                over_starTime = over_endTime - datetime.timedelta(seconds=int(30))
            else:
                over_starTime = over_endTime - datetime.timedelta(seconds=int(TimeInterval))

            tablename = "RealTimeData"
            over_all_station_data = mongo_data(over_starTime, over_endTime, stations_list, tablename, 0)
            # print("我是over_all_station_data",over_all_station_data)
            for every_rule in rule_list:
                point_id = every_rule['DGIMN']
                PollutantCode = every_rule['PollutantCode']

                # 返回的是 查询到mongo里面 MonitorValue 的值
                single_data_list = append_data(point_id, over_all_station_data, PollutantCode)
                # print("我是single_data_list", single_data_list)

                A = every_rule['AlarmRule']
                gongshi = A.split(',')
                # print('我是gongshi', gongshi)
                AlarmRule = gongshi
                AlarmType = every_rule['AlarmType']
                PushInterval = every_rule['PushInterval']
                # print('推送间隔',PushInterval)
                OverTarget(over_endTime, point_id, single_data_list, AlarmRule, AlarmType, PollutantCode, PushInterval)

        # （3） 数据智能报警 3
        if '3' in AlarmType_list:
            print('开始数据智能报警3')
            # 报警规则
            rule_list = list([i for i in ruledatas['data'] if i['AlarmType'] == "3"])

            # 站点信息 sqlserver
            station_list = list(set([i['DGIMN'] for i in ruledatas['data'] if i['AlarmType'] == "3"]))

            # 对应所有3公里站点
            distance = 3000

            # 根据规则表里面的（station_list）获取站点表172.16.12.4（T_Grid_PointRelation）三公里范围内的站点列表
            point_diatance = Get_PointRelation_List(station_list, distance)
            # print('对应所有3公里站点', point_diatance)

            all_station_data = []
            for point_mn in point_diatance:
                all_station_data.append(point_mn[0])
                all_station_data.append(point_mn[1])
            # print(list(set(all_station_data)))

            # 去重之后（获取站点表（T_Grid_PointRelation）三公里范围内的站点列表）
            station_data = list(set(all_station_data))
            # print("我是station_data", station_data)

            now_time = (datetime.datetime.now() - datetime.timedelta(minutes=1))
            # 获取整秒
            second_int = int(now_time.strftime('%S'))
            s_header = now_time.strftime('%Y-%m-%d %H:%M')
            r = 30 if second_int >= 30 else '00'
            eTime = s_header + ":" + str(r)

            # 结束时间
            endTime = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:%S')
            # 开始时间
            starTime = endTime - datetime.timedelta(seconds=30)
            tablename = "RealTimeData"

            # 根据三公里范围内的站点 station_data（也就是point_diatance去重之后） 获取mongo数据
            now_stationdata = mongo_data(starTime, endTime, station_data, tablename, 0)

            # 获取前一时刻mongo数据
            up_sTime = endTime -datetime.timedelta(seconds=60)
            up_eTime = endTime -datetime.timedelta(seconds=30)
            # print('前一时刻开始时间',up_sTime)
            # print('前一时刻结束时间',up_eTime)

            # 根据三公里范围内的站点 station_data 获取mongo 数据
            last_stationdata = mongo_data(up_sTime, up_eTime, station_data, tablename, 0)

            for every_rule in rule_list:
                for now_station in now_stationdata:
                    if now_station['DataGatherCode'] == every_rule['DGIMN']:
                        for data in now_station['RealDataList']:
                            if data['PollutantCode'] == every_rule['PollutantCode']:
                                DataGatherCode = every_rule['DGIMN']
                                PollutantCode = data['PollutantCode']
                                # print("我是mongo 里面的 PollutantCode", PollutantCode)
                                MonitorValue = float(data['MonitorValue'])
                                # print("我是mongo 里面的 MonitorValue",  MonitorValue)
                                # 智能报警
                                SmartAlarm(endTime, DataGatherCode, PollutantCode, MonitorValue,
                                                        point_diatance, now_stationdata, last_stationdata)

        # （4） 恒定值报警 4
        if '4' in AlarmType_list:
            print('进入恒定值报警判断')
            # 恒定值报警4
            # 规则信息：ruledatas
            rule_list = list([i for i in ruledatas['data'] if i['AlarmType'] == "4"])

            # 规则里面所有站点mn【用这个站点去查mongo数据】
            station_list = list(set([i['DGIMN'] for i in ruledatas['data'] if i['AlarmType'] == "4"]))
            # 往后推的时间
            TimeInterval_list = list(set([i['TimeInterval'] for i in ruledatas['data'] if i['AlarmType'] == "4"]))
            TimeInterval = TimeInterval_list[0]

            # 现在的时间
            now_time = (datetime.datetime.now() - datetime.timedelta(minutes=1))
            second_int = int(now_time.strftime('%S'))
            s_header = now_time.strftime('%Y-%m-%d %H:%M')
            r = 30 if second_int >= 30 else '00'
            eTime = s_header + ":" + str(r)

            # 恒定值结束时间
            endTime = datetime.datetime.strptime(eTime, '%Y-%m-%d %H:%M:%S')

            # 恒定值开始时间
            startTime = endTime - datetime.timedelta(seconds=int(TimeInterval))

            # print('恒定值开始时间', startTime)
            # print('恒定值结束时间', endTime)

            # 获取mongo数据
            tablename = "RealTimeData"

            # 根据规则里面取到的站点信息 查询到mongo里面的站点信息
            all_stationdata = mongo_data(startTime, endTime, station_list, tablename, 0)
            # print("我是all_stationdata", all_stationdata)

            for every_rule in rule_list:
                point_id = every_rule['DGIMN']
                PollutantCode = every_rule['PollutantCode']
                PushInterval = every_rule['PushInterval']
                AlarmType = every_rule['AlarmType']

                # 规则
                AlarmRule = every_rule['AlarmRule']
                if AlarmRule != None:
                    A = every_rule['AlarmRule']
                    AlarmRule = A.split(',')
                    # print("我是AlarmRule",AlarmRule)
                else:
                    AlarmRule = ""

                # 根据 规则的站点信息 返回 mongo 里面的站点信息
                single_data_list = append_data(point_id, all_stationdata, PollutantCode)
                # print("我是single_data_list",single_data_list)
                SteadyValue(endTime, point_id, PollutantCode, single_data_list, AlarmRule, AlarmType,
                                         PushInterval)

    def start_jobs(spark):
        print('rrrrr',spark)
        scheduler = BlockingScheduler()
        scheduler.add_job(func=EarlyWarning.main, args=[spark],trigger="interval", seconds=int(spark['time_interval']))

        scheduler.start()

    def update_thread(spark):
        thread = threading.Thread(target=EarlyWarning.start_jobs, args=[spark])
        thread.start()
        print("start")

    def TimerWays(job_func, data_list):
        job_thread = threading.Thread(target=job_func, args=[data_list])
        job_thread.start()





    def start_jobs(spark):
        print('rrrrr',spark)
        scheduler = BlockingScheduler()
        scheduler.add_job(func=EarlyWarning.run, seconds=int(20))
        scheduler.add_job(func=EarlyWarning.main, args=[spark],trigger="interval", seconds=int(spark['time_interval']))

        scheduler.start()

    def update_thread(spark):
        thread = threading.Thread(target=EarlyWarning.start_jobs, args=[spark])
        thread.start()
        print("start")










