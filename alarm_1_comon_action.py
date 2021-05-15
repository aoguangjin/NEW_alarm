# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/17 15:27
# ☯ File  : alarm_1_comon_action.py

import datetime
from dbPart.MSSql_SqlHelp_copy import MSSQL
from dbPart.Mongo_Connect import MongoConnect
from send.push_way import push_way

ms = MSSQL(server="172.16.12.4", user="sa", pwd="@admin123", db="Grid_PositionPhoto")
client1 = MongoConnect(host_port="172.16.12.170:27017,172.16.12.110:27017,172.16.12.161:27017", db="OriginalData")


# 读mongo原始数据,也就是后台入库的数据
def mongo_data(sTime, eTime, point_list, tablename, limit):
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
    # 离线开始时间  转成UTC时间
    sTime = datetime.datetime.utcfromtimestamp(sTime.timestamp())
    # 离线结束时间  转成UTC时间
    eTime = datetime.datetime.utcfromtimestamp(eTime.timestamp())
    all_data_sql = {"MonitorTime": {"$gt": sTime, "$lte": eTime}, "DataGatherCode": {"$in": point_list}}
    # print(all_data_sql)
    datas = client1.FindData(all_data_sql, tablename, sort, limit)
    return datas


# 设备离线/恒定值报警 合并站点
def append_data(every_point, all_stations_data, PollutantCode):
    single_point_list = []
    for point_data in all_stations_data:
        # 判断是否是相等的站点
        if every_point == point_data['DataGatherCode']:
            # print("我是PollutantCode", PollutantCode)
            if PollutantCode:
                # print("woowowowowowowowo")
                for data in point_data['RealDataList']:
                    # 判断因子是否相等
                    if data['PollutantCode'] == PollutantCode:
                        # 判断 监视的值是否等于零
                        if float(data['MonitorValue']) != 0:
                            up_MonitorValue = float(data['MonitorValue'])
                            single_point_list.append(up_MonitorValue)
            else:
                # print("huhuhuhuhhu")
                single_point_list.append(point_data['DataGatherCode'])

    return single_point_list



def result_insertDB(device_id, utc_time, except_info, ExceptType, storage_time, AlarmType, PollutantCode, PushInterval):
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
    sql = " select count(ID) from T_Grid_PointDataExcept where device_id = '%s' and utc_time = '%s' and pollutant_name = '%s' and AlarmType = '%s'" % (
        device_id, utc_time, PollutantCode, AlarmType)
    print("我是SQl:", sql)
    isRepeat = ms.ExecQuery(sql)
    print('我是isRepeat', isRepeat)
    if isRepeat[0][0] == 0:
        push_judge_info = push_judge(device_id, AlarmType, PollutantCode, utc_time, PushInterval)
        print("push_judge_info", push_judge_info)
        if push_judge_info == True:
            except_state = '0'
            sql = " insert into T_Grid_PointDataExcept(device_id,utc_time,except_state,except_info ,ExceptType,storage_time,AlarmType,pollutant_name) values('%s','%s','%s','%s','%s','%s','%s','%s') " % (
                device_id, utc_time, except_state, except_info, ExceptType, storage_time, AlarmType, PollutantCode)
            print('存库sql（插入）', sql)
            ms.ExecNonQuery(sql)

            # 推送报警信息（微信群，短信）
            push_sql = "SELECT a.DGIMN,a.AlarmType,a.IsPush,a.PushType,a.PushObject,a.PushInterval,b.PollutantCode,b.AlarmRule,b.TimeInterval FROM Base_PushRuleInfo a INNER JOIN Base_AlarmRuleInfo b on  a.DGIMN = b.DGIMN and a.AlarmType = b.AlarmType  WHERE a.DGIMN = '" + device_id + "' and  a.AlarmType = '" + AlarmType + "' AND b.PollutantCode = '" + PollutantCode + "'"
            # print(push_sql)
            push_info = ms.ExecQuery(push_sql)
            if push_info:
                push_infos = push_info[0]
                if push_infos[2] == '1':
                    # device_id + AlarmType ==> sql报警站点信息
                    print('推送部分：',device_id, AlarmType,utc_time,except_info,push_infos)
                    # push_way(device_id, AlarmType, utc_time, except_info, push_infos)


def push_judge(device_id, AlarmType, factor, utc_time, pushInterval):
    # 查数据库中上个报警的时间点对比
    uptime_sql = "SELECT top 1 utc_time FROM T_Grid_PointDataExcept WHERE device_id =  '" + device_id + "' and  AlarmType = '" + AlarmType + "' and pollutant_name = '" + factor + "' ORDER BY utc_time DESC "
    print('查数据库中上个报警的时间sql', uptime_sql)
    uptime_data = ms.ExecQuery(uptime_sql)
    print("uptime_data",uptime_data)
    print('上个报警的时间uptime_data', uptime_data)
    if pushInterval != None:
        if uptime_data:
            # time_inter = single_point[9]
            # 推送间隔
            time_inter = pushInterval
            print('time_inter', uptime_data[0][0], time_inter)
            add_uptime = uptime_data[0][0] + datetime.timedelta(seconds=int(time_inter))
            print('对比结果：',utc_time, add_uptime)
            if utc_time > add_uptime:
                # print('推送存库')
                return True
        else:
            return True

