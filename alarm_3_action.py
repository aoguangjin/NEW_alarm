# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/17 14:57
# ☯ File  : alarm_3_action.py

import datetime
import numpy as np
from dbPart.MSSql_SqlHelp_copy import MSSQL


ms2 = MSSQL(server="172.16.12.3", user="sa", pwd="@admin123", db="AlarmInfo_test")
time_anomaly = float(0.2)

# 获取关联点位列表
def Get_PointRelation_List(point_list, distance):

    '''
    :param point_list: 站点列表
    :param distance: 公里数
    :return: 关联站点代码列表（含自身）
    '''

    point_list_id = str(point_list).replace('[', '').replace(']', '') if point_list else "''"
    sql1 = "select device_id, association_device_id from T_Grid_PointRelation where distance <= '3000' and device_id in (" + point_list_id + ") and association_device_id in (SELECT device_id FROM T_Grid_PointInfo WHERE  device_type != '国控')"
    temp_point_list = ms2.ExecQuery(sql1)
    # print('获取完成3000米站点')
    return temp_point_list


# 智能报警
def SmartAlarm(sTime, DataGatherCode, PollutantCode, MonitorValue, point_diatance, now_stationdata,
               last_stationdata):
    # 时间异常
    time_warning_MoreTime(sTime, DataGatherCode, MonitorValue, PollutantCode, last_stationdata)

    # 空间异常
    Spatial_Outlier_MoreTime(sTime, DataGatherCode, MonitorValue, PollutantCode, point_diatance, now_stationdata)

# 时间异常调用的函数
def time_warning_MoreTime(sTime, DataGatherCode, MonitorValue, PollutantCode, last_stationdata):
    # print('进入时间异常')
    # 时间异常取上一时刻报警
    value_data = get_value(last_stationdata, DataGatherCode, PollutantCode)
    # print("我是value_data", value_data)
    # print('时间异常取上一时刻报警',value_data)

    if value_data:
        # print('开始时间异常')
        time_warning(sTime, DataGatherCode, MonitorValue, value_data['MonitorValue'], PollutantCode)


# 时间异常调用的函数
def get_value(last_stationdata, DataGatherCode, PollutantCode):
    # print('获取mongo的当前值或者上一个值')
    data_item = {}
    for every_station in last_stationdata:
        try:
            if every_station['DataGatherCode'] == DataGatherCode:
                for data in every_station['RealDataList']:
                    if data['PollutantCode'] == PollutantCode:
                        if float(data['MonitorValue']) != 0:
                            data_item['PollutantCode'] = data['PollutantCode']
                            data_item['MonitorValue'] = float(data['MonitorValue'])
                            break
        except Exception as e:
            print('循环mongo取值', e)
    # print('已整理好mongo值')
    return data_item


# 时间异常调用的的函数
def time_warning(sTime, DataGatherCode, MonitorValue, last_MonitorValue, PollutantCode):
    '''
    :param sTime: 时间
    :param DataGatherCode: mn号
    :param MonitorValue: 当前时刻值
    :param last_MonitorValue: 上一时刻值
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
        # print("我是up_MonitorValue",last_MonitorValue)
        if last_MonitorValue < 0:
            temp = MonitorValue * (-time_anomaly)
        else:
            temp = MonitorValue * time_anomaly
        # print(temp)
        print(MonitorValue - last_MonitorValue)
        if MonitorValue - last_MonitorValue > temp:
            print('时间异常咯')

            # except_info = "当前值：" + str(all_data[0]) + "-----前一个值：" + str(all_data[1]) + "-----系数：" + str(temp)
            except_info = "当前值：" + str(MonitorValue) + " ,前一个值：" + str(last_MonitorValue) + " ,超过上一个浓度值：" + str(
                round(MonitorValue - last_MonitorValue, 1))

            print("时间异常存库信息", DataGatherCode, sTime_utc.strftime('%Y-%m-%d %H:%M:%S'), 'TimeAnomaly', '',
                  except_info, '', str(MonitorValue), PollutantCode)

            insertDB(DataGatherCode, sTime_utc.strftime('%Y-%m-%d %H:%M:%S'), 'TimeAnomaly', '',
                                  except_info=except_info, except_factor=PollutantCode,
                                  except_value=str(MonitorValue))

            print('时间异常存库结束...')



# 空间异常调用的函数
def Spatial_Outlier_MoreTime(sTime, DataGatherCode, MonitorValue, PollutantCode, point_diatance, now_stationdata):
    # print('进入空间异常')

    # 取监测中心点和周边3公里站点的值
    value = Range_data(PollutantCode, DataGatherCode, point_diatance, now_stationdata)
    # print('空间异常;l获取3公里结束。。。')
    # print('周边站点的数据value', value)
    value.append(MonitorValue)
    # 空间异常计算
    Spatial_Outlier(sTime, DataGatherCode, PollutantCode, value, MonitorValue)


# 空间异常调用的函数
def Range_data(PollutantCode, DataGatherCode, point_diatance, now_stationdata):
    '''
    :param PollutantCode: 因子
    :param DataGatherCode: 站点mn
    :param point_diatance: 站点距离
    :param all_stationdata: mongo数据
    :return: 关联站点得mongo数据
    '''
    # print('空间异常：获取周边3公里....')
    mongo_data = []
    for every_data in now_stationdata:
        for data in every_data['RealDataList']:
            if data['PollutantCode'] == PollutantCode:
                for p_d in point_diatance:
                    # print("我是p_d[0]", p_d[0])
                    # print("我是p_d[1]", p_d[1])
                    if p_d[0] == DataGatherCode and p_d[1] == every_data['DataGatherCode']:
                        if float(data['MonitorValue']) != 0:
                            up_MonitorValue = float(data['MonitorValue'])
                            # print("嘿嘿嘿嘿嘿嘿嘿",  up_MonitorValue)
                            mongo_data.append(up_MonitorValue)
    return mongo_data


# 空间异常调用的函数
def Spatial_Outlier(sTime, DataGatherCode, PollutantCode, value, MonitorValue):
    # print('空间异常：计算开始')

    sTime_utc = sTime - datetime.timedelta(hours=8)
    all_data = [all_d for all_d in value if all_d != 0]

    # print('周边3公里站点的值', all_data)
    # print("哼哼哼哼", all_data, MonitorValue)

    if MonitorValue <= 0:
        # print('比较站点此刻缺少数据')
        return False

    elif len(all_data) > 2 and MonitorValue > 0:
        # 在python中计算一个多维数组的任意百分比分位数，此处的百分位是从小到大排列
        quantile_25 = np.percentile(all_data, 25)
        # print("我是quantile_25", quantile_25)

        quantile_75 = np.percentile(all_data, 75)
        # print("我是quantile_75", quantile_75)

        if MonitorValue >= quantile_75 + 1.5 * (quantile_75 - quantile_25):
            print('判断出空间异常')

            insertDB(DataGatherCode, sTime_utc.strftime('%Y-%m-%d %H:%M:%S'), 'SpatialAnomaly', 'max',
                                  '', PollutantCode, '')
            print('空间异常存库结束。。。')
            print('空间异常存库信息', DataGatherCode, PollutantCode, sTime_utc.strftime('%Y-%m-%d %H:%M:%S'),
                  'SpatialAnomaly', 'max',
                  '', PollutantCode, '')


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
    print("我是智能能报警", sql)
    isRepeat = ms2.ExecQuery(sql)
    print("我是isRepeat", isRepeat)

    if isRepeat[0][0] == 0:
        print("isRepeat[0][0]", isRepeat[0][0])
        sql = "insert into T_Grid_PointDataExcept_test(device_id,utc_time,except_type,except_tag,except_info ,except_diff,pollutant_name,except_value) values('%s','%s','%s','%s','%s','%s','%s','%s') " % (
            point_id, datatime, except_type, except_tag, except_info, '', except_factor, except_value)
        print("我是智能报警存储sql",sql)
        ms2.ExecNonQuery(sql)
        print('存库结束')


