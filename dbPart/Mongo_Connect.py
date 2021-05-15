# -*- coding: utf-8 -*-
# @Time    : 2020/8/4 0004 上午 11:58
# @Author  : WuHuiMin
# @File    : Mongo_Connect.py
# @Software: PyCharm

#mongo连接服务
import pymongo

class MongoConnect:
    def __init__(self,host_port,db):
        self.host_port = host_port
        self.db = db

    # 连接mogo数据库
    def __GetConnect(self):
        if not self.db:
            print('没有设置数据库信息')
            raise(NameError,"没有设置数据库信息")
        self.conn = pymongo.MongoClient(self.host_port)
        client = self.conn.OriginalData
        if not client:
            raise(NameError,"连接数据库失败")
        else:
            return client

    # 查询mogo数据库
    def FindData(self,sql,tablename,sort,limit):

        '''
        :param sql: 查询语句
        :param tablename: 表名
        :param sort: 排序
        :param limit:个数限制
        :return:
        '''
        client = self.__GetConnect()
        if sort:
            data_list = client[tablename].find(sql,no_cursor_timeout = True).sort(sort).limit(limit)
        else:
            data_list = client[tablename].find(sql,no_cursor_timeout = True).limit(limit)
        data_l = []
        for data_li in data_list:
            data_l.append(data_li)
        return data_l

    # 关闭mogo数据库
    def close_connect(self):
        client = self.__GetConnect()
        client.close()
