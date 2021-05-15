# -*- coding: utf-8 -*-
# @Time    : 2020/8/4 0004 下午 1:39
# @Author  : WuHuiMin
# @File    : MSSql_SqlHelp11.py
# @Software: PyCharm

# -*- coding:utf-8 -*-

import pymssql
import time

# # 单例模式
# def singleton(cls):
#     _instance = {}
#     def wapper(*args, **kwargs):
#         if cls not in _instance:
#             _instance[cls] = cls(*args, **kwargs)
#         return _instance[cls]
#     return wapper

# @singleton



class MSSQL:
    def __init__(self,server,user,pwd,db):
        self.server = server
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        print('连接数据库....')
        if not self.db:
            # raise(NameError,"没有设置数据库信息")
            print('没有设置数据库信息')
        self.conn = pymssql.connect(server=self.server,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            # raise(NameError,"连接数据库失败")
            print("连接数据库失败")
        else:
            return cur

    def ExecQuery(self,sql):
        try:
            cur = self.__GetConnect()
            cur.execute(sql)
            print('执行sql')
            resList = cur.fetchall()
            print('执行完毕后，获取结果')
            #查询完毕后必须关闭连接
            cur.close()
            self.conn.close()
            print('查询结果集')
            return resList
        except Exception as e:
            print('查询异常1', e)
            time.sleep(2)
            cur = self.__GetConnect()
            cur.execute(sql)

            resList = cur.fetchall()

            # 查询完毕后必须关闭连接
            cur.close()
            self.conn.close()
            print('再次查询结果集' )
            return resList

    def ExecNonQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        cur.close()
        self.conn.commit()
        self.conn.close()






# @singleton
# class SecondMSSQL:
#     def __init__(self, server, user, pwd, db):
#         self.server = server
#         self.user = user
#         self.pwd = pwd
#         self.db = db
#
#     def __GetConnect(self):
#         if not self.db:
#             raise (NameError, "没有设置数据库信息")
#         self.conn = pymssql.connect(server=self.server, user=self.user, password=self.pwd, database=self.db,
#                                     charset="utf8")
#         cur = self.conn.cursor()
#         if not cur:
#             raise (NameError, "连接数据库失败")
#         else:
#             return cur
#
#     def ExecQuery(self, sql):
#         try:
#             cur = self.__GetConnect()
#             cur.execute(sql)
#             resList = cur.fetchall()
#
#             # 查询完毕后必须关闭连接
#             self.conn.close()
#             return resList
#         except Exception as e:
#             print('查询异常2', e)
#             time.sleep(1)
#             cur = self.__GetConnect()
#             cur.execute(sql)
#             resList = cur.fetchall()
#             # 查询完毕后必须关闭连接
#             self.conn.close()
#             return resList
#
#     def ExecNonQuery(self, sql):
#         cur = self.__GetConnect()
#         cur.execute(sql)
#         self.conn.commit()
#         self.conn.close()
#
# @singleton
# class ThirdMSSQL:
#     def __init__(self, server, user, pwd, db):
#         self.server = server
#         self.user = user
#         self.pwd = pwd
#         self.db = db
#
#     def __GetConnect(self):
#         if not self.db:
#             raise (NameError, "没有设置数据库信息")
#         self.conn = pymssql.connect(server=self.server, user=self.user, password=self.pwd, database=self.db,
#                                     charset="utf8")
#         cur = self.conn.cursor()
#         if not cur:
#             raise (NameError, "连接数据库失败")
#         else:
#             return cur
#
#     def ExecQuery(self, sql):
#         try:
#             cur = self.__GetConnect()
#             cur.execute(sql)
#             resList = cur.fetchall()
#
#             # 查询完毕后必须关闭连接
#             self.conn.close()
#             return resList
#         except Exception as e:
#             print('查询异常3',e)
#             time.sleep(1)
#             cur = self.__GetConnect()
#             cur.execute(sql)
#             resList = cur.fetchall()
#
#             # 查询完毕后必须关闭连接
#             self.conn.close()
#             return resList
#
#     def ExecNonQuery(self, sql):
#         cur = self.__GetConnect()
#         cur.execute(sql)
#         self.conn.commit()
#         self.conn.close()


                # ms = MSSQL(host=".",user="sa",pwd="sa",db="SmallIsBeautiful")
# reslist = ms.ExecQuery("select * from Space0002A")
# for i in reslist:
#     print(i)

# newsql="update Space0002A set column_0='%s' where id='%s'" %(u'2012年测试',u'2')
# print(newsql)
# ms.ExecNonQuery(newsql.encode('utf-8'))
