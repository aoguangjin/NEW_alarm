# # -*- coding:utf-8 -*-
# import pymssql
# import pyodbc
# import time
#
#
# # # 单例模式
# # def singleton(cls):
# #     _instance = {}
# #     def wapper(*args, **kwargs):
# #         if cls not in _instance:
# #             _instance[cls] = cls(*args, **kwargs)
# #         return _instance[cls]
# #     return wapper
# #
# # @singleton
#
# class MSSQL:
#     def __init__(self,server,user,pwd,db):
#         self.server = server
#         self.user = user
#         self.pwd = pwd
#         self.db = db
#
#     def __GetConnect(self):
#         if not self.db:
#             # raise(NameError,"没有设置数据库信息")
#             print(NameError,"没有设置数据库信息")
#         self.conn = pymssql.connect(host=self.server, user=self.user, password=self.pwd, database=self.db,
#                                     charset="utf8")
#         try:
#             self.conn = pymssql.connect(host=self.server,user=self.user,password=self.pwd,database=self.db,charset="utf8")
#             # print("使用pymssql连接数据库")
#         except Exception as e:
#             self.conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+self.server+';DATABASE='+self.db+';UID='+self.user+';PWD='+self.pwd)
#             # print("使用pyodbc连接数据库")
#         cur = self.conn.cursor()
#         if not cur:
#             # raise(NameError,"连接数据库失败")
#             print(NameError,"连接数据库失败")
#         else:
#             return cur
#
#     def ExecQuery(self,sql):
#         try:
#             cur = self.__GetConnect()
#             cur.execute(sql)
#             print('执行sql')
#             resList = cur.fetchall()
#             print('执行完毕后，获取结果')
#             #查询完毕后必须关闭连接
#             # cur.close()
#             # self.conn.close()
#             print('查询结果集')
#             return resList
#         except Exception as e:
#             print('查询异常1', e)
#             time.sleep(2)
#             cur = self.__GetConnect()
#             cur.execute(sql)
#             resList = cur.fetchall()
#             # 查询完毕后必须关闭连接
#             # cur.close()
#             # self.conn.close()
#             print('再次查询结果集' )
#             return resList
#
#
#     def ExecNonQuery(self,sql):
#         cur = self.__GetConnect()
#         cur.close()
#         cur.execute(sql)
#
#         self.conn.commit()
#         self.conn.close()
#
#
#
#
#
#
#
#
#
#
#
# # ms = MSSQL(host="172.16.12.35",user="sa",pwd="sa",db="SmallIsBeautiful_2017-03-15")
# # ms = MSSQL(host=".",user="sa",pwd="sa",db="SmallIsBeautiful")
#
# # reslist = ms.ExecQuery("select * from T_PC_User")
# # for i in reslist:
# #     print(i)
#
# # newsql="update Space0002A set column_0='%s' where id='%s'" %(u'2012年测试',u'2')
# # print(newsql)
# # ms.ExecNonQuery(newsql.encode('utf-8'))





# -*- coding:utf-8 -*-
# import pymssql
import pyodbc
import time


# 单例模式
def singleton(cls):
    _instance = {}
    def wapper(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]
    return wapper
@singleton
class MSSQL:
    def __init__(self, server, user, pwd, db):
        self.server = server
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            # raise(NameError,"没有设置数据库信息")
            print(NameError, "没有设置数据库信息")
        # self.conn = pymssql.connect(host=self.server, user=self.user, password=self.pwd, database=self.db,
        #                             charset="utf8")
        try:
            # self.conn = pymssql.connect(host=self.server,user=self.user,password=self.pwd,database=self.db,charset="utf8")
            self.conn = pyodbc.connect(
                'DRIVER={SQL Server Native Client 11.0};Mars_Connection=yes;SERVER=' + self.server + ';DATABASE=' + self.db + ';UID=' + self.user + ';PWD=' + self.pwd)
            # print("使用pymssql连接数据库")
        except Exception as e:
            self.conn = pyodbc.connect(
                'DRIVER={SQL Server Native Client 11.0};Mars_Connection=yes;SERVER=' + self.server + ';DATABASE=' + self.db + ';UID=' + self.user + ';PWD=' + self.pwd)
            # print("使用pyodbc连接数据库")
        cur = self.conn.cursor()
        if not cur:
            # raise(NameError,"连接数据库失败")
            print(NameError, "连接数据库失败")
        else:
            return cur

    def ExecQuery(self, sql):
        # try:
        cur = self.__GetConnect()
        try:
            cur.execute(sql)
            try:
                resList = cur.fetchall()
                try:
                    # self.conn.close()
                    # test=1
                    try:
                        print(resList)
                        return resList
                    except Exception as e:
                        print('方法返回', sql, e)
                except Exception as e:
                    print('关闭链接', sql, e)
            except Exception as e:
                print('内存', sql, e)
        except Exception as e:
            print('游标执行', sql, e)
            # self.conn.close()
            time.sleep(5)
            return MSSQL.ExecQuery(self, sql)

            # print('执行sql')

            # print('执行完毕后，获取结果')
            # 查询完毕后必须关闭连接

            # print('查询结果集')

        # except Exception as e:
        #     print('游标异常', sql, e)



            # time.sleep(2)
            # cur = self.__GetConnect()
            # cur.execute(sql)
            # resList = cur.fetchall()
            # # 查询完毕后必须关闭连接
            # self.conn.close()
            # print('再次查询结果集' )
            # return resList

    def ExecNonQuery(self, sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        # self.conn.close()








        # ms = MSSQL(host="172.16.12.35",user="sa",pwd="sa",db="SmallIsBeautiful_2017-03-15")
        # ms = MSSQL(host=".",user="sa",pwd="sa",db="SmallIsBeautiful")

        # reslist = ms.ExecQuery("select * from T_PC_User")
        # for i in reslist:
        #     print(i)

        # newsql="update Space0002A set column_0='%s' where id='%s'" %(u'2012年测试',u'2')
        # print(newsql)
        # ms.ExecNonQuery(newsql.encode('utf-8'))