# -*- coding:utf-8 -*-
# pip install sqlalchemy
# pip install pymssql

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session


class MSSQL:
    def __init__(self,server,user,pwd,db):
        #ms = MSSQL(host="172.16.12.35",user="sa",pwd="sa",db="SmallIsBeautiful_2017-03-15")
        engine = create_engine("mssql+pymssql://"+user+":"+pwd+"@"+server+"/"+db,
        max_overflow = 0,  # 超过连接池大小外最多创建的连接
        pool_size = 50,  # 连接池大小
        pool_timeout = 50,  # 池中没有线程最多等待的时间，否则报错
        pool_recycle = -1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
)
        SessionFactory = sessionmaker(bind=engine)
        self.session = scoped_session(SessionFactory)


    def ExecQuery(self,sql):
        # reslist = ms.ExecQuery("select top 10 *  from T_Sys_SQLOptimization order by id desc ")
        cursor = self.session.execute(sql)
        result = cursor.fetchall()
        self.session.remove()
        return result


    def ExecNonQuery(self,sql):
        cursor = self.session.execute(sql)
        self.session.commit()
        self.session.remove()

