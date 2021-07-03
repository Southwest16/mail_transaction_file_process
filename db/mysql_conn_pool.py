# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: MySQL数据库连接池、增删改查接口

import pymysql
from transaction.utils.logger import logger
from transaction.resources.config import db_config
from DBUtils.PooledDB import PooledDB, PooledDBError


class MySQLConnector():
    def __init__(self):
        pass

    @staticmethod
    def create_pool():
        pool = PooledDB(
            creator=pymysql,
            host=db_config()["host"],
            user=db_config()["user"],
            password=db_config()["password"],
            database=db_config()["database"],
            charset=db_config()["charset"],
            blocking=True,
            maxconnections=2,
            mincached=1,
            autocommit=True
        )
        return pool

    def fetch_one(self, pool, sql):
        try:
            conn = pool.connection()
            cursor = conn.cursor()

            cursor.execute(sql)
            return cursor.fetchone()
        finally:
            self.close(conn, cursor)

    def fetch_many(self, pool, sql_count, sql, size):
        try:
            conn = pool.connection()
            cursor = conn.cursor()

            cursor.execute(sql_count)
            cursor.fetchall()
            row_count = cursor.rowcount

            cursor.execute(sql)
            while row_count > 0:
                records = cursor.fetchmany(size)
                row_count -= size

                yield records
        finally:
            self.close(conn, cursor)

    def execute_many(self, pool, sql, args=None):
        """
        执行SQL语句
        :param pool:
        :param sql:
        :param args:
        :param commit:
        :return:
        """
        try:
            # 从连接池中获取一个连接
            conn = pool.connection()
            cursor = conn.cursor()

            # 是否传参(args: sequence或者mapping)
            # 批量执行多个SQL
            if args:
                cursor.executemany(sql, args)
            else:
                cursor.executemany(sql)

        except PooledDBError as e:
            logger.error("Failed to insert records into MySQL table {}".format(e))
        finally:
            self.close(conn, cursor)

    def close(self, conn, cursor):
        """
        :param conn:
        :param cursor:
        :return:
        """
        cursor.close()
        conn.close()
