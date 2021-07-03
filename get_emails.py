# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: 获取邮件列表

import base64
import poplib
import zipfile
import rarfile
import traceback
from io import BytesIO
from datetime import datetime
from transaction.resources.config import *
from transaction.utils.logger import logger
from transaction.get_files import get_files_from_zip, get_files_from_rar
from transaction.get_compressed_pkg import get_mail_list
from transaction.db.mysql_conn_pool import MySQLConnector
from transaction.db.sql_stm import origin_max_time_select_sql, origin_insert_sql


def parse(pop, conn, max_email_id, max_email_receive_time):
    """
    邮件解析入口
    :param pop: POP3客户端
    :param conn: 数据库连接
    :param max_email_id: 表中最大邮件索引
    :param max_email_receive_time: 表中最大邮件接收时间
    :return:
    """
    for data, item in get_mail_list(pop, max_email_id, max_email_receive_time):
        item['compressed_file_name'] = data.get("name", "")

        try:
            if data['name'].endswith(".zip"):  # zip格式的压缩文件
                zip_obj = zipfile.ZipFile(BytesIO(base64.b64decode(data['file_data'].encode())))

                logger.info(str(datetime.now()) + " 邮件索引：" + str(item["email_id"]) + " --- 邮件主题："
                            + item["email_subject"] + " ------------------ 解析开始 ")

                origin_json_records = []  # 原始数据json
                origin_json_value_records = []  # 原始数据值
                for record in get_files_from_zip(zip_obj, item):
                    record["raw_data"] = str(record["raw_data"])
                    origin_json_records.append(record.copy())
                    origin_json_value_records.append(tuple(record.values()))

                if len(origin_json_value_records) == 0:
                    continue

                # 写入外部数据库
                conn.execute_many(pool, origin_insert_sql, origin_json_value_records)
                logger.info(str(datetime.now()) + " 邮件索引：" + str(item["email_id"]) + " --- 邮件主题：" + item[
                    "email_subject"] + " ------------------ 解析结束 \n")

                # logger.info(str(datetime.now()) + " 邮件索引：" + str(item["email_id"]) + " --- 邮件主题：" + item["email_subject"] + " ------------------ 清洗开始 ")
                # clean(origin_json_records)  # 数据清洗
                # logger.info(str(datetime.now()) + " 邮件索引：" + str(item["email_id"]) + " --- 邮件主题：" + item["email_subject"] + " ------------------ 清洗结束 \n")

                zip_obj.close()
            elif data['name'].endswith(".rar"):  # rar格式的压缩文件
                rar_obj = rarfile.RarFile(BytesIO(base64.b64decode(data['file_data'].encode())))

                logger.info(str(datetime.now()) + " 邮件索引：" + str(item["email_id"]) + " --- 邮件主题："
                            + item["email_subject"] + " ------------------ 解析开始 ")
                origin_json_records = []  # 原始数据json
                origin_json_value_records = []  # 原始数据值
                for record in get_files_from_rar(rar_obj, item):
                    record["raw_data"] = str(record["raw_data"])
                    origin_json_records.append(record.copy())
                    origin_json_value_records.append(tuple(record.values()))

                if len(origin_json_value_records) == 0:
                    continue

                # 写入外部数据库
                conn.execute_many(pool, origin_insert_sql, origin_json_value_records)
                logger.info(str(datetime.now()) + " 邮件索引：" + str(item["email_id"]) + " --- 邮件主题："
                            + item["email_subject"] + " ------------------ 解析结束 \n")

                # logger.info(str(datetime.now()) + " 邮件索引：" + str(item["email_id"]) + " --- 邮件主题：" + item["email_subject"] + " ------------------ 清洗开始 ")
                # clean(origin_json_records)  # 数据清洗
                # logger.info(str(datetime.now()) + " 邮件索引：" + str(item["email_id"]) + " --- 邮件主题：" + item["email_subject"] + " ------------------ 清洗结束 \n")

                rar_obj.close()
        except Exception as e:
            logger.error(traceback.format_exc())


if __name__ == '__main__':
    pop = poplib.POP3_SSL(mail_account.get('host', ""))
    try:
        # 邮箱登录
        pop.user(mail_account.get('user', ""))
        pop.pass_(mail_account.get("password", ""))
    except poplib.error_proto as e:
        logger.error("Login failed: " + e)
    else:
        conn = None
        max_email_id = -1
        max_email_receive_time = ""
        try:
            # 初始化MySQL连接池
            conn = MySQLConnector()
            pool = conn.create_pool()

            # 获取表中最大邮件索引, 以从下一个索引开始获取邮件
            result_set = conn.fetch_one(pool, origin_max_time_select_sql)
            max_email_id = result_set[0]
            max_email_receive_time = result_set[1]
        except Exception as e:
            logger.error(traceback.format_exc())
        else:
            # 数据抽取解析
            parse(pop, conn, max_email_id, max_email_receive_time)
    finally:
        # 退出邮箱
        pop.quit()
