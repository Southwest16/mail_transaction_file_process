# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: SQL语句


# 最大值查询
origin_max_time_select_sql = "SELECT max(email_id), max(email_receive_time) from db.transaction"


# 原始数据插入语句
origin_insert_sql = """
    insert into db.transaction (email_id,email_uuid,email_subject,email_from_addr,
    email_from_name,email_receive_time,compressed_file_name,raw_data,file_name,webpage_uuid) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
