# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: 配置信息


# 数据存入数据库配置
def db_config():
    db = {
        "host": "192.168.10.1",
        "port": "3306",
        "user": "root",
        "password": "123456",
        "database": "db",
        'charset': 'utf8'
    }
    return db


# 接收流水邮箱账号信息
mail_account = {
    "host": "pop3.xxx.com",
    "user": "Mike",
    "password": "123456"
}