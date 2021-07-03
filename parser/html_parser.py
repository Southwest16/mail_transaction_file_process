# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: HTML文件解析

import re
import json
from lxml.etree import HTML
from transaction.utils import tools


def col_to_dic(file_data, item, file_name):
    """
    表格中的列组成字典
    :param file_data:
    :param item:
    :param file_name:
    :return:
    """
    for info in html_parse(file_data):
        item['raw_data'] = info
        item['file_name'] = file_name.split("/")[-1]
        item['webpage_uuid'] = tools.hash_sha1(info)
        yield item


def html_parse(file_data):
    """
    html文件解析
    :param file_data:
    :param col_process:
    :return:
    """
    # 从一个字符串常量中解析HTML文档
    html = HTML(file_data.read())
    html_tables = html.findall("body/table")

    # 将HTML中的所有<table>都放到一个列表中, 以便后续统一处理
    table_rows = []
    for html_table in html_tables:
        table_rows.extend(list(html_table))

    rows = len(table_rows)  # 表格总行数
    start_row = 0  # 起始行

    # 每一行数据是否存在指定的字符()
    header_col_name = "关键字1|关键字2|...|关键字n"
    header = []

    # 获取有效记录的起始行号和表头
    for i in range(0, rows):
        # 取出HTML中每个<tr><td>中的值
        values = [col.text for col in table_rows[i]]
        if re.search(header_col_name, str(values).replace(" ", "")):
            header = values
            start_row = i + 1
            break

    # 遍历表记录
    for i in range(start_row, rows):
        values = [col.text for col in table_rows[i]]
        # 表记录值与对应的列名组成字典
        col_dic = dict(zip(header, values))

        # 移除空行
        if "".join(str(e) for e in values) == "":
            continue

        # 字典转json
        raw_item = json.dumps(col_dic, ensure_ascii=False)
        yield raw_item
