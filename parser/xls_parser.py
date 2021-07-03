# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: Excel文件解析

import re
import json
import xlrd
from transaction.utils import tools


def col_to_dic(file_data, item, file_name):
    """
    表格中的列组成字典
    :param file_data:
    :param item:
    :param file_name:
    :return:
    """
    for info in xls_parse(file_data, file_name):
        item['raw_data'] = info
        item['file_name'] = file_name.split("/")[-1]
        item['webpage_uuid'] = tools.hash_sha1(info)
        yield item


def xls_parse(file_data, file_name):
    """
    xls/xls文件解析
    :param file_data:
    :param file_name:
    :return:
    """
    # 打开一个电子表格文件进行数据提取
    data = xlrd.open_workbook(file_contents=file_data.read(), encoding_override='utf-8')
    # data = xlrd.open_workbook(file_contents=BytesIO(open("", "rb").read()).read(), encoding_override='utf-8')

    # 一个Excel文件中所有sheet的名称
    sheets = data.sheet_names()
    # 通过sheet名获取对应的sheet表内容
    table = data.sheet_by_name(sheets[0])

    start_row = 0  # 起始行
    rows = table.nrows  # 表格总行数

    # 每一行数据是否存在指定的字符()
    # 找出表头位于第几行, 从表头的下一行开始读取表记录
    public_column = "关键字1|关键字2|...|关键字n"
    header = []

    # 获取有效记录的起始行号和表头
    for i in range(0, rows):
        if re.search(public_column, str(table.row_values(i)).replace(" ", "")):
            # header = [re.sub("\\s+", "", str(v)) for v in table.row_values(i)]
            header = table.row_values(i)
            start_row = i + 1
            break

            # 遍历表记录
    for i in range(start_row, rows):
        # 表记录值与对应的列名组成字典
        col_dic = dict(zip(header, table.row_values(i)))

        # 移除空行
        if "".join(str(e).strip() for e in table.row_values(i)) == "":
            continue

        # 字典转json
        raw_item = json.dumps(col_dic, ensure_ascii=False)
        yield raw_item
