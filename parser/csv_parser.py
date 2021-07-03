# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: csv文件解析

import re
import csv
import json
import chardet
import traceback
from io import StringIO
from transaction.utils import tools
from transaction.utils.logger import logger


def col_to_dic(file_data, item, file_name):
    """
    表格中的列组成字典
    :param file_data:
    :param item:
    :param file_name:
    :return:
    """
    for info in csv_parse(file_data, file_name):
        item['raw_data'] = info
        item['file_name'] = file_name.split("/")[-1]
        item['webpage_uuid'] = tools.hash_sha1(info)
        yield item


def csv_parse(file_data, file_name):
    """
    csv文件解析
    :param file_data:
    :param file_name:
    :return:
    """
    file_bytes_obj = file_data.read()
    encode = chardet.detect(file_bytes_obj)["encoding"]

    file_string_io = None
    try:
        file_string_io = StringIO(file_bytes_obj.decode(encode).replace('\r', '\r\n'))
    except Exception as e:
        try:
            file_string_io = StringIO(file_bytes_obj.decode("gb18030").replace('\r', '\r\n'))
        except Exception as e:
            logger.error(traceback.format_exc())

    # # 编码范围: gb18030 > gb2312 > gbk
    # file_string_io = StringIO(file_bytes_obj.decode("gbk").replace('\r', '\r\n'))

    if file_string_io is not None:
        csv_data = list(csv.reader(file_string_io))

        start_row = -1  # 起始行
        rows = len(csv_data)  # 表格总行数

        public_column = "关键字1|关键字2|...|关键字n"
        header = []

        # 获取有效记录的起始行号和表头
        for i in range(0, rows):
            # 每一行数据是否存在指定的字符()
            if re.search(public_column, re.sub(r"\s+", "", str(csv_data[i]))):
                if len(csv_data[i]) == 1:
                    header = csv_data[i][0].split("\t")
                else:
                    header = csv_data[i]
                start_row = i + 1
                break

        # 遍历表记录
        for i in range(start_row, rows):
            # 表记录值与对应的列名组成字典
            col_dic = {}
            col_dic = dict(zip(header, csv_data[i]))

            # 移除空行
            if "".join(str(e).strip() for e in csv_data[i]) == "":
                continue

            # 字典转json
            raw_item = json.dumps(col_dic, ensure_ascii=False)
            yield raw_item
