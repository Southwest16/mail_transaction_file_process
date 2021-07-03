# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: PDF文件解析

import re
import json
import tabula
import pandas as pd
from transaction.utils import tools


def col_to_dic(file_data, item, file_name):
    """
    表格中的列组成字典
    :param file_data:
    :param item:
    :param file_name:
    :return:
    """
    for info in pdf_parse(file_data):
        item['raw_data'] = info
        item['file_name'] = file_name.split("/")[-1]
        item['webpage_uuid'] = tools.hash_sha1(info)
        yield item


def pdf_parse(file_data):
    # 提取PDF文件中的表格
    tables = tabula.read_pdf(file_data, pages="all")
    # 遍历提取出的所有表格
    for df in tables:
        # 提取表头
        columns = list(df.columns)

        # 按列切分表格记录
        df_new = pd.DataFrame(columns=columns)
        for col in columns:
            df_new[col] = re.sub(r"\[|\]|'", "", str(df[col].values.tolist())).split("\\r")

        # DataFrame转JSON
        json_array = json.loads(df_new.to_json(orient="records", force_ascii=False))
        for item in json_array:
            yield json.dumps(item, ensure_ascii=False)
