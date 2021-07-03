# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: 解压压缩包(rar/zip)

import re
import base64
import zipfile
import rarfile
from io import BytesIO
from transaction.parser import xls_parser
from transaction.parser import csv_parser
from transaction.parser import html_parser
from transaction.parser import pdf_parser
from transaction.utils.logger import logger

inner_compress_name = ""


def get_files_from_zip(zip_obj, item):
    """
    获取zip压缩包中的文件
    :param zip_obj:
    :param item:
    :return:
    """
    # 获取压缩包中文件列表, 同时过滤一些空目录
    bank_files = [file for file in zip_obj.namelist() if re.search("\\..*$", file)]

    # 遍历每个压缩包中的所有文件
    for file in bank_files:
        # 压缩包中嵌套压缩包
        if file.endswith("zip"):
            # global inner_compress_name
            # inner_compress_name = file.encode('cp437').decode('gbk').split("/")[-1]
            inner_zip_obj = zipfile.ZipFile(BytesIO(base64.b64decode(base64.b64encode(zip_obj.read(file)).decode().encode())))
            yield from get_files_from_zip(inner_zip_obj, item)
            continue
        elif file.endswith("rar"):
            inner_rar_obj = rarfile.RarFile(BytesIO(base64.b64decode(base64.b64encode(zip_obj.read(file)).decode().encode())))
            yield from get_files_from_rar(inner_rar_obj, item)
            continue

        file_name = ""
        if re.sub(r'[^\u4e00-\u9fa5]+', '', file).isalpha():
            file_name = file
        else:
            file_name = file.encode('cp437').decode('gbk')  # 临时解码处理

        logger.info(" 开始解析 ...... " + file_name)
        file_data = BytesIO(zip_obj.read(file))  # 构造文件对象

        # 解析每个文件内容
        for eve_data in get_data_from_files(item, file_data, file_name):
            yield eve_data


def get_files_from_rar(rar_obj, item):
    """
    获取rar压缩包中的文件
    :param rar_obj:
    :param item:
    :return:
    """

    # 获取压缩包中文件列表, 同时过滤一些空目录
    bank_files = [file for file in rar_obj.namelist() if re.search("\\..*$", file)]

    # 解析每个文件内容
    for file in bank_files:
        # 压缩包中嵌套压缩包
        if file.endswith("rar"):
            inner_rar_obj = rarfile.RarFile(BytesIO(base64.b64decode(base64.b64encode(rar_obj.read(file)).decode().encode())))
            yield from get_files_from_rar(inner_rar_obj, item)
            continue
        elif file.endswith("zip"):
            inner_zip_obj = zipfile.ZipFile(BytesIO(base64.b64decode(base64.b64encode(rar_obj.read(file)).decode().encode())))
            yield from get_files_from_zip(inner_zip_obj, item)
            continue

        file_name = ""
        if re.sub(r'[^\u4e00-\u9fa5]+', '', file).isalpha():
            file_name = file
        else:
            file_name = file.encode('cp437').decode('gbk')  # 临时解码处理

        logger.info(" 开始解析 ...... " + file_name)
        file_data = BytesIO(rar_obj.read(file))  # 构造文件对象

        # 遍历每个文件中的所有数据
        for eve_data in get_data_from_files(item, file_data, file_name):
            yield eve_data


def get_data_from_files(item, file_data, file_name):
    """
    获取文件数据
    :param item:
    :param file_data:
    :param file_name:
    :return:
    """

    file_name_suffix = file_name.split(".")[-1].lower()  # 文件名后缀(xls, xlsx, csv, html, pdf, ...)
    if file_name_suffix in ("xls", "xlsx", "et"):  # Excel文件
        for row_data in xls_parser.col_to_dic(file_data, item, file_name):
            yield row_data
    elif file_name_suffix == "csv":  # csv文件
        for row_data in csv_parser.col_to_dic(file_data, item, file_name):
            yield row_data
    elif file_name_suffix == "html":  # html文件
        for row_data in html_parser.col_to_dic(file_data, item, file_name):
            yield row_data
    elif file_name_suffix == "pdf":  # PDF文件
        for row_data in pdf_parser.col_to_dic(file_data, item, file_name):
            yield row_data
    else:
        logger.error(file_name_suffix + " ---> 暂不支持解析此种类型的文件！")
