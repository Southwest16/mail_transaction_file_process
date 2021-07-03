# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: 获取邮箱压缩包

import base64
from datetime import datetime
from email.parser import Parser
from email.utils import parseaddr, parsedate_tz, mktime_tz
from email.header import decode_header
from transaction.utils import tools


def get_mail_list(pop, max_email_id, max_email_receive_time):
    """
    下载邮件附件
    :return:
    """

    # 邮件列表
    emails = pop.list()[1]
    # 邮件数量
    email_num = len(emails)
    print(email_num)

    for i in range(max_email_id + 1, email_num + 1):
        lines = pop.retr(i)[1]
        line_bytes = b'\r\n'.join(lines)
        msg_content = line_bytes.decode('utf-8')
        # 邮件信息
        msg = Parser().parsestr(msg_content)
        header, address = parseaddr(msg.get("From", ""))

        # 发件人, 邮箱地址
        name, address = decode_str(header), decode_str(address)
        # 邮件主题
        email_subject = decode_str(msg.get('Subject', ""))

        # 每封邮件都对应一个字典来存放数据(Anti-pattern)
        item = dict()
        # 邮件在整个邮箱列表中的索引
        item['email_id'] = i
        # 每封邮件唯一值
        item['email_uuid'] = tools.hash_sha1(pop.uidl(i).decode().split("---")[-1])

        item['email_subject'] = email_subject
        item['email_from_addr'] = address
        item['email_from_name'] = name

        # 邮件接收时间
        date_tuple = parsedate_tz(msg.get("Date", ""))
        date_formatted = datetime.fromtimestamp(mktime_tz(date_tuple)).strftime("%Y-%m-%d %H:%M:%S")
        item['email_receive_time'] = date_formatted

        # 获取有效附件
        compressed_files = list()
        for compressed_file in get_attachment(msg, compressed_files):
            yield compressed_file, item

            # 处理重发邮件
            # zip_obj = zipfile.ZipFile(BytesIO(base64.b64decode(compressed_file['file_data'].encode())))
            # zip_rar_files = [file for file in zip_obj.namelist() if re.search("\\..*$", file)]
            # for file in zip_rar_files:
            #     yield {"file_data": base64.b64encode(zip_obj.read(file)).decode(), "name": file}, item


def get_attachment(msg, compressed_files):
    """
    获取附件(联合授信情况下, 附件中会存在多个附件)
    :return:
    """
    if msg.is_multipart() is True:
        # 分层信息
        parts = msg.get_payload()
        for n, part in enumerate(parts):
            res = get_attachment(part, compressed_files)
        return res
    else:
        content_type = msg.get_content_type()
        if content_type == 'application/octet-stream':
            # 遍历消息树(深度优先搜索算法), 获取每个子节点
            for subpart in msg.walk():
                file_name_encoder = subpart.get_filename()
                # 解码获取文件名
                file_name = decode_str(file_name_encoder)
                # 判断是否是zip或rar格式文件
                if file_name.split(".")[-1] not in ['zip', "rar"]:
                    continue

                data = msg.get_payload(decode=True)  # 附件二进制
                file_data = base64.b64encode(data).decode()  # base64编码
                # 存储到列表
                compressed_files.append({"file_data": file_data, "name": file_name})

    return compressed_files


def decode_str(s):
    """
    解码
    :param s:
    :return:
    """
    value, charset = decode_header(s)[0]
    if charset:
        value = value.decode(charset)
    return value


def guess_charset(msg):
    """
    编码判断
    :param msg:
    :return:
    """
    charset = msg.get_charset()
    if charset is None:
        content_type = msg.get('Content-Type', '').lower()
        pos = content_type.find('charset=')
        if pos >= 0:
            charset = content_type[pos + 8:].strip()
    return charset
