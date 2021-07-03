# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: 唯一值

import hashlib


def hash_sha1(con):
    """
    sha1加密字符串
    :param con: string
    :return: res加密值
    """
    # 创建md5对象
    h = hashlib.sha1()
    h.update(con.encode(encoding='utf-8'))
    res = h.hexdigest()
    return res
