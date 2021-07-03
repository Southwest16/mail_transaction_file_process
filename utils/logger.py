# -*- coding: utf-8 -*-
# Author  : xxx
# Date    :
# Description: 日志等级、日志保存目录

import logging

logging.basicConfig(filename="/file/xxx.log", level=logging.INFO)
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
