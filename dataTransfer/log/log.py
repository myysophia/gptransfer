# -*- coding: utf-8 -*-
import os
import time
import logging
import sys

log_dir1 = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
today = time.strftime('%Y%m%d', time.localtime(time.time()))
full_path = os.path.join(log_dir1, today)
if not os.path.exists(full_path):
    os.makedirs(full_path)
log_path = os.path.join(full_path, "transfer.log")


def get_logger():
    # 获取logger实例，如果参数为空则返回root logger
    logger = logging.getLogger("__name__")
    if not logger.handlers:
        # 指定logger输出格式
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')

        # 文件日志
        file_handler = logging.FileHandler(log_path, encoding="utf8")
        file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式

        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter  # 也可以直接给formatter赋值

        # 为logger添加的日志处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        # 指定日志的最低输出级别，默认为WARN级别
        logger.setLevel(logging.INFO)
    # 添加下面一句，在记录日志之后移除句柄
    return logger