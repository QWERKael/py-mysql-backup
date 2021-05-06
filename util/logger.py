# -*- coding: utf-8 -*-
import logging
import logging.handlers
import sys
import os


def set_logger(name):
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    pre_path = os.path.split(os.path.realpath(__file__))[0]
    log_path = os.path.dirname(pre_path) + '/log/pybackup.log'
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    smtp_handler = logging.handlers.SMTPHandler(("mail.xxx.com", 587), 'dba@xxx.com', ['user@xxx.com'],
                                                "MySQL数据库备份报警", credentials=('dba', 'password'), secure=())
    smtp_handler.setFormatter(formatter)
    smtp_handler.setLevel(logging.ERROR)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.addHandler(smtp_handler)
    # handler 的日志输出界别不会低于 logger 的日志级别
    logger.setLevel(logging.DEBUG)
    return logger


if __name__ == '__main__':
    logger = set_logger(__name__)
    logger.error('日志邮件测试', exc_info=True)
