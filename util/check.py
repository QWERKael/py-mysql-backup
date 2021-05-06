import functools
import os
import shutil
import socket
import sys
import json
import util.logger

logger = util.logger.set_logger(__name__)


def check_disk_free():
    statvfs = os.statvfs('/')
    total_disk_space = statvfs.f_frsize * statvfs.f_blocks
    free_disk_space = statvfs.f_frsize * statvfs.f_bfree
    disk_usage = (total_disk_space - free_disk_space) * 100.0 / total_disk_space
    disk_usage = int(disk_usage)
    logger.info("硬盘空间已使用: " + str(disk_usage) + "%")
    return False if disk_usage >= 90 else True


def check_port(host, port):
    """根据host和port检查端口是否被监听"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((host, int(port)))
    if result == 0:
        logger.error(host + ':' + port + ' 已经被使用')
        sock.close()
        sys.exit(1)
    else:
        logger.info(host + ':' + port + ' 可以被使用')
        sock.close()
        return True


def check_file(path):
    """检查文件是否存在"""
    return os.path.isfile(path)


def check_make_dir(path, isfile=False):
    """检查目录是否存在,并创建不存在的目录"""
    # 如果传入的第一个参数是文件，则检查并创建文件所在的目录
    if isfile:
        path = os.path.dirname(path)
    if os.path.isdir(path):
        return path
    else:
        logger.info("创建目录 " + path)
        os.makedirs(path)
        return path


def check_xtrabackup_completed(log_text):
    """根据xtrabackup的日志检查xtrabackup是否执行成功"""
    last_row = log_text[log_text.rfind('\n') + 1:]
    if last_row[-13:] != 'completed OK!':
        logger.error('-' * 15 + ' ' + ' xtrabackup（innobackupex）备份结果校验失败 ' + '-' * 15)
        sys.exit(1)


def check_cmd(func):
    """检查shell命令是否存在，以及是否执行成功"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        prog = ''
        comment = args[1]
        # 找到要执行的程序，剔除掉环境变量赋值
        for arg in args[0]:
            if '=' not in str(arg):
                prog = str(arg)
                break
        logger.info('检查程序 ' + prog + ' 是否存在')
        # 检查程序是否存在
        if not shutil.which(prog):
            logger.error(prog + ' : 程序不存在')
            # raise ProgramError("Cannot locate binary: " + binary)
            sys.exit(prog)
        # 执行命令
        returncode, out_value, err_value = func(*args, **kwargs)
        # 检查命令行命令是否执行成功
        if returncode is None:
            logger.info('-' * 15 + ' ' + comment + ' 程序执行中 ' + '-' * 15)
        elif returncode == 0:
            logger.info('-' * 15 + ' ' + comment + ' 完成 ' + '-' * 15)
        else:
            logger.error('-' * 15 + ' ' + comment + ' 执行失败 ' + '-' * 15)
            sys.exit(1)
        # 根据xtrabackup的日志检查备份是否成功
        if prog in ['innobackupex', 'xtrabackup']:
            check_xtrabackup_completed(err_value.strip())
            logger.info('-' * 15 + ' ' + ' xtrabackup（innobackupex）备份结果校验成功 ' + '-' * 15)
        return returncode, out_value, err_value

    return wrapper


def check_option(setting):
    if 'backup-package' in setting:
        for bp in setting['backup-package']:
            if not check_file(bp) and '::' not in bp and setting['use-dir'] is False:
                return '指定的备份包 ' + bp + ' 不存在'
    # del setting['rdb']
    # print(setting)
    # print(json.dumps(setting, indent=2))
    return None
