# -*- coding: utf-8 -*-
import util.logger
import functools
import datetime
import shutil
import sys
import os
import socket
import re

logger = util.logger.set_logger(__name__)


def timer(func):
    """计时器"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        fdoc = func.__doc__
        start_time = datetime.datetime.now()
        logger.debug(fdoc + ' 开始时间' + str(start_time))
        func(*args, **kwargs)
        end_time = datetime.datetime.now()
        logger.debug(fdoc + ' 结束时间' + str(end_time))
        logger.info(fdoc + ' 执行耗时' + str(end_time - start_time))

    return wrapper


def get_xtrabackup_info(backup_dir):
    """获取备份包的信息"""
    lsn_path = backup_dir + '/xtrabackup_info'
    xtrabackup_info = {}
    logger.debug('获取lsn信息')
    try:
        row_list = []
        with open(lsn_path, 'r') as rfile:
            for line in rfile:
                if '=' in line:
                    row_list.append(line)
                else:
                    row_list[-1] += line
        for line in row_list:
            key, var = line.strip().split('=')[:2]
            key = key.strip()
            var = var.strip()
            if key.strip() == 'uuid':
                xtrabackup_info['uuid'] = var
            if key.strip() == 'server_version':
                xtrabackup_info['server_version'] = var
            if key.strip() == 'start_time':
                xtrabackup_info['backup_start_time'] = var
            if key.strip() == 'end_time':
                xtrabackup_info['backup_end_time'] = var
            if key.strip() == 'binlog_pos':
                p = re.compile("'([a-zA-Z0-9\-\n\.\:,]+)'")
                binlog_pos = p.findall(var.replace('\n', ''))
                if len(binlog_pos) == 1:
                    binlog_pos = [i.split()[-1].strip("'") for i in var.split(',')]
                if len(binlog_pos) == 2:
                    xtrabackup_info['binlog_file'], xtrabackup_info['binlog_pos'] = binlog_pos
                elif len(binlog_pos) == 3:
                    xtrabackup_info['binlog_file'], xtrabackup_info['binlog_pos'], xtrabackup_info[
                        'binlog_gtid'] = binlog_pos
                else:
                    logger.error('binlog_pos: ' + var)
                    logger.error('binlog_pos无法识别')
            if key.strip() == 'innodb_from_lsn':
                xtrabackup_info['from_lsn'] = var
            if key.strip() == 'innodb_to_lsn':
                xtrabackup_info['to_lsn'] = var
    except Exception as e:
        logger.error('获取 xtrabackup_info 文件信息出错:\n' + str(e))
    slave_info_path = backup_dir + '/xtrabackup_slave_info'
    logger.debug('获取slave_info信息')
    try:
        with open(slave_info_path, 'r') as rfile2:
            slave_info = rfile2.read()
            if slave_info:
                xtrabackup_info['slave_info'] = slave_info
                logger.debug('获取到 slave_info 信息: \n' + xtrabackup_info['slave_info'])
                slave_infos = dict([i.split('=') for i in filter(lambda x: '=' in x, slave_info.split())])
                if 'MASTER_LOG_FILE' in slave_infos:
                    xtrabackup_info['master_binlog_file'] = slave_infos['MASTER_LOG_FILE'].strip(";',")
                    logger.debug('获取到 master_binlog_file 信息: \n' + xtrabackup_info['master_binlog_file'])
                if 'MASTER_LOG_POS' in slave_infos:
                    xtrabackup_info['master_binlog_pos'] = slave_infos['MASTER_LOG_POS'].strip(";',")
                    logger.debug('获取到 master_binlog_pos 信息: \n' + xtrabackup_info['master_binlog_pos'])
    except Exception as e:
        logger.error('获取 xtrabackup_slave_info 文件信息出错:\n' + str(e))
    return xtrabackup_info


def move_dir(src_dir, dest_dir):
    """移动文件夹"""
    if os.path.exists(dest_dir) and len(os.listdir(dest_dir)) > 0:
        logger.error('目标文件夹不为空')
        sys.exit(1)
    try:
        logger.info('移动文件, 从[{}]到[{}]', src_dir, dest_dir)
        os.rename(src_dir, dest_dir)
    except FileNotFoundError as e:
        logger.error('源文件不存在: ' + src_dir)
        logger.error('错误信息: ' + e)
    except FileExistsError as e:
        logger.error('源文件不存在: ' + src_dir)
        logger.error('错误信息: ' + e)
    except IsADirectoryError as e:
        logger.error('源文件不是一个文件夹: ' + src_dir)
        logger.error('错误信息: ' + e)
    except NotADirectoryError as e:
        logger.error('目标不是一个文件夹: ' + dest_dir)
        logger.error('错误信息: ' + e)
    except Exception as e:
        logger.error('移动出错')
        logger.error('错误信息: ' + e)
    else:
        logger.info(src_dir + ' 被移动到了 ' + dest_dir)


def remove_dir(dir):
    """删除目录"""
    try:
        shutil.rmtree(dir)
    except Exception as e:
        logger.error('删除目录出错')
        logger.error('错误信息: ' + e)
    else:
        logger.info('删除目录 ' + dir + ' 成功')


def get_ip_addr():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ipaddr = s.getsockname()[0]
    logger.info('获取本机ip: ' + ipaddr)
    s.close()
    return ipaddr


def get_file_size(path):
    size = os.path.getsize(path)
    return size
