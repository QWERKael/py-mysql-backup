# -*- coding: utf-8 -*-
import util.exec
from util.tools import timer
import util.tools
import util.logger
import sys
import os
import shutil

logger = util.logger.set_logger(__name__)


@timer
def exec_full_backup_cmd(setting):
    """数据库全量备份"""
    cmd = [
        'innobackupex',
        '--defaults-file=' + setting['defaults-file'],
        '--user=' + setting['user'],
        '--password=' + setting['password'],
        '--socket=' + setting['socket'],
        '--parallel=' + str(setting['parallel']),
        '--no-lock',
        '--no-timestamp',
        '--slave-info',
        '--safe-slave-backup',
        setting['backup-dir']]
    util.exec.exec_cmd(cmd, exec_full_backup_cmd.__doc__)


@timer
def exec_partial_backup_cmd(setting):
    """数据库部分备份"""
    cmd = [
        'innobackupex',
        '--defaults-file=' + setting['defaults-file'],
        '--user=' + setting['user'],
        '--password=' + setting['password'],
        '--socket=' + setting['socket'],
        '--parallel=' + str(setting['parallel']),
        '--no-lock',
        '--no-timestamp',
        '--slave-info',
        '--safe-slave-backup',
        '--tables-file=' + str(setting['tmp-table-list']),
        setting['backup-dir']]
    util.exec.exec_cmd(cmd, exec_full_backup_cmd.__doc__)


@timer
def exec_inc_backup_cmd(setting, to_lsn):
    """数据库增量备份"""
    cmd = [
        'innobackupex',
        '--defaults-file=' + setting['defaults-file'],
        '--user=' + setting['user'],
        '--password=' + setting['password'],
        '--socket=' + setting['socket'],
        '--parallel=' + str(setting['parallel']),
        '--no-lock',
        '--no-timestamp',
        '--slave-info',
        '--safe-slave-backup',
        '--incremental-lsn=' + str(to_lsn),
        '--incremental',
        setting['backup-dir']]
    util.exec.exec_cmd(cmd, exec_inc_backup_cmd.__doc__)


@timer
def exec_compression(setting, threads_num=1):
    """压缩备份"""
    env = None
    cwd = os.path.dirname(setting['backup-dir'])
    if setting['compress-type'] == 'gz':
        args_str = '-zcvf'
    elif setting['compress-type'] == 'xz':
        args_str = '-Jcvf'
        env = {'XZ_OPT': '-T' + str(threads_num)}
    else:
        logger.info('不支持该压缩方式: ' + setting['compress-type'])
        sys.exit(1)
    cmd = [
        'tar',
        args_str,
        setting['backup-name'] + '.tar.' + setting['compress-type'],
        setting['backup-name']]
    util.exec.exec_cmd(cmd, exec_compression.__doc__, env, cwd)


def remove_backup_dir(path):
    """删除备份的文件夹"""
    try:
        shutil.rmtree(path)
        logger.info('备份目录 ' + path + ' 已删除')
    except Exception as e:
        logger.error('删除备份的文件夹失败')
        sys.exit(1)
