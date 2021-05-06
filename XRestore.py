# -*- coding: utf-8 -*-
import util.check
import util.exec
import util.remoteTransport
import sys
import util.logger
import util.record
import shutil

from util.tools import timer

logger = util.logger.set_logger(__name__)


@timer
def exec_uncompression(file_path, uncompression_root_dir):
    """解压缩备份"""
    compress_type = file_path.split('.')[-1]
    if compress_type == 'gz':
        args_str = '-zxvf'
    elif compress_type == 'xz':
        args_str = '-Jxvf'
    else:
        logger.info('不支持该解压缩方式: ' + compress_type)
        sys.exit(1)
    cmd = [
        'tar',
        args_str,
        file_path,
        '-C',
        uncompression_root_dir]
    util.exec.exec_cmd(cmd, exec_uncompression.__doc__)


@timer
def exec_apply_log(backup_dir, redo_only=False, incremental_dir=None, partial=False):
    """预处理备份"""
    cmd = [
        'innobackupex',
        '--apply-log',
        backup_dir]
    if redo_only:
        cmd.append('--redo-only')
    if incremental_dir:
        cmd.append('--incremental-dir=' + incremental_dir)
    if partial:
        cmd.append('--export')
    util.exec.exec_cmd(cmd, exec_apply_log.__doc__)


def check_backups_completeness(full_backup_info, increment_backup_info):
    """校验备份完整性"""
    if len(increment_backup_info) == 0:
        logger.info('进行全量恢复')
        logger.info('备份完整')
        return True
    elif len(increment_backup_info) > 0:
        logger.info('进行全量恢复 + 增量恢复')
        checkpoint_lsn = full_backup_info[0]['to_lsn']
        for ibi in increment_backup_info:
            if checkpoint_lsn != ibi['from_lsn']:
                logger.info('备份不完整')
                logger.info('id为' + ibi['id'] + '的增量备份信息出错')
                sys.exit(1)
            else:
                checkpoint_lsn = ibi['to_lsn']
        logger.info('备份完整')
        return True


def get_backup_file_path(backup_info, local_store_dir):
    """获取备份地址"""
    # if backup_info['local_store_path'] and util.check.check_file(backup_info['local_store_path']):
    if backup_info['local_store_path'] != '':
        logger.info('检测到本地备份文件: ' + backup_info['local_store_path'])
        return backup_info['local_store_path']
    else:
        logger.info('未检测到本地备份文件: ' + backup_info['local_store_path'])
        logger.info('正在从远端获取...')
        util.check.check_make_dir(local_store_dir)
        util.remoteTransport.get_backup_file_from_remote(backup_info['store_host'],
                                                         backup_info['store_dir'] + '/' + backup_info['store_name'],
                                                         local_store_dir)
        local_store_path = local_store_dir + '/' + backup_info['store_name']
        logger.info('获取远端备份文件到: ' + local_store_path)
        return local_store_path


def make_mysql_config(recover_dir, recover_config, server_version):
    """生成mysql的备份文件"""
    util.check.check_make_dir(recover_dir)
    sub_paths = {'socket': recover_dir + '/mysql.sock',
                 'datadir': recover_dir + '/data',
                 'slow-query-log-file': recover_dir + '/slow/slow_sql.log',
                 'server-id': util.tools.get_ip_addr()[-2:].replace('.', '') + recover_config['port']}
    for key, val in sub_paths.items():
        if key not in recover_config:
            recover_config.update({key: val})
    util.check.check_make_dir(recover_config['socket'], True)
    util.check.check_make_dir(recover_config['datadir'])
    util.check.check_make_dir(recover_config['slow-query-log-file'], True)
    with open('template/my.cnf.' + server_version, 'r') as rf:
        mysql_config = rf.read().format(**recover_config)
    mysql_config_path = recover_dir + '/my.cnf'
    with open(mysql_config_path, 'w') as wf:
        wf.write(mysql_config)
    return mysql_config_path, recover_config


def move_table_backup_file(backup_dir, data_dir, restore_db, db_table_pairs):
    for dtp in db_table_pairs:
        for file_type in ['.ibd', '.cfg', '.exp']:
            shutil.copy(backup_dir + '/' + dtp[0] + '/' + dtp[1] + file_type, data_dir + '/' + restore_db)



def exec_start_mysqld_safe(mysql_config_path):
    """使用mysqld_safe启动mysql服务"""
    logger.info('尝试使用mysqld_safe启动mysql')
    cmd = [
        'mysqld_safe',
        '--defaults-file=' + mysql_config_path]
    util.exec.exec_cmd(cmd, exec_start_mysqld_safe.__doc__, backgroud=True)


def exec_start_mysqld(mysql_config_path):
    """使用mysqld启动mysql服务"""
    logger.info('尝试使用mysqld启动mysql')
    cmd = [
        'mysqld',
        '--defaults-file=' + mysql_config_path,
        '--user=mysql',
        '--daemonize']
    util.exec.exec_cmd(cmd, exec_start_mysqld.__doc__)


def exec_service_mysqld_start():
    """使用service启动mysql服务"""
    logger.info('尝试使用service启动mysql')
    cmd = [
        'service',
        'mysqld',
        'start']
    util.exec.exec_cmd(cmd, exec_start_mysqld.__doc__)


def start_mysql_server(start_type, mysql_config_path):
    """start_type
    1 使用service启动mysql服务
    2 使用mysqld_safe启动mysql服务
    3 使用mysqld启动mysql服务"""
    start_type = int(start_type)
    if start_type == 1:
        exec_service_mysqld_start()
    elif start_type == 2:
        exec_start_mysqld_safe(mysql_config_path)
    elif start_type == 3:
        exec_start_mysqld(mysql_config_path)
    else:
        logger.info('不启动mysql')


def exec_chown_mysql(path):
    """更改mysql目录的所有者"""
    cmd = [
        'chown',
        '-R',
        'mysql:mysql',
        path]
    util.exec.exec_cmd(cmd, exec_chown_mysql.__doc__)
