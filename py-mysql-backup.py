# -*- coding: utf-8 -*-
import util.check
import util.logger
import util.tools
import util.connector
import util.remoteTransport
import util.config
import util.record
import click
import XBackup
import XRestore
import time
import os
import sys


@click.command()
@click.option('--config', help='py-mysql-backup配置文件的路径.')
@click.option('--defaults-file', help='MySQL配置文件的路径.')
@click.option('--host', help='本机ip')
@click.option('--port', help='mysql端口号')
@click.option('--socket', help='socket地址')
@click.option('--user', help='用户名')
@click.option('--password', hide_input=True, help='密码')
@click.option('--record-db-sqlite', help='记录备份信息的本地sqlite数据库地址')
@click.option('--record-type', help='记录备份信息的方式，sqlite 使用本地sqlite记录备份信息，mysql 使用远端mysql的方式记录备份信息，both 同时使用两种方式')
@click.option('--tmp-dir', help='备份文件的临时存放路径')
@click.option('--parallel', help='并行线程数')
@click.option('--backup-prefix', help='备份文件前缀名')
@click.option('--remote-host', help='远程服务器地址')
@click.option('--remote-store-dir', help='远程存储地址')
@click.option('--compress-type', help='备份类型')
@click.option('--compress-threads-num', help='压缩线程数')
@click.option('--increment', default=False, is_flag=True, help='增量备份')
@click.option('--restore', default=False, is_flag=True, help='执行恢复操作')
@click.option('--partial', default=False, is_flag=True, help='执行部分备份还原，只支持innodb表')
@click.option('--skip-sync', default=False, is_flag=True, help='备份后不传输备份包到远端')
@click.option('--backup-only', default=False, is_flag=True, help='仅备份，不进行压缩，也不进行数据传输')
@click.option('--recover-dir', help='mysql的恢复目录')
@click.option('--recover-tmp-dir', help='获取远程备份的目录')
@click.option('--recover-point', help='恢复到该时间点最近的备份')
@click.option('--backup-package', help='通过指定备份包的形式还原,请写绝对路径，如果指定包为一组全量和增量备份，请以逗号分隔')
@click.option('--use-dir', default=False, is_flag=True, help='backup-package使用目录的形式还原，而非压缩包')
@click.option('--start-type',
              help='启动mysql的方式，0 不启动mysql，1 使用service启动mysql服务，2 使用mysqld_safe启动mysql服务，3 使用mysqld启动mysql服务')
def pybackup(**options):
    """数据库备份"""
    # 检查磁盘空间
    if not util.check.check_disk_free():
        logger.error('磁盘空间不足，请释放空间')
        sys.exit(0)
    # 解析配置文件并获取参数
    backup_setting = util.config.get_config(options)
    # 记录本次执行的参数
    logger.debug(backup_setting)
    # 创建一个记录数据库实例
    rdb = util.record.RecordDB(backup_setting['record-type'], backup_setting['record-db-sqlite'],
                               backup_setting['record-db-mysql'])
    backup_setting.update({'rdb': rdb})
    # 检测参数合法性
    check_error = util.check.check_option(backup_setting)
    if check_error:
        logger.error(check_error)
        sys.exit(1)
    # 执行备份或恢复流程
    if options['restore']:
        # 执行恢复流程
        if 'backup-package' in backup_setting:
            # 从指定备份包恢复
            restore_from_specified_packages(backup_setting, backup_setting['backup-package'])
        else:
            # 从数据库获取恢复信息
            restore_process(backup_setting, backup_setting['host'], backup_setting['port'],
                            backup_setting['recover-point'])
    else:
        # 执行备份流程
        backup_process(backup_setting)


def backup_process(setting):
    """执行备份流程"""
    # 检查备份路径
    util.check.check_make_dir(os.path.dirname(setting['backup-dir']))
    # 执行全量备份或者增量备份
    if setting['increment']:
        backup_type = 'increment backup'
    elif setting['partial']:
        backup_type = 'partial backup'
    else:
        backup_type = 'full backup'
    # 获取数据库列表
    dbs_list = util.connector.get_databases_list(setting['user'], setting['password'], "127.0.0.1", 3306,
                                                 setting['socket'])
    logger.debug('获取数据库列表: ')
    logger.debug(dbs_list)

    rid = setting['rdb'].insert_record({
        'host': setting['host'],
        'port': setting['port'],
        'sock': setting['socket'],
        'backup_type': backup_type,
        'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'dbs': ','.join(dbs_list),
        'state': '开始备份'
    })
    if setting['increment']:
        # 执行增量本分
        to_lsn = setting['rdb'].get_lsn_for_increment(setting['host'], setting['port'])
        XBackup.exec_inc_backup_cmd(setting, to_lsn)
    elif setting['partial']:
        # 执行部分（全量）备份
        partial_info = util.connector.get_table_list(setting['partial-backup'], setting['user'], setting['password'],
                                                     "127.0.0.1", 3306, setting['socket'])
        # 添加表清单文件
        with open(setting['tmp-table-list'], 'w') as fw1:
            tls = ['.'.join(tl.split('.')[:2]) for tl in partial_info['table_list']]
            fw1.write('\n'.join(tls) + '\n')
        XBackup.exec_partial_backup_cmd(setting)
        # 记录部分备份信息到备份文件夹
        for k, v in partial_info.items():
            with open(setting['backup-dir'] + '/' + k, 'w') as fw2:
                fw2.write('\n'.join(v) + '\n')
    else:
        # 执行全量备份
        XBackup.exec_full_backup_cmd(setting)
    # 更新备份信息
    xtrabackup_info = util.tools.get_xtrabackup_info(setting['backup-dir'])
    if 'slave_info' in xtrabackup_info:
        # 查询复制信息
        master_info = util.connector.get_master_info(setting['user'], setting['password'], "127.0.0.1", 3306,
                                                     setting['socket'])
        logger.debug('获得master信息: ')
        logger.debug(master_info)
        setting['master_host'] = master_info['Master_Host']
        setting['master_port'] = master_info['Master_Port']
        xtrabackup_info['master_host'] = master_info['Master_Host']
        xtrabackup_info['master_port'] = master_info['Master_Port']
        if ('master_binlog_file' in xtrabackup_info) and ('master_binlog_pos' in xtrabackup_info):
            xtrabackup_info[
                'slave_info'] = "CHANGE MASTER TO MASTER_HOST='{master_host}', MASTER_PORT={master_port}, MASTER_LOG_FILE='{binlog_file}', MASTER_LOG_POS={binlog_pos}".format(
                master_host=setting['master_host'], master_port=setting['master_port'],
                binlog_file=xtrabackup_info['master_binlog_file'],
                binlog_pos=xtrabackup_info['master_binlog_pos'])
        elif 'binlog_gtid' in xtrabackup_info:
            xtrabackup_info['slave_info'] = """SET GLOBAL gtid_purged='{binlog_gtid}';
CHANGE MASTER TO MASTER_HOST='{master_host}', MASTER_PORT={master_port}, MASTER_AUTO_POSITION=1;""".format(
                binlog_gtid=xtrabackup_info['binlog_gtid'], master_host=setting['master_host'],
                master_port=setting['master_port'])
        xtrabackup_info['comment'] = '检测到备份库存在主库，slave_info 信息指向备份库的主库'
    elif 'binlog_gtid' in xtrabackup_info:
        xtrabackup_info['slave_info'] = """SET GLOBAL gtid_purged='{binlog_gtid}';
CHANGE MASTER TO MASTER_HOST='{master_host}', MASTER_PORT={master_port}, MASTER_AUTO_POSITION=1;""".format(
            binlog_gtid=xtrabackup_info['binlog_gtid'], master_host=setting['host'], master_port=setting['port'])
        xtrabackup_info['comment'] = '未检测到备份库的主库，slave_info 信息指向备份库'
    else:
        xtrabackup_info[
            'slave_info'] = "CHANGE MASTER TO MASTER_HOST='{master_host}', MASTER_PORT={master_port}, MASTER_LOG_FILE='{binlog_file}', MASTER_LOG_POS={binlog_pos}".format(
            master_host=setting['host'], master_port=setting['port'], binlog_file=xtrabackup_info['binlog_file'],
            binlog_pos=xtrabackup_info['binlog_pos'])
        xtrabackup_info['comment'] = '未检测到备份库的主库，slave_info 信息指向备份库'
    xtrabackup_info.update({
        'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'state': '备份完成，更新备份信息'
    })
    setting['rdb'].update_record(rid, xtrabackup_info)

    # 仅备份
    if setting['backup-only'] is True:
        logger.info('------------------仅备份，不进行压缩，也不进行数据传输------------------')
        setting['rdb'].update_record(rid, {
            'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'state': '备份完成'
        })
        logger.info('------------------备份结束------------------')
        logger.info('------------------THE END------------------')
        return

    # 执行压缩
    setting['rdb'].update_record(rid, {
        'compress_type': setting['compress-type'],
        'local_store_path': setting['local-store-path'],
        'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'state': '开始压缩'
    })
    XBackup.exec_compression(setting, setting['compress-threads-num'])
    # 获取压缩包大小
    size = util.tools.get_file_size(setting['local-store-path'])
    setting['rdb'].update_record(rid, {
        'file_size': size
    })
    # 删除备份文件夹
    setting['rdb'].update_record(rid, {
        'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'state': '删除备份文件夹'
    })
    util.tools.remove_dir(setting['backup-dir'])
    # 传输到远程服务器
    if 'remote-host' in setting and setting['remote-host'] and setting['skip-sync'] is False:
        setting['rdb'].update_record(rid, {
            'store_host': setting['remote-host'],
            'store_dir': setting['remote-store-dir'],
            'store_name': os.path.basename(setting['local-store-path']),
            'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'state': '开始传输到远端'
        })
        # 同步目录结构到远端
        # util.remoteTransport.sync_dirs(setting['remote-host'], setting['tmp-dir'], os.path.dirname(setting['remote-store-dir']))
        # 同步文件到远端
        # util.remoteTransport.send_backup_file_to_remote(setting['remote-host'], setting['local-store-path'],
        #                                                 setting['remote-store-dir'])
        util.remoteTransport.send_backup_file_to_remote(setting['remote-host'], setting['tmp-dir'],
                                                        os.path.dirname(setting['remote-store-dir']))
    # 结束
    setting['rdb'].update_record(rid, {
        'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'state': '备份完成'
    })
    logger.info('------------------备份结束------------------')
    logger.info('------------------THE END------------------')


def restore_process(setting, host, port, recover_point):
    """根据时间点恢复到最近的全备/增量备份"""
    if recover_point == 'now':
        recover_point = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 查找备份信息
    full_backup_info, increment_backup_info = setting['rdb'].get_backup_info(host, port, recover_point)
    base_backup_info = full_backup_info[0]
    version = base_backup_info['server_version'][:3]
    # 校验备份完整性
    XRestore.check_backups_completeness(full_backup_info, increment_backup_info)
    # 获取备份压缩文件的地址
    backup_files = [XRestore.get_backup_file_path(base_backup_info, setting['recover-tmp-dir'])]
    for ibi in increment_backup_info:
        # ibi['store_dir'] = ibi['store_dir'] + '/' + ibi['backup_start_time'].strftime("%Y%m")
        backup_files.append(XRestore.get_backup_file_path(ibi, setting['recover-tmp-dir']))
    restore_from_backup_package_list(setting, backup_files, version)


def restore_from_specified_packages(setting, backup_package_list):
    """从指定包恢复备份"""
    backup_files = []
    for bp in backup_package_list:
        if '::' in bp:
            # 如果为远程地址，则将远程地址的信息拆解开
            store_host, store_path = bp.split('::')[:2]
            store_dir = os.path.dirname(store_path)
            store_name = os.path.basename(store_path)
            bpi = {'local_store_path': '', 'store_host': store_host, 'store_dir': store_dir, 'store_name': store_name}
        else:
            bpi = {'local_store_path': bp, 'store_host': '', 'store_dir': '', 'store_name': ''}
        backup_files.append(XRestore.get_backup_file_path(bpi, setting['recover-tmp-dir']))
    restore_from_backup_package_list(setting, backup_files)


def restore_from_backup_package_list(setting, backup_files, version=None):
    """根据备份的列表进行恢复"""
    rid = setting['rdb'].insert_record({
        'host': setting['host'],
        'port': setting['port'],
        'sock': setting['socket'],
        'backup_type': 'recover',
        'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'state': '开始恢复',
        'comment': '恢复到' + util.tools.get_ip_addr()
    })
    # 判断是否需要解压缩
    ucmp_dirs = []
    if setting['use-dir'] is True:
        logger.info('直接使用目录，不进行解压缩')
        ucmp_dirs = backup_files
    else:
        # 解压缩备份，生成解压缩目录列表
        setting['rdb'].update_record(rid, {
            'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'state': '解压缩备份'
        })
        print(backup_files)
        for bf in backup_files:
            ucmp_dir = os.path.dirname(bf) + '/' + os.path.basename(bf).split('.tar')[0]
            logger.info('备份压缩包 ' + bf + ' 将被解压缩到目录 ' + ucmp_dir + ' 中')
            XRestore.exec_uncompression(bf, os.path.dirname(ucmp_dir))
            ucmp_dirs.append(ucmp_dir)
    # 应用redo-log
    setting['rdb'].update_record(rid, {
        'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'state': '应用redo-log'
    })
    if 'partial' in setting and setting['partial']:
        # 进行部分还原
        # 准备部分还原
        XRestore.exec_apply_log(ucmp_dirs[0], partial=True)
        # 准备还原库的连接信息
        partial_restore_user = setting['partial-restore']['user']
        partial_restore_password = setting['partial-restore']['password']
        partial_restore_port = setting['partial-restore']['port']
        partial_restore_socket = setting['partial-restore']['socket']
        partial_restore_db = setting['partial-restore']['restore-db']
        # 获取还原库的数据目录地址
        datadir = util.connector.get_data_dir(partial_restore_user, partial_restore_password, '127.0.0.1',
                                              partial_restore_port, partial_restore_socket)
        # 获取部分备份的库表信息
        with open(ucmp_dirs[0] + '/table_list', 'r') as fr1:
            db_table_pairs = [full_table_name.split('.') for full_table_name in fr1.read().strip().split('\n')]
        # 检查还原的目标数据库是否为空并准备还原的表结构
        with open(ucmp_dirs[0] + '/create_table', 'r') as fr2:
            create_table_sql = fr2.read()
        isOK = util.connector.check_database_and_prepare_table_struct(partial_restore_db, create_table_sql,
                                                                      db_table_pairs, partial_restore_user,
                                                                      partial_restore_password, '127.0.0.1',
                                                                      partial_restore_port, partial_restore_socket)
        if isOK is False:
            logger.error('指定的恢复库不为空')
            sys.exit(1)
        # 移动还原的表 ibd, cfg, exp 文件到还原的目录下
        XRestore.move_table_backup_file(ucmp_dirs[0], datadir, partial_restore_db, db_table_pairs)
        # 更改mysql目录的所有者
        XRestore.exec_chown_mysql(datadir + '/' + partial_restore_db)
        # 导入表空间
        util.connector.import_table_space(partial_restore_db, db_table_pairs, partial_restore_user,
                                          partial_restore_password, '127.0.0.1', partial_restore_port,
                                          partial_restore_socket)
        # 结束
        setting['rdb'].update_record(rid, {
            'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'state': '恢复结束'
        })
        logger.info('------------------恢复结束------------------')
        logger.info('------------------THE END------------------')
        return
    elif len(ucmp_dirs) > 1:
        # 准备增量还原
        XRestore.exec_apply_log(ucmp_dirs[0], True)
        for ud in ucmp_dirs[1:-1]:
            XRestore.exec_apply_log(ucmp_dirs[0], True, ud)
        XRestore.exec_apply_log(ucmp_dirs[0], False, ucmp_dirs[-1])
    else:
        # 准备全量还原
        XRestore.exec_apply_log(ucmp_dirs[0])
    # 从文件获取版本号
    if version is None:
        version = util.tools.get_xtrabackup_info(ucmp_dirs[0])['server_version'][:3]
    # 创建恢复的mysql配置文件
    mysql_config_path, recover_config = XRestore.make_mysql_config(setting['recover-dir'], setting['recover-config'],
                                                                   version)
    # 恢复到指定位置
    util.tools.move_dir(ucmp_dirs[0], recover_config['datadir'])
    # 更改mysql目录的所有者
    XRestore.exec_chown_mysql(setting['recover-dir'])
    # 检测端口号是否被监听
    util.check.check_port('127.0.0.1', recover_config['port'])
    # 启动
    XRestore.start_mysql_server(setting['start-type'], mysql_config_path)
    # try:
    #     XRestore.exec_start_mysqld_safe(mysql_config_path)
    #     # 结束
    #     logger.info('------------------恢复结束------------------')
    #     logger.info('------------------THE END------------------')
    #     sys.exit(0)
    # except SystemExit as e:
    #     if str(e) == 'mysqld_safe':
    #         logger.error('使用mysqld_safe启动失败')
    # XRestore.exec_start_mysqld(mysql_config_path)
    # 结束
    setting['rdb'].update_record(rid, {
        'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'state': '恢复结束'
    })
    logger.info('------------------恢复结束------------------')
    logger.info('------------------THE END------------------')


def restore_by_master(master_host, master_port, date):
    """根据主库进行恢复"""
    pass


if __name__ == '__main__':
    logger = util.logger.set_logger('pybackup')
    pybackup()
