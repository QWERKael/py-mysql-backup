# -*- coding: utf-8 -*-
import paramiko
import util.logger
import util.exec
from util.tools import timer

logger = util.logger.set_logger(__name__)


@timer
def send_file_ssh(host, port, local_addr, remote_addr):
    """使用SSH（免密）传输文件"""
    logger.info('------------------远程传输（SSH免密）------------------')
    logger.info('从：本地   %s' % local_addr)
    logger.info('到：%s:%s   %s' % (host, port, remote_addr))
    private_key = paramiko.RSAKey.from_private_key_file('/root/.ssh/id_rsa')
    transport = paramiko.Transport((host, port))
    transport.connect(username='root', pkey=private_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(local_addr, remote_addr)
    transport.close()
    logger.info('------------------远程传输完成------------------')


@timer
def transport_file_by_rsync(from_addr, to_addr, dir_only=False):
    """使用rsync传输文件"""
    logger.info('------------------远程传输（rsync）------------------')
    logger.info('从：%s' % from_addr)
    logger.info('到：%s' % to_addr)
    if dir_only:
        cmd = [
            'rsync',
            '-av',
            "--include='*/'",
            "--exclude='*'",
            from_addr,
            to_addr]
    else:
        cmd = [
            'rsync',
            '-av',
            from_addr,
            to_addr]
    util.exec.exec_cmd(cmd, transport_file_by_rsync.__doc__)
    logger.info('------------------远程传输完成------------------')


def get_backup_file_from_remote(host, remote_file_path, local_file_path):
    """从远程获取备份文件"""
    transport_file_by_rsync(host + '::' + remote_file_path, local_file_path)


def send_backup_file_to_remote(host, local_file_path, remote_file_path):
    """将备份文件发送到远端"""
    transport_file_by_rsync(local_file_path, host + '::' + remote_file_path)


def sync_dirs(host, local_file_path, remote_file_path):
    """同步目录结构"""
    transport_file_by_rsync(local_file_path, host + '::' + remote_file_path, True)
