# -*- coding: utf-8 -*-
import util.check
import util.logger
import yaml
import util.tools
import datetime

logger = util.logger.set_logger(__name__)


def parser_yaml(path):
    with open(path, 'r') as file:
        conf = yaml.load(file.read(), Loader=yaml.FullLoader)
        return conf


def get_config(options):
    # 加载配置文件
    if options['config'] and util.check.check_file(options['config']):
        logger.info('加载配置文件 ' + options['config'])
        backup_setting = parser_yaml(options['config'])
    else:
        logger.warning('未指定配置文件')
        backup_setting = {}
    # 命令行参数与配置文件冲突时，以命令行参数为准
    for op in options:
        if options[op] is not None and op != 'config':
            if op.replace('_', '-') in backup_setting:
                backup_setting[op.replace('_', '-')] = options[op]
            else:
                backup_setting.update({op.replace('_', '-'): options[op]})
    # 解析backup-package为列表
    if 'backup-package' in backup_setting and backup_setting['backup-package']:
        backup_setting['backup-package'] = backup_setting['backup-package'].split(',')
    # 添加默认值
    _host = util.tools.get_ip_addr()
    _port = backup_setting['port']
    default = {'host': _host,
               'backup-prefix': 'DB' + '.'.join(_host.split('.')[-2:]) + '_' + str(_port),
               'start-type': 0}
    set_default(backup_setting, default)
    backup_setting['remote-store-dir'] = backup_setting['remote-store-dir'].format(host=backup_setting['host'])
    backup_setting['tmp-dir'] = backup_setting['tmp-dir'].format(host=backup_setting['host'])
    _current_time = datetime.datetime.now()
    backup_setting['backup-name'] = backup_setting['backup-prefix'] + '_' + _current_time.strftime(
        "%Y%m%d_%H%M%S")
    backup_setting['backup-dir'] = '/'.join([backup_setting['tmp-dir'], _current_time.strftime("%Y%m"),
                                             backup_setting['backup-name']])
    backup_setting['local-store-path'] = backup_setting['backup-dir'] + '.tar.' + backup_setting['compress-type']
    return backup_setting


def set_default(setting, default):
    for key, value in default.items():
        if key not in setting or setting[key] is None:
            setting.update({key: value})


if __name__ == '__main__':
    print(parser_yaml('../pybackup.yml'))
