# -*- coding: utf-8 -*-
from util.check import check_cmd
import subprocess
import util.logger

logger = util.logger.set_logger(__name__)


@check_cmd
def exec_cmd(cmd, comment, env=None, cwd=None, backgroud=False):
    logger.info('-' * 15 + ' ' + comment + ' ' + '-' * 15)
    logger.info('执行命令行命令：')
    logger.info(' '.join(cmd))
    if env:
        logger.info('env: ' + str(env))
    if cwd:
        logger.info('cwd: ' + str(cwd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=cwd)
    if backgroud:
        out_value, err_value = None, None
        returncode = p.poll()
    else:
        out_value, err_value = p.communicate()
        out_value = out_value.decode()
        err_value = err_value.decode()
        returncode = p.returncode
    logger.debug('*' * 40)
    logger.debug('标准输出：\n' + str(out_value))
    logger.debug('错误输出：\n' + str(err_value))
    logger.debug('*' * 40)
    return returncode, out_value, err_value
