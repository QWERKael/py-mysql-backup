import sqlite3
import mysql.connector
import time

import util.check
import util.logger
import util.tools
import functools

logger = util.logger.set_logger(__name__)


def cursor_maker(func):
    """创建数据库连接并回收"""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.record_type in ['sqlite', 'both']:
            conn1 = sqlite3.connect(self.sqlite_conn_info)
            cursor1 = conn1.cursor()
            kwargs.update({'cursor': cursor1})
            self.placeholder = ('?',)
            rst = func(self, *args, **kwargs)
            cursor1.close()
            conn1.commit()
            conn1.close()
        if self.record_type in ['mysql', 'both']:
            conn2 = mysql.connector.connect(**self.mysql_conn_info)
            cursor2 = conn2.cursor()
            kwargs.update({'cursor': cursor2})
            self.placeholder = ('%s',)
            rst = func(self, *args, **kwargs)
            cursor2.close()
            conn2.commit()
            conn2.close()
        return rst

    return wrapper


class RecordDB(object):
    def __init__(self, record_type, sqlite_conn_info, mysql_conn_info):
        self.record_type = record_type
        self.sqlite_conn_info = sqlite_conn_info
        self.mysql_conn_info = mysql_conn_info
        self.placeholder = ''
        init_table_sql = """CREATE TABLE backup_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid VARCHAR(255),
            host VARCHAR(20),
            port INT,
            sock VARCHAR(255),
            server_version VARCHAR(20),
            backup_type VARCHAR(20),
            backup_start_time DATETIME,
            backup_end_time DATETIME,
            updatetime DATETIME,
            store_host VARCHAR(255),
            store_dir VARCHAR(255),
            store_name VARCHAR(255),
            local_store_path VARCHAR(255),
            file_size BIGINT,
            compress_type VARCHAR(20),
            binlog_file VARCHAR(255),
            binlog_pos VARCHAR(255),
            binlog_gtid VARCHAR(255),
            from_lsn BIGINT,
            to_lsn BIGINT,
            master_host VARCHAR(20),
            master_port INT,
            master_binlog_file VARCHAR(255),
            master_binlog_pos VARCHAR(255),
            slave_info VARCHAR(255),
            DBS VARCHAR(5000),
            state VARCHAR(20),
            comment VARCHAR(255))"""
        # 检查 pybackup.db 是否存在，若不存在则创建
        if self.record_type in ['sqlite', 'both'] and not util.check.check_file(self.sqlite_conn_info):
            conn1 = sqlite3.connect(self.sqlite_conn_info)
            cursor1 = conn1.cursor()
            cursor1.execute(init_table_sql)
            cursor1.close()
            conn1.commit()
            conn1.close()
        if self.record_type in ['mysql', 'both']:
            conn2 = mysql.connector.connect(**self.mysql_conn_info)
            cursor2 = conn2.cursor()
            cursor2.execute("show tables like 'backup_config'")
            values = cursor2.fetchall()
            if not values:
                init_table_sql = init_table_sql.replace('AUTOINCREMENT', 'AUTO_INCREMENT') + ' DEFAULT CHARSET=utf8'
                cursor2.execute(init_table_sql)
            cursor2.close()
            conn2.commit()
            conn2.close()

    @cursor_maker
    def query_record(self, sql, cursor=None):
        """查找记录"""
        logger.debug('执行查询:')
        logger.debug(sql)
        cursor.execute(
            sql)
        values = cursor.fetchall()
        keys = zip(*cursor.description).__next__()
        dict_record = [dict(zip(keys, value)) for value in values]
        logger.debug('查询结果:')
        logger.debug(dict_record)
        return dict_record

    @cursor_maker
    def insert_record(self, record, cursor=None):
        fields = tuple(record)
        values = tuple(record.values())
        cursor.execute(
            "insert into backup_config (" + ", ".join(fields) + ") values (" + ", ".join(
                self.placeholder * len(record)) + ")",
            values)
        rid = cursor.lastrowid
        return rid

    @cursor_maker
    def update_record(self, rid, record, cursor=None):
        fields = tuple(record)
        values = tuple(record.values())
        cursor.execute("update backup_config set " + ", ".join(
            [i + ' = ' + self.placeholder[0] for i in fields]) + " where id = " + str(rid),
                       values)

    @cursor_maker
    def fetchall_record(self, cursor=None):
        cursor.execute('select * from backup_config')
        values = cursor.fetchall()
        print(values)

    def get_lsn_for_increment(self, host, port):
        # 根据uuid获取上次备份的to_lsn号
        records = self.query_record(
            "select to_lsn from backup_config where host = '{}' and port = {} order by backup_end_time desc limit 1".format(
                host, str(port)))
        return records[0]['to_lsn']

    def get_backup_info(self, host, port, recover_point):
        # 获取全备信息
        full_backup_info = self.query_record(
            "select * from backup_config where host = '{}' and port = {} and backup_type = 'full backup' and backup_end_time < '{}' and state = '备份完成' order by backup_end_time desc limit 1".format(
                host, str(port), recover_point))
        logger.debug('获取全备信息:')
        logger.debug(full_backup_info)
        full_backup_time = full_backup_info[0]['backup_end_time']
        # 获取增量备份信息
        increment_backup_info = self.query_record(
            "select * from backup_config where host = '{}' and port = {} and backup_type = 'increment backup' and backup_end_time < '{}' and backup_end_time > '{}' and state = '备份完成' order by backup_end_time asc".format(
                host, str(port), recover_point, full_backup_time))
        logger.debug('获取增量备份信息:')
        logger.debug(increment_backup_info)
        return full_backup_info, increment_backup_info


if __name__ == '__main__':
    # init_sqlite()
    record1 = {'host': '10.10.0.0',
               'port': 3306,
               'sock': '/tmp/mysql.sock',
               'backup_type': 'full backup',
               'updatetime': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
               'store_host': 'localhost',
               'store_name': 'fullbackup-20180612',
               'store_path': '/data/backup/',
               'compress_type': 'xz',
               'from_lsn': 0,
               'to_lsn': 999999,
               'comment': '备注'}
    insert_record(record1)
    update_record(1, {'comment': '这是update操作'})
    fetchall_record()
