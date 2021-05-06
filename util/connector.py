import mysql.connector
import functools
import util.logger

logger = util.logger.set_logger(__name__)


# def connect_mysql(user, password, host, port=3306, unix_socket='/tmp/mysql.sock'):
#     def decorate(func):
#         conn_info = {
#             "user": user,
#             "password": password,
#             "host": host,
#             "port": port,
#             "unix_socket": unix_socket,
#             "charset": "utf8"
#         }
#         conn = mysql.connector.connect(**conn_info)
#         cursor = conn.cursor()
#
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             kwargs.update({'cursor': cursor})
#             rst = func(*args, **kwargs)
#             cursor.close()
#             conn.commit()
#             conn.close()
#             return rst
#
#         return wrapper
#
#     return decorate


def connect_mysql(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # print(func.__code__.co_varnames)
        # print(func.__defaults__)
        mykwargs = {}
        mykwargs.update(dict(
            zip(func.__code__.co_varnames[func.__code__.co_argcount - len(func.__defaults__):], func.__defaults__)))
        mykwargs.update(kwargs)
        mykwargs.update(dict(zip(func.__code__.co_varnames[:len(args)], args)))
        # print(mykwargs)
        conn_info = {
            "user": str(mykwargs['user']),
            "password": str(mykwargs['password']),
            "host": str(mykwargs['host']),
            "port": int(mykwargs['port']),
            "unix_socket": str(mykwargs['unix_socket']),
            "charset": str(mykwargs['charset'])
        }
        logger.debug("conn_info: " + str(conn_info))
        conn = mysql.connector.connect(**conn_info)
        cursor = conn.cursor()
        mykwargs.update({'cursor': cursor})
        rst = func(**mykwargs)
        cursor.close()
        conn.commit()
        conn.close()
        return rst

    return wrapper


# def get_master_info(user, password, host, port=3306, unix_socket='/tmp/mysql.sock'):
#     conn_info = {
#         "user": user,
#         "password": password,
#         "host": host,
#         "port": port,
#         "unix_socket": unix_socket,
#         "charset": "utf8"
#     }
#     conn = mysql.connector.connect(**conn_info)
#     cursor = conn.cursor()
#     sql = "SHOW SLAVE STATUS"
#     cursor.execute(sql)
#     values = cursor.fetchall()
#     keys = zip(*cursor.description).__next__()
#     dict_record = [dict(zip(keys, value)) for value in values]
#     # print(dict_record)
#     cursor.close()
#     conn.commit()
#     conn.close()
#     return dict_record[0]

# @connect_mysql('root', '', "127.0.0.1", 3306, '/tmp/mysql.sock')
# @connect_mysql('pybackup', '123456', "127.0.0.1", 3306, '/tmp/mysql.sock')
@connect_mysql
def get_master_info(user, password, host, port=3306, unix_socket='/tmp/mysql.sock', charset='utf8',
                    cursor=None):
    logger.debug('user: ' + str(user) + 'password: ' + str(password) + 'host: ' + str(host) + 'port: ' + str(
        port) + 'unix_socket: ' + str(unix_socket) + 'charset: ' + str(charset))
    sql = "SHOW SLAVE STATUS"
    # sql = "SHOW processlist"
    cursor.execute(sql)
    values = cursor.fetchall()
    keys = zip(*cursor.description).__next__()
    dict_record = [dict(zip(keys, value)) for value in values]
    # print(dict_record)
    return dict_record[0]


@connect_mysql
def get_databases_list(user, password, host, port=3306, unix_socket='/tmp/mysql.sock', charset='utf8',
                    cursor=None):
    logger.debug('user: ' + str(user) + 'password: ' + str(password) + 'host: ' + str(host) + 'port: ' + str(
        port) + 'unix_socket: ' + str(unix_socket) + 'charset: ' + str(charset))
    sql = "SHOW DATABASES"
    # sql = "SHOW processlist"
    cursor.execute(sql)
    values = cursor.fetchall()
    dbs = [value[0] for value in values]
    return dbs


@connect_mysql
def get_table_list(table_list, user, password, host, port=3306, unix_socket='/tmp/mysql.sock', charset='utf8',
                   cursor=None):
    logger.debug('table_list: ' + str(table_list))
    logger.debug('user: ' + str(user) + 'password: ' + str(password) + 'host: ' + str(host) + 'port: ' + str(
        port) + 'unix_socket: ' + str(unix_socket) + 'charset: ' + str(charset))
    # sql_template = "SELECT CONCAT(table_schema,'.',table_name) AS full_name FROM information_schema.tables WHERE table_schema = '{db}' AND table_name in ('{tbs}')"
    sql_template = "SELECT {select_content} FROM information_schema.tables WHERE table_schema = '{db}' AND table_name in ('{tbs}') AND ENGINE='InnoDB'"
    sql_cache = '\nUNION\n'.join(
        [sql_template.format(select_content='{select_content}', db=db, tbs="', '".join(tbs)) for db, tbs in
         table_list.items()])
    select_contents = ["CONCAT(table_schema, '.', table_name, '.', row_format) AS table_list"]
    sqls = [sql_cache.format(select_content=select_content) for select_content in select_contents]
    logger.debug('SQL: \n' + sql_cache)
    values = {}
    for sql in sqls:
        cursor.execute(sql)
        value = cursor.fetchall()
        keys = zip(*cursor.description).__next__()
        values.update({keys[0]: [v[0] for v in value]})
    create_table = []
    logger.debug('')
    for tb in values['table_list']:
        create_sql = 'SHOW CREATE TABLE {table}'.format(table='.'.join(tb.split('.')[:2]))
        logger.debug(create_sql)
        cursor.execute(create_sql)
        value = cursor.fetchall()
        create_table.append('-- ' + value[0][0] + '\n' + value[0][1] + ';')
    values['create_table'] = create_table
    return values


@connect_mysql
def get_data_dir(user, password, host, port=3306, unix_socket='/tmp/mysql.sock', charset='utf8', cursor=None):
    logger.debug('user: ' + str(user) + 'password: ' + str(password) + 'host: ' + str(host) + 'port: ' + str(
        port) + 'unix_socket: ' + str(unix_socket) + 'charset: ' + str(charset))
    cursor.execute("SHOW VARIABLES LIKE 'datadir'")
    value = cursor.fetchall()
    datadir = value[0][1]
    return datadir


@connect_mysql
def check_database_and_prepare_table_struct(restore_db, create_table_sql, db_table_pairs, user, password, host,
                                            port=3306, unix_socket='/tmp/mysql.sock', charset='utf8', cursor=None):
    logger.debug('user: ' + str(user) + 'password: ' + str(password) + 'host: ' + str(host) + 'port: ' + str(
        port) + 'unix_socket: ' + str(unix_socket) + 'charset: ' + str(charset))
    cursor.execute("SHOW DATABASES")
    dbs = cursor.fetchall()
    if restore_db in [v[0] for v in dbs]:
        cursor.execute("SHOW TABLES FROM " + restore_db)
        tbs = cursor.fetchall()
        if len(tbs) != 0:
            logger.info(restore_db + ' 库中存在 ' + str(len(tbs)) + ' 张表：')
            logger.info(str(tbs))
            return False
    else:
        logger.info('创建数据库' + restore_db)
        cursor.execute("CREATE DATABASE " + restore_db)
    logger.info('进入还原数据库')
    cursor.execute("USE " + restore_db)
    logger.info('创建还原表')
    # logger.debug('还原表建表语句：')
    # logger.debug(create_table_sql)
    # cursor.execute(create_table_sql, multi=True)
    print("执行SQL: ", create_table_sql)
    for result in cursor.execute(create_table_sql, multi=True):
        logger.debug('已创建表结构：')
        logger.debug(result.statement)
    logger.info('表结构创建完成：')
    cursor.execute("SHOW TABLES FROM " + restore_db)
    restore_tables = [v[0] for v in cursor.fetchall()]
    logger.info(str(restore_tables))
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    logger.info("关闭外键依赖")
    for db_table_pair in db_table_pairs:
        cursor.execute("ALTER TABLE {database}.{table} ROW_FORMAT={row_format};".format(database=restore_db,
                                                                                        table=db_table_pair[1],
                                                                                        row_format=db_table_pair[2]))
        cursor.execute("ALTER TABLE {database}.{table} DISCARD TABLESPACE;".format(database=restore_db,
                                                                                   table=db_table_pair[1]))

    return True

@connect_mysql
def import_table_space(restore_db, db_table_pairs, user, password, host, port=3306, unix_socket='/tmp/mysql.sock',
                       charset='utf8', cursor=None):
    logger.debug('user: ' + str(user) + 'password: ' + str(password) + 'host: ' + str(host) + 'port: ' + str(
        port) + 'unix_socket: ' + str(unix_socket) + 'charset: ' + str(charset))
    for db_table_pair in db_table_pairs:
        cursor.execute("ALTER TABLE {database}.{table} IMPORT TABLESPACE;".format(database=restore_db,
                                                                                  table=db_table_pair[1]))
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    logger.info("打开外键依赖")
