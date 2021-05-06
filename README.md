# 环境配置
## 配置 `python` 环境：需要使用 `python3` 执行，并用到了 `sqlite` 模块，需要在 `python3` 安装之前安装 `sqlite-devel`
```shell
yum install sqlite-devel
yum install zlib zlib-devel
yum install openssl-devel
wget https://www.python.org/ftp/python/3.6.5/Python-3.6.5.tgz
# wget https://www.python.org/ftp/python/3.8.0/Python-3.8.0.tgz
tar -zxf Python-3.6.5.tgz 
cd Python-3.6.5
mkdir -p /usr/local/python3
./configure --prefix=/usr/local/python3
make && make install
echo "export PATH=$PATH:/usr/local/python3/bin" >> /etc/profile
source /etc/profile
```
## 依赖的 `python` 包：使用 `requirements.txt` 文件进行安装，使用豆瓣的 `pip` 源会比较快
```shell
pip3 install -r requirements.txt -i http://pypi.douban.com/simple/ --trusted-host=pypi.douban.com
```
## 依赖的软件
### sqlite
`sqlite-devel` 需要在 `python` 安装之前安装，这样安装 `python` 的时候才会自动安装 `python` 的 `sqlite` 模块
```shell
yum install sqlite-devel
```
如果已经安装了 `python` ，则需要在安装 `sqlite-devel` 后重新编译 `python`
### xz
```shell
cd /usr/local/
wget https://www.tukaani.org/xz/xz-5.2.4.tar.gz --no-check-certificate
tar -zxf xz-5.2.4.tar.gz
cd xz-5.2.4/
./configure
make && make install
ln -s /usr/local/bin/xz /usr/bin/xz
```
### rsync
# 配置文件
```
{
  "defaults-file": "/etc/my.cnf",
  "host": "10.10.10.10",
  "port": 3306,
  "socket": "/tmp/mysql.sock",
  "user": "root",
  "password": "password",                                                                       # 以上是备份数据库的相关配置
  "record-db-sqlite": "pybackup.db",            # 记录备份信息的本地sqlite数据库路径
  "record-db-mysql": {
    "user": "pybackup",
    "password": "123456",
    "host": "123.123.123.123",
    "port": 3306,
    "database": "pybackup",
    "charset": "utf8"
  },                                            # 记录备份信息的远端mysql数据库路径
  "record-type": "both",                        # 选择记录信息的数据库 sqlite|mysql|both         # 以上是记录信息的相关配置
  "tmp-dir": "/data/backups",                   # 数据会被备份到此目录，请预留足够的空间
  "parallel": 4,                                # xtrabackup执行时的并行数
  "backup-prefix": "mysql-backup",              # 备份名的前缀                                   # 以上是xtrabackup的相关配置
  "remote-host": "10.10.11.11",                # 远端备份的服务器地址
  "remote-store-dir": "pybackups",              # 远端备份的目录地址                             # 以上是远端备份的相关配置
  "compress-type": "xz",                        # 备份压缩的格式
  "increment": false,                           # 增量备份标志
  "recover-dir": "/data/mysql_recover",         # 恢复目录，恢复成功后，数据库会在此目录运行     # 以下是数据库恢复的相关配置
  "recover-tmp-dir": "/data/backups",           # 恢复临时目录，远端备份将会被拉取到此目录
  "recover-point": "now",                       # 恢复点，指定恢复时间或者now，数据库会被恢复到最近的全备或增备
  "recover-config": {
    "port": "3307",
    "socket": "/data/mysql_recover/mysql.scok",
    "datadir": "/data/mysql_recover/data",
    "server-id": "7316607",
    "slow-query-log-file": "/data/mysql_recover/data/slow_sql.log",
    "innodb-buffer-pool-size": "1280M"
  }                                             # 用以启动恢复数据库的相关配置，socket/datadir/slow-query-log-file可以不配置，程序会根据recover-dir自动设置
}
```
# 使用方式
### 全量备份
> python3 py-mysql-backup.py
### 增量备份
> python3 py-mysql-backup.py --increment
### 还原备份
> python3 py-mysql-backup.py --restore
### 根据指定备份包（压缩包）还原
> python3 py-mysql-backup.py --restore --backup-package=包地址
### 还原并自动启动
    0 不启动
    1 使用service启动mysql服务
    2 使用mysqld_safe启动mysql服务
    3 使用mysqld启动mysql服务
> python3 py-mysql-backup.py --restore --start-type=[0|1|2|3]
### 部分备份（1. 需要配置 partial-backup 段 2. 备份包只保留在本地，不同步到远端）
> python3 py-mysql-backup.py  --config /data/scripts/py-mysql-backup/pybackup.yml --partial --skip-sync
### 部分还原（需要配置 partial-restore 段）
> python3 py-mysql-backup.py --config=pybackup.yml --restore --partial --backup-package '10.255.53.157::backups/10.255.53.157/201811/DB53.157_3306_20181123_140250.tar.xz'

### 快捷备份(backup-only)与恢复(use-dir)

