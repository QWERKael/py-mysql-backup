#!/bin/bash
# 记录初始目录
mydir=`pwd`
mv xz-5.2.4.tar.gz /usr/local/
mv Python-3.6.5.tgz /usr/local/
# 安装xz
cd /usr/local/
# wget https://www.tukaani.org/xz/xz-5.2.4.tar.gz --no-check-certificate
#wget -O xz-5.2.4.tar.gz  http://10.255.0.120:8022/download/linux/xz-5.2.4.tar.gz
tar -zxf xz-5.2.4.tar.gz
cd xz-5.2.4/
./configure
make && make install
ln -s /usr/local/bin/xz /usr/bin/xz
# 安装python3.6
yum install -y sqlite-devel zlib-devel
cd /usr/local/
# wget https://www.python.org/ftp/python/3.6.5/Python-3.6.5.tgz
#wget -O Python-3.6.5.tgz  http://10.255.0.120:8022/download/linux/Python-3.6.5.tgz
tar -zxf Python-3.6.5.tgz
cd Python-3.6.5
mkdir -p /usr/local/python3
./configure --prefix=/usr/local/python3
make && make install
echo "export PATH=$PATH:/usr/local/python3/bin" >> /etc/profile
source /etc/profile
# 安装python模块
cd $mydir
/usr/local/python3/bin/pip3 install -r requirements.txt  -i http://pypi.douban.com/simple/ --trusted-host=pypi.douban.com