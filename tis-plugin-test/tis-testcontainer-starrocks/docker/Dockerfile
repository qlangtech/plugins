FROM apache/incubator-doris:build-env-ldb-toolchain-latest
# https://www.starrocks.com/zh-CN/download/community
# https://download.starrocks.com/zh-CN/download/request-download/47/StarRocks-2.3.1.tar.gz

#FROM centos:7
# FROM openjdk:8-jre
# build :  docker build -t tis/starrocks .
# 可以设置 --network host 参数直接使用本地网络
# add backend: alter system add backend '192.168.28.201:9050';
# Start :  docker run -d –-network host  -p 9030:9030 -p 8030:8030 -m 2G --name starrocks tis/starrocks
# TesTt :  docker run -it  -m 600m --entrypoint /bin/bash  tis/starrocks
WORKDIR /opt/


EXPOSE 8030 9030 8040

ADD ./StarRocks-2.3.1.tar.gz .
ADD ./run.sh .
RUN INSTALL_PKGS="tar unzip bc which lsof gettext" && \
    yum install -y --enablerepo=centosplus --nogpgcheck $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    alternatives --set java java-1.8.0-openjdk.x86_64 &&\
    alternatives --set javac java-1.8.0-openjdk.x86_64 &&\
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0 &&\
    mv StarRocks-2.3.1 starrocks && chmod +x /opt/run.sh  && mkdir -p /opt/starrocks/fe/meta && mkdir -p /opt/starrocks/be/storage

ENTRYPOINT ["sh", "-c", "/opt/run.sh"]
