# 基础镜像（支持 amd64 & arm64），based on Ubuntu 18.04.4 LTS
#FROM adoptopenjdk:8-jdk-hotspot
FROM eclipse-temurin:11-jdk
# 维护者
MAINTAINER dudiao(idudiao@163.com)
# 设置时区
ENV TZ=Asia/Shanghai



# 设置其他环境变量
ENV APP_NAME=powerjob-worker-samples
# 传递 SpringBoot 启动参数 和 JVM参数
ENV PARAMS=""

ENV JVMOPTIONS=""
ADD ./tis-data.tar.gz /opt/data/
ADD ./tis-datax-executor.tar.gz /opt/tis/

RUN mv /opt/data/data /opt/data/tis  && \
	mkdir -p /opt/logs && \
    mkdir -p /opt/tis

# 将应用 jar 包拷入 docker
COPY target/powerjob-worker-samples.jar /powerjob-worker-samples.jar
COPY ./wait-for-it.sh /wait-for-it.sh
# 暴露端口（HTTP + AKKA）
EXPOSE 8081 27777
# 启动应用
ENTRYPOINT ["sh","-c","java $JVMOPTIONS -jar /powerjob-worker-samples.jar $PARAMS"]
