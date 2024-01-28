tis_version="4.0.0"
docker rmi registry.cn-hangzhou.aliyuncs.com/tis/tis-datax-executor:$tis_version
docker rmi tis/tis-datax-executor:$tis_version
docker build . -t tis/tis-datax-executor:$tis_version
docker tag tis/tis-datax-executor:$tis_version registry.cn-hangzhou.aliyuncs.com/tis/tis-datax-executor:$tis_version
docker push registry.cn-hangzhou.aliyuncs.com/tis/tis-datax-executor:$tis_version
