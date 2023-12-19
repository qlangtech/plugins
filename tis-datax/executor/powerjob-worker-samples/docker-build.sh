tis_version="4.0.0"
docker rmi registry.cn-hangzhou.aliyuncs.com/tis/powerjob-worker:$tis_version
docker rmi tis/powerjob-worker:$tis_version
docker build . -t tis/powerjob-worker:$tis_version
docker tag tis/powerjob-worker:$tis_version registry.cn-hangzhou.aliyuncs.com/tis/powerjob-worker:$tis_version
docker push registry.cn-hangzhou.aliyuncs.com/tis/powerjob-worker:$tis_version