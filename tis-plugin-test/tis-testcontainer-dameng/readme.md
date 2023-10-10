达梦本地docker安装：https://eco.dameng.com/document/dm/zh-cn/start/dm-install-docker.html
1. 下载 docker tar
2. 执行 docker load -i dm8_20230808_rev197096_x86_rh6_64_single.tar
3. 在Mac中需要在docker控制面板中设置file sharing https://docs.docker.com/desktop/settings/mac/#file-sharing
4. 启动docker：docker run -d -p 30236:5236 --restart=always --name dm8_test --privileged=true -e PAGE_SIZE=16 -e LD_LIBRARY_PATH=/opt/dmdbms/bin -e  EXTENT_SIZE=32 -e BLANK_PAD_MODE=1 -e LOG_SIZE=1024 -e UNICODE_FLAG=1 -e LENGTH_IN_CHAR=1 -e INSTANCE_NAME=dm8_test -v /data/dm8_test:/opt/dmdbms/data dm8_single:dm8_20230808_rev197096_x86_rh6_64
5. 进入docker： docker exec -ti fd2701674892 /bin/bash
    /opt/dmdbms/bin/disql