docker 支持：

1. 说明：https://bbs.kingbase.com.cn/forumDetail?articleId=3c085bbf8d4599112ea945ae6376fa26
2. 下载：https://download.kingbase.com.cn/xzzx/index.htm
```shell
 docker run -tid --privileged -p 4321:54321 -v /root/kingbase/data:/home/kingbase/userdata/ -e ENABLE_CI=yes -e NEED_START=yes -e DB_USER=kingbase -e DB_PASSWORD=123456 -e DB_MODE=oracle --name kingbase kingbase_v009r001c002b0014_single_x86:v1
```