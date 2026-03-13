## pathStyleAccess
控制 S3 API 的 URL 寻址方式。
* 是（Path-Style）：URL 格式为 http://endpoint/bucket/key，MinIO、Ceph 等自建 S3 兼容存储必须开启此选项。
* 否（Virtual-Hosted-Style）：URL 格式为 http://bucket.endpoint/key，Amazon S3 推荐使用此模式（AWS 自 2020 年 9 月起对新建 Bucket 已弃用 Path-Style）。
如不确定，自建存储选 true，AWS S3 选 false。