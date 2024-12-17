## replicationNum
设置Doris create table脚本中的副本数目：
```sql
CREATE TABLE `test`
(
    `id`      VARCHAR(96) NOT NULL,
)
 ENGINE=olap
UNIQUE KEY(`id`)
PROPERTIES("replication_num" = "1"  )
```

## bucketsNum
表Bucket的作用主要有以下几点：

1. 数据分布：Bucket可以帮助数据在集群中更均匀地分布，提高数据的可靠性和容错性。
2. 加快数据查询速度：通过对数据进行分桶，查询时可以只扫描涉及的Bucket，减少扫描的数据量，从而加快查询速度。
3. 数据归档：Bucket可以用于数据的归档管理，将不再更新的数据移动到较为冷的Bucket中。
4. 数据安全：Bucket也可以用于数据备份，一般会有多个Bucket副本以防止数据丢失。

创建带Bucket的表的示例SQL语句如下：
```sql
CREATE TABLE `test`
(
    `id`      VARCHAR(96) NOT NULL,
)
 ENGINE=olap
UNIQUE KEY(`id`)
BUCKETS 16
PROPERTIES("replication_num" = "1"  )
```