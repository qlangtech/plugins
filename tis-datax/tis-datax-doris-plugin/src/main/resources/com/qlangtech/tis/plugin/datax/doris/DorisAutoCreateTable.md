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

## createTableModel

TIS可以帮助用户自动生成Doris端的建表DDL语句，如Doris中已存在对应的表可选择`Off`,如需要生成可以选择`Unique`和`Duplicate`之一，如需要使用`Aggregate`模型，由于Agg模型需要设置非聚合列的聚合函数，系统无法预知。
可先选择`Unique`和`Duplicate`任意一种，待到DDL生成之后，手动在DDL之上进行修改。

Doris 支持三种数据模型：

1. Aggregate
2. Unique
3. Duplicate

[数据模型详细](https://doris.apache.org/docs/table-design/data-model/overview)