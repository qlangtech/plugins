## hiveConn

Hive连接实例配置

## tabType

Hudi 支持以下两种表类型：

* [COPY_ON_WRITE](https://hudi.apache.org/docs/table_types#copy-on-write-table) ：Stores data using exclusively columnar file formats (e.g parquet). Updates simply version & rewrite the files by performing a synchronous merge during write.
* [MERGE_ON_READ](https://hudi.apache.org/docs/table_types#merge-on-read-table) ：Stores data using a combination of columnar (e.g parquet) + row based (e.g avro) file formats. Updates are logged to delta files & later compacted to produce new versions of columnar files synchronously or asynchronously.

详细请参考 [https://hudi.apache.org/docs/table_types](https://hudi.apache.org/docs/table_types)

## shuffleParallelism

## autoCreateTable

## sparkConn

指定Spark服务端连接地址

## batchOp

* Takes one of these values : UPSERT (default), INSERT (use when input is  purely new data/inserts to gain speed)
* Default: `BULK_INSERT`
* Possible Values: `UPSERT`, `INSERT`, `BULK_INSERT`

## partitionedBy
