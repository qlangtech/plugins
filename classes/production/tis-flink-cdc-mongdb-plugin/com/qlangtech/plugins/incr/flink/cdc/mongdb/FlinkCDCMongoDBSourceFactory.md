## connectionOptions

The ampersand-separated connection options of MongoDB. eg: `replicaSet=test&connectTimeoutMS=300000`

Default: none

https://docs.mongodb.com/manual/reference/connection-string/#std-label-connections-connection-options

## errorsTolerance

Whether to continue processing messages if an error is encountered. 
Accept `none` or `all`. 

* `none`: the connector reports an error and blocks further processing of the rest of the records when it encounters an error. 

* `all`: the connector silently ignores any bad messages.

Default: `none`

## copyExistingPipeline

An array of JSON objects describing the pipeline operations to run when copying existing data.

This can improve the use of indexes by the copying manager and make copying more efficient. 
eg. 
```json
[{"$match": {"closed": "false"}}] 
```
ensures that only documents in which the closed field is set to false are copied.

## updateRecordComplete

MongoDB 发生更新时候，before数据获取策略，目前有两种方式
1. FULL_CHANGE_LOG: （包括： `RowKind.UPDATE_BEFORE`,`RowKind.UPDATE_AFTER` 两种类型消息） [Full Changelog详细参考](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/#full-changeloga-namefull-changelog-id003-a)
2. UPDATE_LOOKUP: 通过 CDC内部合并更新内容和更新之前的整条记录值（包括： 只有`RowKind.UPDATE_AFTER`一种类型消息）

## startupOption

Debezium startup options

参数详细请参考：[https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/#startup-reading-position](https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/#startup-reading-position)

* `Initial`:
  Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest oplog.

* `Latest`(default):
  Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the oplog which means only have the changes since the connector was started.

* `Timestamp`:
  Skip snapshot phase and start reading oplog events from a specific timestamp.