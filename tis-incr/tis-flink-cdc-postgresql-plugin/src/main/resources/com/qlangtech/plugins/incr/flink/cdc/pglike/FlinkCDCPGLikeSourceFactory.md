## startupOptions

Debezium startup options

参数详细请参考：[https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-options](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-options)

* `Initial`:
  Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.     

* `Latest`:
  Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.

## replicaIdentity

在 PostgreSQL 中，ALTER TABLE ... REPLICA IDENTITY 命令用于指定在逻辑复制或行级触发器中如何标识已更新或删除的行。https://developer.aliyun.com/ask/575334

可选项有以下两个
* `FULL`: 使用此值需要确保对应的表执行`ALTER TABLE your_table_name REPLICA IDENTITY FULL;`，表记录更新时会带上更新Before值，使用此方式比较耗费性能。
* `DEFAULT`: 默认值，更新删除操作时不会带上Before值。

  
     
