## startupOptions

Debezium startup options

参数详细请参考：[https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-options](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html#incremental-snapshot-options)

* `Initial`:
  Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.     

* `Latest`:
  Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
  
     
