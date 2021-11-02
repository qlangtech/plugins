## startupOptions

Debezium startup options

参数详细请参考：[https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options](https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options)
，[https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-property-snapshot-mode](https://debezium.io/documentation/reference/1.5/connectors/mysql.html#mysql-property-snapshot-mode)

* `Initial`:
  Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
     
* `Earliest`:
  Never to perform snapshot on the monitored database tables upon first startup, just read from the beginning of the binlog. This should be used with care, as it is only valid when the binlog is guaranteed to contain the entire history of the database.

* `Latest`:
  Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.

     
