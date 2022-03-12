## dumpTimeStamp

导入批次，如历史数据采用DataX批量导入方式，则每次导入会在HDFS中生成一个'yyyyMMddHHmmss' 格式的时间戳目录以保存该批次数据

启动增量通道需要选择一个历史批次目录

## currentLimit

如通过Flink-CDC Snapshot导入全量历史数据，由于一个checkpoint周期内的数据量巨大，导致执行checkpoint超时，可以通过限流的方式避免checkpoint超时


