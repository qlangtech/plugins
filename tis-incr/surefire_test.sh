mvn test -DoutputDirectory=../../../surefire -Dmaven.test.failure.ignore=true  -pl\
 tis-flink-chunjun-postgresql-plugin\
,tis-flink-chunjun-clickhouse-plugin\
,tis-flink-chunjun-doris-plugin\
,tis-flink-chunjun-mysql-plugin\
,tis-flink-chunjun-oracle-plugin
