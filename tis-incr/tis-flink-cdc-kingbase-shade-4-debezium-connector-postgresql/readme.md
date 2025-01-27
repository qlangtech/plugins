## 将debezium-connector-postgres 项目中依赖到postgresql相关的 jdbc的类全部要替换成 kingbase驱动相关的类

操作步骤：
1. 找到所有依赖到的类 ，执行如下命令：
   grep -Rh 'import org.postgresql' ./ | sort | uniq |  sed 's/^import //; s/;$//'
2. 得到从postgresql 中导入的类：
   import org.postgresql.PGStatement;
   import org.postgresql.core.BaseConnection;
   import org.postgresql.core.Oid;
   import org.postgresql.core.ServerVersion;
   import org.postgresql.core.TypeInfo;
   import org.postgresql.geometric.PGbox;
   import org.postgresql.geometric.PGcircle;
   import org.postgresql.geometric.PGline;
   import org.postgresql.geometric.PGlseg;
   import org.postgresql.geometric.PGpath;
   import org.postgresql.geometric.PGpoint;
   import org.postgresql.geometric.PGpolygon;
   import org.postgresql.jdbc.PgArray;
   import org.postgresql.jdbc.PgConnection;
   import org.postgresql.jdbc.PgDatabaseMetaData;
   import org.postgresql.jdbc.PgStatement;
   import org.postgresql.jdbc.TimestampUtils;
   import org.postgresql.replication.LogSequenceNumber;
   import org.postgresql.replication.PGReplicationStream;
   import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
   import org.postgresql.util.HStoreConverter;
   import org.postgresql.util.PGInterval;
   import org.postgresql.util.PGmoney;
   import org.postgresql.util.PGobject;
   import org.postgresql.util.PGtokenizer;
   import org.postgresql.util.PSQLException;
   import org.postgresql.util.PSQLState;
3. 编写maven-shade-plugin的relocation 配置
4. 调用方法名变更：

   ```shell
   Caused by: java.lang.NoSuchMethodError: com.kingbase8.core.TypeInfo.getPGTypeNamesWithSQLTypes()Ljava/util/Iterator;
	at io.debezium.connector.postgresql.TypeRegistry$SqlTypeMapper.<init>(TypeRegistry.java:440)
	at io.debezium.connector.postgresql.TypeRegistry$SqlTypeMapper.<init>(TypeRegistry.java:410)
	at io.debezium.connector.postgresql.TypeRegistry.<init>(TypeRegistry.java:121)
	at io.debezium.connector.postgresql.connection.PostgresConnection.<init>(PostgresConnection.java:160)
	at com.qlangtech.plugins.incr.flink.cdc.kingbase.source.KingBaseConnection.<init>(KingBaseConnection.java:39)
	at com.qlangtech.plugins.incr.flink.cdc.kingbase.source.KingBaseDialect.openJdbcConnection(KingBaseDialect.java:61)
	at org.apache.flink.cdc.connectors.postgres.source.PostgresDialect.openJdbcConnection(PostgresDialect.java:102)
	at org.apache.flink.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator.createSlotForGlobalStreamSplit(PostgresSourceEnumerator.java:121)
   ```
   需要将以上`getPGTypeNamesWithSQLTypes()`方法改成`getKBTypeNamesWithSQLTypes()`(Ref: com.qlangtech.plugins.incr.flink.cdc.kingbase.plugin.ShadeClassRemapper)

   ```shell
   Caused by: java.lang.NoSuchMethodError: com.kingbase8.core.TypeInfo.getSQLType(Ljava/lang/String;)I
	at io.debezium.connector.postgresql.TypeRegistry$SqlTypeMapper.getSqlType(TypeRegistry.java:450)
	at io.debezium.connector.postgresql.TypeRegistry.createTypeBuilderFromResultSet(TypeRegistry.java:349)
	at io.debezium.connector.postgresql.TypeRegistry.prime(TypeRegistry.java:317)
	at io.debezium.connector.postgresql.TypeRegistry.<init>(TypeRegistry.java:123) 
   ```
   
   ```shell
   Caused by: java.lang.ClassCastException: class org.postgresql.jdbc.PgConnection cannot be cast to class com.kingbase8.core.BaseConnection (org.postgresql.jdbc.PgConnection is in unnamed module of loader com.qlangtech.tis.extension.impl.PluginFirstClassLoader @18851c10; com.kingbase8.core.BaseConnection is in unnamed module of loader com.qlangtech.tis.extension.impl.PluginFirstClassLoader @603dba05)
	at io.debezium.connector.postgresql.connection.PostgresReplicationConnection.pgConnection(PostgresReplicationConnection.java:471)
	at io.debezium.connector.postgresql.connection.PostgresReplicationConnection.createReplicationSlot(PostgresReplicationConnection.java:439)
	at org.apache.flink.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator.createSlotForGlobalStreamSplit(PostgresSourceEnumerator.java:131)
	... 10 more
   ```
   需要将pg的默认ConnectionFactory(`io.debezium.connector.postgresql.connection.PostgresConnection.FACTORY`)改成由TIS自定义ConnectionFactory接管

   ```shell
   Caused by: java.lang.NoSuchMethodError: com.kingbase8.core.BaseConnection.haveMinimumServerVersion(Lorg/postgresql/core/Version;)Z
   at io.debezium.connector.postgresql.connection.PostgresReplicationConnection.createReplicationSlot(PostgresReplicationConnection.java:439)
   at org.apache.flink.cdc.connectors.postgres.source.enumerator.PostgresSourceEnumerator.createSlotForGlobalStreamSplit(PostgresSourceEnumerator.java:131)
   ... 10 more
   ```
   ```shell
   Caused by: com.kingbase8.util.KSQLException: ERROR: syntax error at or near "CREATE_REPLICATION_SLOT"
   Position: 1 At Line: 1, Line Position: 1
   at com.kingbase8.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:3553) ~[?:?]
   at com.kingbase8.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2995) ~[?:?]
   at com.kingbase8.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:407) ~[?:?]
   at com.kingbase8.jdbc.KbStatement.executeInternal_(KbStatement.java:781) ~[?:?]
   at com.kingbase8.jdbc.KbStatement.execute(KbStatement.java:673) ~[?:?]
   at com.kingbase8.jdbc.KbStatement.executeWithFlags(KbStatement.java:576) ~[?:?]
   at com.kingbase8.jdbc.KbStatement.executeCachedSql(KbStatement.java:550) ~[?:?]
   at com.kingbase8.jdbc.KbStatement.executeWithFlags(KbStatement.java:511) ~[?:?]
   at com.kingbase8.jdbc.KbStatement.execute(KbStatement.java:497) ~[?:?]
   at io.debezium.connector.postgresql.connection.PostgresReplicationConnection.createReplicationSlot(PostgresReplicationConnection.java:459) ~[?:?]
   ```
  PostgresReplicationConnection 需要使用`SELECT * FROM pg_create_logical_replication_slot('%s', '%s')` 来创建replication_slot