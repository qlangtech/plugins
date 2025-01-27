/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.cdc.kingbase.source;

import com.qlangtech.tis.plugin.ds.kingbase.KingBaseDataSourceFactory;
import com.qlangtech.tis.plugin.ds.postgresql.PGLikeDataSourceFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.PostgresConnectionPoolFactory;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-15 08:31
 **/
public class KingBaseConnectionPoolFactory extends PostgresConnectionPoolFactory {
    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
        // String dbType, String ip,int port, String dbName
        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        String database = sourceConfig.getDatabaseList().get(0);
        // String.format(JDBC_URL_PATTERN, hostName, port, database);
        return PGLikeDataSourceFactory.buildJdbcUrl(KingBaseDataSourceFactory.JDBC_SCHEMA_TYPE, hostName, port, database);
    }

    @Override
    public HikariDataSource createPooledDataSource(JdbcSourceConfig sourceConfig) {
        final HikariConfig config = new HikariConfig();

        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();

        config.setPoolName(CONNECTION_POOL_PREFIX + hostName + ":" + port);
        config.setJdbcUrl(getJdbcUrl(sourceConfig));
        config.setUsername(sourceConfig.getUsername());
        config.setPassword(sourceConfig.getPassword());
        config.setMinimumIdle(MINIMUM_POOL_SIZE);
        config.setMaximumPoolSize(sourceConfig.getConnectionPoolSize());
        config.setConnectionTimeout(sourceConfig.getConnectTimeout().toMillis());
        config.setDriverClassName(com.kingbase8.Driver.class.getName());

        // note: the following properties should be optional (only applied to MySQL)
        config.addDataSourceProperty(SERVER_TIMEZONE_KEY, sourceConfig.getServerTimeZone());
        // optional optimization configurations for pooled DataSource
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        config.addDataSourceProperty("charSet", "UTF8");
        // 或者使用 client_encoding
        config.addDataSourceProperty("client_encoding", "UTF8");

        return new HikariDataSource(config);
    }

//    @Override
//    public HikariDataSource createPooledDataSource(JdbcSourceConfig sourceConfig) {
//        return super.createPooledDataSource(new AdapterJdbcSourceConfig((PostgresSourceConfig) sourceConfig));
//    }

//    private static class AdapterJdbcSourceConfig extends PostgresSourceConfig {
//        public AdapterJdbcSourceConfig(PostgresSourceConfig sourceConfig) {
//            super(sourceConfig.getSubtaskId(), sourceConfig.getStartupOptions()
//                    , sourceConfig.getDatabaseList(), getSchemaList(sourceConfig), sourceConfig.getTableList()
//                    , sourceConfig.getSplitSize(), sourceConfig.getSplitMetaGroupSize()
//                    , sourceConfig.getDistributionFactorUpper(), sourceConfig.getDistributionFactorLower()
//                    , sourceConfig.isIncludeSchemaChanges(), sourceConfig.isCloseIdleReaders(), sourceConfig.getDbzProperties()
//                    , sourceConfig.getDbzConfiguration(), com.kingbase8.Driver.class.getName()
//                    , sourceConfig.getHostname(), sourceConfig.getPort(), sourceConfig.getUsername()
//                    , sourceConfig.getPassword(), sourceConfig.getFetchSize(), sourceConfig.getServerTimeZone()
//                    , sourceConfig.getConnectTimeout(), sourceConfig.getConnectMaxRetries()
//                    , sourceConfig.getConnectionPoolSize(), sourceConfig.getChunkKeyColumn()
//                    , sourceConfig.isSkipSnapshotBackfill(), sourceConfig.isScanNewlyAddedTableEnabled());
//        }
//    }
//
//    private static Field schemaListField;
//
//    static {
//        try {
//            schemaListField = JdbcSourceConfig.class.getDeclaredField("schemaList");
//            schemaListField.setAccessible(true);
//        } catch (NoSuchFieldException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    static List<String> getSchemaList(PostgresSourceConfig sourceConfig) {
//        try {
//            return (List<String>) schemaListField.get(sourceConfig);
//        } catch (IllegalAccessException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
