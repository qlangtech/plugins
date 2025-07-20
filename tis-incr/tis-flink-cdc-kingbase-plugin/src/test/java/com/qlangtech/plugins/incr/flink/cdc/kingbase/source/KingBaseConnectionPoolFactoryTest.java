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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.config.Configuration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-15 10:06
 **/
public class KingBaseConnectionPoolFactoryTest {


    @Test
    public void testCreatePooledDataSource() throws Exception {
        KingBaseConnectionPoolFactory connectionPoolFactory = new KingBaseConnectionPoolFactory();
        StubPostgresSourceConfig sourceConfig = new StubPostgresSourceConfig();
        try (HikariDataSource dataSource = connectionPoolFactory.createPooledDataSource(sourceConfig)) {
            try (Connection connection = dataSource.getConnection()) {

                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select count(1) from \"orderdetail\" ");
                if (resultSet.next()) {
                    System.out.println("resultcount:" + resultSet.getInt(1));
                }


            }
        }


    }


    /**
     * int subtaskId, StartupOptions startupOptions, List<String> databaseList, List<String> schemaList, List<String> tableList, int splitSize, int splitMetaGroupSize, double distributionFactorUpper, double distributionFactorLower, boolean includeSchemaChanges, boolean closeIdleReaders, Properties dbzProperties, Configuration dbzConfiguration, String driverClassName, String hostname, int port, String username, String password, int fetchSize, String serverTimeZone, Duration connectTimeout, int connectMaxRetries, int connectionPoolSize, @Nullable String chunkKeyColumn, boolean skipSnapshotBackfill, boolean isScanNewlyAddedTableEnabled, int lsnCommitCheckpointsDelay, boolean assignUnboundedChunkFirst
     */

    private static class StubPostgresSourceConfig extends PostgresSourceConfig {
//        public StubPostgresSourceConfig() {
//            super(0, null, Lists.newArrayList("test"), Lists.newArrayList("public"), null, 0, 0, 0d, 0d, true, true, null, null, null, "192.168.28.201", 4321, "kingbase", "123456", 0, "Asia/Shanghai", Duration.ofSeconds(10), 1, 1, null, true, true);
//        }

        public StubPostgresSourceConfig( //int subtaskId, StartupOptions startupOptions, List<String> databaseList, List<String> schemaList, List<String> tableList, int splitSize, int splitMetaGroupSize, double distributionFactorUpper, double distributionFactorLower, boolean includeSchemaChanges, boolean closeIdleReaders, Properties dbzProperties, Configuration dbzConfiguration, String driverClassName, String hostname, int port, String username, String password, int fetchSize, String serverTimeZone, Duration connectTimeout, int connectMaxRetries, int connectionPoolSize, @Nullable String chunkKeyColumn, boolean skipSnapshotBackfill, boolean isScanNewlyAddedTableEnabled, int lsnCommitCheckpointsDelay, boolean assignUnboundedChunkFirst
        ) {
            super(0, null, Lists.newArrayList("test"), Lists.newArrayList("public"), null, 0, 0, 0d, 0d, true, true, new Properties(), Configuration.from(Maps.newHashMap()), null, "192.168.28.201", 4321, "kingbase", "123456", 0, "Asia/Shanghai", Duration.ofSeconds(10), 1, 1, null, true, false, 1, true);
        }
    }
}