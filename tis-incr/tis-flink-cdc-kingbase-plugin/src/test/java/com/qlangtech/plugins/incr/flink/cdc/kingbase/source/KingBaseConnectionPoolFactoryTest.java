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
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

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

    //    @Test
//    public void testGetSchemaList() {
//        //
//
//        PostgresSourceConfig cfg = new StubPostgresSourceConfig();
//        List<String> schemaList = KingBaseConnectionPoolFactory.getSchemaList(cfg);
//        Assert.assertTrue(CollectionUtils.isEqualCollection(Lists.newArrayList("schema1", "schema2"), schemaList));
//    }
//
//
    private static class StubPostgresSourceConfig extends PostgresSourceConfig {
        public StubPostgresSourceConfig() {
            super(0, null, Lists.newArrayList("test"), Lists.newArrayList("public"), null, 0, 0, 0d, 0d, true, true, null, null, null, "192.168.28.201", 4321, "kingbase", "123456", 0, "Asia/Shanghai", Duration.ofSeconds(10), 1, 1, null, true, true);
        }
    }
}