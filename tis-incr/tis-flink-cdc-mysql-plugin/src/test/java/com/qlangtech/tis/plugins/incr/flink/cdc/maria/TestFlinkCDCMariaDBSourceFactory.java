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

package com.qlangtech.tis.plugins.incr.flink.cdc.maria;

import com.qlangtech.tis.plugins.incr.flink.cdc.mysql.FlinkCDCMySQLSourceFactory;
import org.apache.flink.cdc.connectors.mariadb.TISMariaDBContainer;
import com.qlangtech.tis.plugins.incr.flink.cdc.mysql.TestFlinkCDCMySQLSourceFactory;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-14 13:16
 **/
public class TestFlinkCDCMariaDBSourceFactory extends TestFlinkCDCMySQLSourceFactory {

    @Override
    protected JdbcDatabaseContainer getMysqlContainer() {
        return TISMariaDBContainer.createMariaDBContainer();
    }
    protected FlinkCDCMySQLSourceFactory createCDCFactory() {
        return new FlinkCDCMariaDBSourceFactory();
    }
    @Test
    @Override
    public void testFullTypesConsume() throws Exception {
        super.testFullTypesConsume();
    }
}
