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

package com.qlangtech.tis.plugins.incr.flink.connector.source;

import com.qlangtech.plugins.incr.flink.cdc.mysql.BasicMySQLCDCTest;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import org.apache.commons.compress.utils.Lists;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-28 14:21
 **/
public class TestMySQLSourceFactory extends BasicMySQLCDCTest {

    @Override
    protected MQListenerFactory createMySQLCDCFactory() {
        return new MySQLSourceFactory();
    }


    @Test
    @Override
    public void testStuBinlogConsume() throws Exception {

        System.out.println(this.getClass().getResource("/org/testcontainers/containers/JdbcDatabaseContainer.class"));

        BasicDataSourceFactory dataSourceFactory = createDataSource(new TargetResName("x"));


        dataSourceFactory.visitFirstConnection((conn) -> {
            List<String> tabs = Lists.newArrayList();
            dataSourceFactory.refectTableInDB(tabs, conn);

            System.out.println("refectTableInDB:" + tabs.stream().collect(Collectors.joining(",")));

        });

        // super.testStuBinlogConsume();
    }

    @Test
    @Override
    public void testBinlogConsume() throws Exception {
        super.testBinlogConsume();
    }

//    @Test
//    @Override
//    public void testBinlogConsumeWithDataStreamRegisterTable() throws Exception {
//        super.testBinlogConsumeWithDataStreamRegisterTable();
//    }
//
//    @Test
//    @Override
//    public void testBinlogConsumeWithDataStreamRegisterInstaneDetailTable() throws Exception {
//        super.testBinlogConsumeWithDataStreamRegisterInstaneDetailTable();
//    }

    //    @Test
//    public void testSourceRead() {
//
//    }
}
