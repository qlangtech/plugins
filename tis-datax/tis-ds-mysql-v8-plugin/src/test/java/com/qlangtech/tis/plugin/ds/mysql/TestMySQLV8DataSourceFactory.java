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

package com.qlangtech.tis.plugin.ds.mysql;

import com.qlangtech.tis.plugin.ds.NoneSplitTableStrategy;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-23 15:09
 **/
public class TestMySQLV8DataSourceFactory {

    @Test
    public void getConnection() {
        MySQLV8DataSourceFactory dataSourceFactory = new MySQLV8DataSourceFactory();
        NoneSplitTableStrategy noneSplit = new NoneSplitTableStrategy();
        noneSplit.host = "127.0.0.1";
        dataSourceFactory.splitTableStrategy = noneSplit;
        dataSourceFactory.dbName = "tis";
        dataSourceFactory.userName = "root";
        dataSourceFactory.password = "123456";
        //dataSourceFactory.nodeDesc =
        dataSourceFactory.port = 3306;
        dataSourceFactory.visitFirstConnection((conn) -> {
            System.out.println("schema:" + conn.getSchema());
            System.out.println("catlog:" + conn.getCatalog());
        });
    }
}