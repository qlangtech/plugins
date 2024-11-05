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

package com.qlangtech.tis.plugin.ds.sqlserver;

import org.junit.Test;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLType;
import java.sql.Types;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-02 15:05
 **/
public class TestSqlServer2019DatasourceFactory {
    @Test
    public void testConnect() {
        SqlServer2019DatasourceFactory sqlServer2019DatasourceFactory = new SqlServer2019DatasourceFactory();
        sqlServer2019DatasourceFactory.name = "test";
        sqlServer2019DatasourceFactory.dbName = "tis";
        sqlServer2019DatasourceFactory.useSSL = false;
        sqlServer2019DatasourceFactory.nodeDesc = "192.168.28.201";
        sqlServer2019DatasourceFactory.port = 1433;
        sqlServer2019DatasourceFactory.userName = "sa";
        sqlServer2019DatasourceFactory.password = "Hello1234!";

        sqlServer2019DatasourceFactory.visitFirstConnection((conn) -> {
            try (PreparedStatement preparedStatement = conn.getConnection().prepareStatement("insert into base(base_id,update_date,col_blob) values( ?, ?, ? )")) {
                preparedStatement.setInt(1, 24);
                preparedStatement.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
                preparedStatement.setObject(3, null, Types.NULL);
                preparedStatement.execute();
            }
        });

    }
}
