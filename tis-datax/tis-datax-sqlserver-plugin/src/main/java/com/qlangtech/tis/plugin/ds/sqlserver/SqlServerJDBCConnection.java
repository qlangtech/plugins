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

import com.qlangtech.tis.plugin.ds.JDBCConnection;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-22 23:21
 **/
public class SqlServerJDBCConnection extends JDBCConnection {
    private final String dbName;
    private final String schema;

    public SqlServerJDBCConnection(Connection conn, String url, String dbName, String schema) {
        super(conn, url);
        if (StringUtils.isEmpty(dbName)) {
            throw new IllegalArgumentException("param dbName can not be null");
        }
        if (StringUtils.isEmpty(schema)) {
            throw new IllegalArgumentException("param schema can not be null");
        }
        this.dbName = dbName;
        this.schema = schema;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getCatalog() {
        return this.dbName;
    }
}
