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

package com.qlangtech.plugins.incr.flink.cdc.oracle;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-02 22:55
 **/
public class OracleConnectionFactory implements JdbcConnection.ConnectionFactory {
    @Override
    public Connection connect(JdbcConfiguration jdbcConfiguration) throws SQLException {
        DataSourceFactory dsFactory = TIS.getDataBasePlugin(PostedDSProp.parse(jdbcConfiguration.getString(DataxUtils.DATASOURCE_FACTORY_IDENTITY)));
        String jdbcUrl = jdbcConfiguration.getString(DataxUtils.DATASOURCE_JDBC_URL);
        if (StringUtils.isEmpty(jdbcUrl)) {
            throw new IllegalArgumentException("param jdbcUrl can not be null, relevant key:" + DataxUtils.DATASOURCE_JDBC_URL);
        }
        JDBCConnection connection = dsFactory.getConnection(jdbcUrl, false, false);
        return connection.getConnection();
    }
}
