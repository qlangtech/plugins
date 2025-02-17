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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-01 10:39
 **/
public class SqlServer2008DatasourceFactory extends SqlServerDatasourceFactory {

    @Override
    protected Driver createDriver() {
        return new com.microsoft.sqlserver.jdbc.SQLServerDriver();
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {
        @Override
        protected String getVersion() {
            return "2008";
        }

        @Override
        protected boolean validateConnection(JDBCConnection conn, BasicDataSourceFactory dsFactory, IControlMsgHandler msgHandler, Context context) throws TisException {

            try {
                DatabaseMetaData metaData = conn.getConnection().getMetaData();
                try (ResultSet typeInfo = metaData.getTypeInfo()) {
                    boolean supportsSqlVariant = false;
                    final String jdbcTypeSqlVariant = "sql_variant";
                    // 遍历结果集，检查是否存在 sql_variant 类型
                    while (typeInfo.next()) {
                        String typeName = typeInfo.getString("TYPE_NAME");
                        if (jdbcTypeSqlVariant.equalsIgnoreCase(typeName)) {
                            supportsSqlVariant = true;
                            break;
                        }
                    }

                    if (supportsSqlVariant) {
                        // 服务端支持sql_variant，则说明现在使用的JDBC驱动太低
                        throw TisException.create("侦测到当前连接的服务端支持" + jdbcTypeSqlVariant
                                + " 请尝试使用高版本" + this.getEndType().name() + "驱动，例如：" + dataSourceName(SQL_SERVER_VERSION_2019));
                    }
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return super.validateConnection(conn, dsFactory, msgHandler, context);
        }
    }
}
