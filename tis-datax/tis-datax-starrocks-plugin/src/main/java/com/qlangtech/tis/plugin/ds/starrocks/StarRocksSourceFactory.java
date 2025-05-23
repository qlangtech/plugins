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

package com.qlangtech.tis.plugin.ds.starrocks;

import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataType.DefaultTypeVisitor;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-29 10:01
 **/
@Public
public class StarRocksSourceFactory extends BasicSourceFactory {

    public static final String DISPLAY_NAME = "StarRocks";

    @Override
    public JDBCConnection createConnection(String jdbcUrl, Optional<Properties> properties, boolean verify) throws SQLException {
        return super.createConnection(jdbcUrl, properties, verify);
    }

    @Override
    protected CreateColumnMeta createColumnMetaBuilder(EntityName table, ResultSet columns1, Set<String> pkCols, JDBCConnection conn) {
        return new CreateColumnMeta(pkCols, columns1) {
            @Override
            protected DataType getDataType(String colName) throws SQLException {
                DataType type = super.getDataType(colName);
                DataType fixType = type.accept(new DefaultTypeVisitor<DataType>() {
                    @Override
                    public DataType varcharType(DataType type) {
                        // 支持StarRocks的json类型
                        if (isJSONColumnType(type)) {
                            DataType jsonType = DataType.create(JDBCTypes.VARCHAR.getType(), type.typeName, 1000); //.createVarChar(1000);
                            return jsonType;
                        }
                        return null;
                    }
                });
                return fixType != null ? fixType : type;
            }
        };

    }

    @Override
    public Optional<String> getEscapeChar() {
        return Optional.of("`");
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicSourceFactory.DefaultDescriptor {
        @Override
        public EndType getEndType() {
            return EndType.StarRocks;
        }

        @Override
        protected String getDataSourceName() {
            return DISPLAY_NAME;
        }
    }
}



