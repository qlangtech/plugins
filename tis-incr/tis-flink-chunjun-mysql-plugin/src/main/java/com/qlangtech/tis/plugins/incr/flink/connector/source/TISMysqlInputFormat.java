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

import com.dtstack.chunjun.connector.jdbc.TableCols;
import com.dtstack.chunjun.connector.mysql.source.MysqlInputFormat;
import com.qlangtech.tis.plugins.incr.flink.chunjun.common.ColMetaUtils;
import com.qlangtech.tis.plugins.incr.flink.chunjun.common.DialectUtils;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-28 15:05
 **/
public class TISMysqlInputFormat extends MysqlInputFormat {
    private final DataSourceFactory dataSourceFactory;

    public TISMysqlInputFormat(DataSourceFactory dataSourceFactory) {
        this.dataSourceFactory = dataSourceFactory;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        return Objects.requireNonNull(dataSourceFactory, "dataSourceFactory can not be null")
                .getConnection(jdbcConf.getJdbcUrl());
    }

    @Override
    protected void initializeRowConverter() {

        //ChunJunCommonConf commonConf, int fieldCount, List<IDeserializationConverter> toInternalConverters
        //            , List<Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType>> toExternalConverters
        if (rowConverter != null) {
            throw new IllegalStateException("rowConverter shall be null");
        }
        this.setRowConverter(
                // rowConverter == null
                DialectUtils.createColumnConverter(jdbcDialect, jdbcConf, this.colsMeta) // jdbcDialect.getColumnConverter(jdbcConf, flinkCols.size(), toInternalConverters, toExternalConverters)
        );
    }

//    public static AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, LogicalType>
//    createColumnConverter(JdbcDialect jdbcDialect, JdbcConf jdbcConf, List<ColMeta> colsMeta) {
//        List<FlinkCol> flinkCols = AbstractRowDataMapper.getAllTabColsMeta(colsMeta.stream().collect(Collectors.toList()));
//        List<IDeserializationConverter> toInternalConverters = Lists.newArrayList();
//        List<Pair<ISerializationConverter<FieldNamedPreparedStatement>, LogicalType>> toExternalConverters = Lists.newArrayList();
//        LogicalType type = null;
//        for (FlinkCol col : flinkCols) {
//            type = col.type.getLogicalType();
//            toInternalConverters.add(JdbcColumnConverter.getRowDataValConverter(type));
//            toExternalConverters.add(Pair.of(JdbcColumnConverter.createJdbcStatementValConverter(type, col.rowDataValGetter), type));
//        }
//
//        return jdbcDialect.getColumnConverter(jdbcConf, flinkCols.size(), toInternalConverters, toExternalConverters)
//    }


    @Override
    protected TableCols getTableMetaData() {
        return new TableCols(ColMetaUtils.getColMetas(this.dataSourceFactory, this.dbConn, this.jdbcConf));
    }

    @Override
    protected boolean useCustomReporter() {
        return false;//jdbcConf.isIncrement() && jdbcConf.getInitReporter();
    }
}
