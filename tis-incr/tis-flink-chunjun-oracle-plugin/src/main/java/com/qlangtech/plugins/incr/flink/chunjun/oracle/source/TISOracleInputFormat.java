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

package com.qlangtech.plugins.incr.flink.chunjun.oracle.source;

import com.dtstack.chunjun.connector.jdbc.TableCols;
import com.dtstack.chunjun.connector.oracle.converter.OracleColumnConverter;
import com.dtstack.chunjun.connector.oracle.source.OracleInputFormat;
import com.qlangtech.plugins.incr.flink.chunjun.common.ColMetaUtils;
import com.qlangtech.plugins.incr.flink.chunjun.common.DialectUtils;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-24 16:02
 **/
public class TISOracleInputFormat extends OracleInputFormat {
    private final DataSourceFactory dataSourceFactory;

    public TISOracleInputFormat(DataSourceFactory dataSourceFactory) {
        this.dataSourceFactory = dataSourceFactory;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        return Objects.requireNonNull(dataSourceFactory, "dataSourceFactory can not be null")
                .getConnection(jdbcConf.getJdbcUrl());
    }


    @Override
    protected final void initializeRowConverter() {
        this.setRowConverter(DialectUtils.createColumnConverter(
                jdbcDialect, jdbcConf, this.colsMeta, OracleColumnConverter::createInternalConverter));
    }


    /**
     * for override. because some databases have case-sensitive metadata。
     */
    @Override
    protected TableCols getTableMetaData() {
        // return JdbcUtil.getTableMetaData(null, jdbcConf.getSchema(), jdbcConf.getTable(), dbConn);
        return new TableCols(ColMetaUtils.getColMetas(this.dataSourceFactory, this.dbConn, this.jdbcConf));
    }


    @Override
    protected boolean useCustomReporter() {
        return false;//jdbcConf.isIncrement() && jdbcConf.getInitReporter();
    }

}
