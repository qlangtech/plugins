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

package com.qlangtech.tis.plugins.incr.flink.connector.mysql;

import com.dtstack.chunjun.connector.mysql.sink.MysqlOutputFormat;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.datax.DataxMySQLWriter;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-22 13:08
 **/
public final class TISMysqlOutputFormat extends MysqlOutputFormat {
    private final String dataXName;

    public TISMysqlOutputFormat(String dataXName) {
        super();
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalArgumentException("param dataXName can not be null");
        }
        this.dataXName = dataXName;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        if (StringUtils.isEmpty(this.dataXName)) {
            throw new IllegalStateException("param dataXName can not be null");
        }
        DataxMySQLWriter dataXWriter = (DataxMySQLWriter) DataxWriter.load(null, dataXName);
        DataSourceFactory dsFactory = dataXWriter.getDataSourceFactory();
        return dsFactory.getConnection(this.jdbcConf.getJdbcUrl());
    }
}
