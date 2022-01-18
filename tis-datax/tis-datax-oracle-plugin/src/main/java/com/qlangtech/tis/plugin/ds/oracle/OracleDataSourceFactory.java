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

package com.qlangtech.tis.plugin.ds.oracle;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-24 13:42
 **/
@Public
public class OracleDataSourceFactory extends BasicDataSourceFactory {

    public static final String ORACLE = "Oracle";

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean asServiceName;

    @FormField(ordinal = 8, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean allAuthorized;

    @Override
    public String identityValue() {
        return this.name;
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        String jdbcUrl = "jdbc:oracle:thin:@" + ip + ":" + this.port + (this.asServiceName ? "/" : ":") + dbName;
        return jdbcUrl;
    }

    protected String getRefectTablesSql() {
        if (allAuthorized != null && allAuthorized) {
            return "SELECT owner ||'.'|| table_name FROM all_tables";
        } else {
            return "SELECT '" + StringUtils.upperCase(this.userName) + "' ||'.'||  (TABLE_NAME) FROM user_tables";
        }
    }


    @Override
    protected ResultSet getColumnsMeta(String table, DatabaseMetaData metaData1) throws SQLException {
        return getColRelevantMeta(table, (tab) -> {
            try {
                return metaData1.getColumns(null, tab.owner, tab.tabName, null);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, () -> {
            return super.getColumnsMeta(table, metaData1);
        });
    }

    @Override
    protected ResultSet getPrimaryKeys(String table, DatabaseMetaData metaData1) throws SQLException {

        return getColRelevantMeta(table, (tab) -> {
            try {

                return metaData1.getPrimaryKeys(null, tab.owner, tab.tabName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, () -> {
            return super.getPrimaryKeys(table, metaData1);
        });
    }

    private ResultSet getColRelevantMeta(String table
            , Function<OracleTab, ResultSet> containSchema, Callable<ResultSet> notContainSchema) throws SQLException {
        try {
            if (StringUtils.indexOf(table, ".") > -1) {
                String[] tab = StringUtils.split(table, ".");
                return containSchema.apply(new OracleTab(tab[0], tab[1]));
            } else {
                return notContainSchema.call();
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private static class OracleTab {
        private final String owner;
        private final String tabName;

        public OracleTab(String owner, String tabName) {
            this.owner = StringUtils.upperCase(owner);
            this.tabName = tabName;
        }
    }


    @Override
    protected ColumnMetaData.DataType getDataType(String keyName, ResultSet cols) throws SQLException {
        ColumnMetaData.DataType type = super.getDataType(keyName, cols);
        // Oracle会将int，smallint映射到Oracle数据库都是number类型，number类型既能表示浮点和整型，所以这里要用进度来鉴别是整型还是浮点
        if (type.type == Types.DECIMAL) {
            int decimalDigits = cols.getInt("decimal_digits");
            if (decimalDigits < 1) {
                return new ColumnMetaData.DataType(Types.INTEGER);
            }
        }
        return type;
    }

    @Override
    public Connection getConnection(String jdbcUrl) throws SQLException {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return DriverManager.getConnection(jdbcUrl, StringUtils.trimToNull(this.userName), StringUtils.trimToNull(password));
    }


    @TISExtension
    public static class DefaultDescriptor extends BasicRdbmsDataSourceFactoryDescriptor {

        @Override
        protected String getDataSourceName() {
            return ORACLE;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }

        @Override
        public boolean validateExtraParams(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return true;
        }
    }
}
