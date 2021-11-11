/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.ds.oracle;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.sql.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-24 13:42
 **/
public class OracleDataSourceFactory extends BasicDataSourceFactory {

    public static final String ORACLE = "Oracle";

    static {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        String jdbcUrl = "jdbc:oracle:thin:@" + ip + ":" + this.port + ":" + dbName;
        return jdbcUrl;
    }

    protected String getRefectTablesSql() {
        return "SELECT (TABLE_NAME) FROM user_tables";
    }


    @Override
    protected ColumnMetaData.DataType getDataType(ResultSet cols) throws SQLException {
        ColumnMetaData.DataType type = super.getDataType(cols);
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
