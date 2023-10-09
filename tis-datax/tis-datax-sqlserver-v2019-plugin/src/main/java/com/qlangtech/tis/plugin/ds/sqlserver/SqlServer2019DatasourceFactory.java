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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.sql.Driver;
import java.util.Properties;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-01 10:43
 **/
public class SqlServer2019DatasourceFactory extends SqlServerDatasourceFactory {


//    @Override
//    public JDBCConnection getConnection(String jdbcUrl) throws SQLException {
//        // return DriverManager.getConnection(jdbcUrl, StringUtils.trimToNull(this.userName), StringUtils.trimToNull(password));
//        try {
//            Class.forName("", true, this.getClass().getClassLoader());
//        } catch (ClassNotFoundException e) {
//            throw new SQLException(e.getMessage(), e);
//        }
//        return new JDBCConnection(DriverManager.getConnection(jdbcUrl), jdbcUrl);
//    }


    @FormField(ordinal = 12, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean useSSL;

    @Override
    protected Properties createJdbcProps() {
        Properties props = super.createJdbcProps();
        PropsSetter.jdbcPropsSet(props, useSSL);
        return props;
    }

    @Override
    protected Driver createDriver() {
        return new com.microsoft.sqlserver.jdbc.SQLServerDriver();
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {
        @Override
        protected String getVersion() {
            return "2019";
        }
    }
}
