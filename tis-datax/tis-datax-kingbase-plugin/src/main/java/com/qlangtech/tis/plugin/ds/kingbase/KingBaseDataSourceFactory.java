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

package com.qlangtech.tis.plugin.ds.kingbase;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.kingbase.KingBaseCompatibleMode;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.postgresql.PGLikeDataSourceFactory;

import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * Kingbase 数据库DataSource <br>
 * <p>
 * 读写分离配置：
 * https://bbs.kingbase.com.cn/docHtml?recId=218c307e5f3d72bf20bb84a51859344a&url=aHR0cHM6Ly9iYnMua2luZ2Jhc2UuY29tLmNuL2tpbmdiYXNlLWRvYy92OS4xLjEuMjQvZmFxL2ZhcS1uZXcvaW50ZXJmYWNlL2pkYmMuaHRtbCNpZDQ
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-14 14:34
 **/
public class KingBaseDataSourceFactory extends PGLikeDataSourceFactory {
    public static final String KingBase_NAME = "KingBase";
    @FormField(ordinal = 8, validate = {Validator.require})
    public KingBaseCompatibleMode dbMode;

    @Override
    protected java.sql.Driver createDriver() {
        return new com.kingbase8.Driver();
    }

    @Override
    public Optional<String> getEscapeChar() {
        return Objects.requireNonNull(dbMode, "dbMode can not be null").getEscapeChar();
    }

    @Override
    protected String getDBType() {
        return "kingbase8";
    }


    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        return super.buidJdbcUrl(db, ip, dbName);
    }

    @Override
    public JDBCConnection createConnection(String jdbcUrl, boolean verify) throws SQLException {
        return super.createConnection(jdbcUrl, verify);
    }

    @TISExtension
    public static class KingBaseDSDescriptor extends BasicPGLikeDefaultDescriptor {
        @Override
        protected String getDataSourceName() {
            return KingBase_NAME;
        }

        @Override
        public EndType getEndType() {
            return EndType.KingBase;
        }

        @Override
        protected String getConnectionSchema(JDBCConnection c) throws SQLException {
            return c.getSchema();
        }
    }

}
