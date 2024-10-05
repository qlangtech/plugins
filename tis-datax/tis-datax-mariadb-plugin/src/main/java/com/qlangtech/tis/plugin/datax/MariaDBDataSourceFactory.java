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

package com.qlangtech.tis.plugin.datax;

import com.google.common.collect.Lists;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-08 21:47
 **/
@Public
public class MariaDBDataSourceFactory extends MySQLDataSourceFactory {

    public static final String DS_TYPE_MARIA_DB = "MariaDB";
    private static final int DEFAULT_FETCH_SIZE = 50000;
    private transient org.mariadb.jdbc.Driver driver;

    @Override
    public JDBCConnection createConnection(String jdbcUrl, boolean verify) throws SQLException {
        if (driver == null) {
            driver = new org.mariadb.jdbc.Driver();
        }
        java.util.Properties info = new java.util.Properties();
        if (this.userName != null) {
            info.put("user", this.userName);
        }
        if (this.password != null) {
            info.put("password", this.password);
        }
        if (verify) {
            info.put("connectTimeout", "3000");
            info.put("socketTimeout", "3000");
            info.put("autoReconnect", "false");
        }

        return new JDBCConnection(driver.connect(jdbcUrl, info), jdbcUrl);
    }

    @Override
    protected EntityName logicTable2PhysicsTable(String jdbcUrl, EntityName table) {
        return super.logicTable2PhysicsTable(jdbcUrl, EntityName.parse(this.removeEscapeChar(table.getFullName()), table.isPhysics()));
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {

        // https://mariadb.com/kb/en/about-mariadb-connector-j/#java-compatibility
//            StringBuffer jdbcUrl = new StringBuffer("jdbc:mariadb://" + ip + ":" + this.port + "/" + dbName +
//                    "?useUnicode=yes&useCursorFetch=true&useSsl=false&serverTimezone=" + URLEncoder.encode(DEFAULT_SERVER_TIME_ZONE.getId(), TisUTF8.getName()));

        StringBuffer jdbcUrl = new StringBuffer("jdbc:mariadb://" + ip + ":" + this.port + "/" + dbName + "?" + JDBC_PARAM_AUTO_RECONNECT + "&useUnicode=yes");
        if (this.useCompression != null) {
            jdbcUrl.append("&useCompression=").append(this.useCompression);
        }
//            if (org.apache.commons.lang.StringUtils.isNotEmpty(this.encode)) {
//                jdbcUrl.append("&characterEncoding=").append(this.encode);
//            }
        if (org.apache.commons.lang.StringUtils.isNotEmpty(this.extraParams)) {
            jdbcUrl.append("&" + this.extraParams);
        }
        return jdbcUrl.toString();

    }

    @Override
    public void setReaderStatement(Statement stmt) throws SQLException {
        org.mariadb.jdbc.Statement statement = (org.mariadb.jdbc.Statement) stmt;
        //statement.enableStreamingResults();
        // when use fetchSize the streamingResults will be switch on  https://github.com/mariadb-corporation/mariadb-connector-j/blob/3657cd62e43968d1c99f6c531ee5766c4d706dc1/src/main/java/org/mariadb/jdbc/message/ClientMessage.java#L19
        statement.setFetchSize(DEFAULT_FETCH_SIZE);
    }

    @TISExtension
    public static class MariaDBDescriptor extends DefaultDescriptor {
        @Override
        protected String getDataSourceName() {
            return DS_TYPE_MARIA_DB;
        }

        @Override
        public final EndType getEndType() {
            return EndType.MariaDB;
        }

        @Override
        public List<String> facadeSourceTypes() {
            return Lists.newArrayList(DS_TYPE_MARIA_DB);
        }

        @Override
        protected boolean validateMySQLVer(String mysqlVer) {
            return StringUtils.containsIgnoreCase(mysqlVer, DS_TYPE_MARIA_DB);
        }
    }
}
