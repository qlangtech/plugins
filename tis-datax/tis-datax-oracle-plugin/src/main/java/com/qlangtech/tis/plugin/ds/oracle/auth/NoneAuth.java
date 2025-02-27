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

package com.qlangtech.tis.plugin.ds.oracle.auth;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.oracle.Authorized;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-03-31 16:22
 **/
public class NoneAuth extends Authorized {

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public String getRefectTablesSql() {
//        if (allAuthorized != null && allAuthorized) {
//            return "SELECT owner ||'.'|| table_name FROM all_tables WHERE REGEXP_INSTR(table_name,'[\\.$]+') < 1";
//        } else {
        //  return "SELECT tablespace_name ||'.'||  (TABLE_NAME) FROM user_tables WHERE REGEXP_INSTR(TABLE_NAME,'[\\.$]+') < 1 AND tablespace_name is not null";
        // 带上 tablespace的话后续取colsMeta会取不出
        return "SELECT  (TABLE_NAME) FROM user_tables WHERE REGEXP_INSTR(TABLE_NAME,'[\\.$]+') < 1 AND tablespace_name is not null";

        // }
    }

    @Override
    public void addRefectedTable(TableInDB tabs, JDBCConnection conn, ResultSet resultSet) throws SQLException {
        tabs.add(conn.getUrl(), resultSet.getString(1));
    }

    /**
     * 先不用，由于增量cdc 环境中必须要使用schema
     */
    // @TISExtension
    public static class DefaultDesc extends Descriptor<Authorized> {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}
