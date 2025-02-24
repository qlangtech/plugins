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
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.oracle.Authorized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-03-31 16:25
 **/
public class AcceptAuthorized extends Authorized {
    private static final List<String> systemSchemas
            = Arrays.asList("GSMADMIN_INTERNAL","ANONYMOUS", "APEX_030200", "APEX_PUBLIC_USER", "APPQOSSYS", "BI", "CTXSYS", "DBSNMP"
            , "DIP", "EXFSYS", "FLOWS_FILES", "HR", "IX", "MDDATA", "MDSYS", "MGMT_VIEW", "OE", "OLAPSYS"
            , "ORACLE_OCM", "ORDDATA", "ORDPLUGINS", "ORDSYS", "OUTLN", "OWBSYS", "OWBSYS_AUDIT"
            , "PM", "SCOTT", "SH", "SI_INFORMTN_SCHEMA", "SPATIAL_CSW_ADMIN_USR", "SPATIAL_WFS_ADMIN_USR", "SYS", "SYSMAN", "SYSTEM", "WMSYS", "XDB", "XS$NULL");
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name,Validator.require})
    public String schema;

    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean excludeSysSchema;

    @Override
    public String getSchema() {
        return this.schema;
    }

    @Override
    public String getRefectTablesSql() {
//        if (allAuthorized != null && allAuthorized) {
        return "SELECT owner ||'.'|| table_name, owner FROM all_tables WHERE REGEXP_INSTR(table_name,'[\\.$]+') < 1 "; // AND owner='" + schema + "'";
//        } else {
        //  return "SELECT tablespace_name ||'.'||  (TABLE_NAME) FROM user_tables WHERE REGEXP_INSTR(TABLE_NAME,'[\\.$]+') < 1 AND tablespace_name is not null";
        // 带上 tablespace的话后续取colsMeta会取不出
        //  return "SELECT  (TABLE_NAME) FROM user_tables WHERE REGEXP_INSTR(TABLE_NAME,'[\\.$]+') < 1 AND tablespace_name is not null";

        // }
    }

    @Override
    public void addRefectedTable(TableInDB tabs, JDBCConnection conn, ResultSet resultSet) throws SQLException {
        if (excludeSysSchema) {
            String owner = resultSet.getString(2);
            if (systemSchemas.contains(owner)) {
                return;
            }
        }
        tabs.add(conn.getUrl(), resultSet.getString(1));
    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<Authorized> {
        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
