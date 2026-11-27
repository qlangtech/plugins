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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Corrects Kingbase JDBC getColumns aliases using the actual column/domain catalog.
 * Applies in both compatibility modes; ordinary types and unrelated domains are untouched.
 * Domain/base OIDs are obtained from this database, never assumed from a release.
 */
public final class KingBaseColumnTypes {
    private KingBaseColumnTypes() { }

    private static final String COLUMN_TYPES_SQL =
            "WITH RECURSIVE column_types(attname, typname, typbasetype, depth) AS ("
            + " SELECT a.attname, t.typname, t.typbasetype, 0"
            + " FROM sys_catalog.sys_attribute a"
            + " JOIN sys_catalog.sys_class c ON c.oid=a.attrelid"
            + " JOIN sys_catalog.sys_namespace n ON n.oid=c.relnamespace"
            + " JOIN sys_catalog.sys_type t ON t.oid=a.atttypid"
            + " WHERE n.nspname=? AND c.relname=? AND a.attnum>0 AND NOT a.attisdropped"
            + " UNION ALL SELECT ct.attname, t.typname, t.typbasetype, CAST(ct.depth+1 AS INTEGER)"
            + " FROM column_types ct JOIN sys_catalog.sys_type t ON t.oid=ct.typbasetype"
            + " WHERE ct.typbasetype<>0 AND ct.depth<32)"
            + " SELECT attname, typname, typbasetype FROM column_types ORDER BY attname, depth";

    public static Map<String, ColumnType> read(Connection connection, String schema, String table)
            throws SQLException {
        Map<String, ColumnType> result = new HashMap<>();
        Set<String> years = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(COLUMN_TYPES_SQL)) {
            statement.setString(1, schema == null ? connection.getSchema() : schema);
            statement.setString(2, table);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String name = rows.getString(1);
                    String type = rows.getString(2);
                    if ("year".equalsIgnoreCase(type)) {
                        years.add(name);
                    }
                    // Only the resolved root is authoritative. A domain called JSON/YEAR
                    // over another type is not sufficient evidence to change its meaning.
                    if (rows.getLong(3) != 0) {
                        continue;
                    }
                    if ("json".equals(type) || "jsonb".equals(type)) {
                        result.put(name, new ColumnType(type, Types.OTHER));
                    } else if (years.contains(name)) {
                        if ("int2".equals(type)) {
                            result.put(name, new ColumnType(type, Types.SMALLINT));
                        } else if ("int4".equals(type)) {
                            result.put(name, new ColumnType(type, Types.INTEGER));
                        } else if ("int8".equals(type)) {
                            result.put(name, new ColumnType(type, Types.BIGINT));
                        }
                    }
                }
            }
        }
        return result;
    }

    public static final class ColumnType {
        private final String typeName;
        private final int jdbcType;

        private ColumnType(String typeName, int jdbcType) {
            this.typeName = typeName;
            this.jdbcType = jdbcType;
        }

        public String getTypeName() { return typeName; }
        public int getJdbcType() { return jdbcType; }
        public int getTisJdbcType() { return jdbcType == Types.OTHER ? Types.VARCHAR : jdbcType; }

        /** Leaves length, scale, defaults, nullability and all other metadata unchanged. */
        public ResultSet correct(ResultSet columns) {
            return (ResultSet) Proxy.newProxyInstance(KingBaseColumnTypes.class.getClassLoader(),
                    new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
                        if (args != null && args.length == 1) {
                            Object field = args[0];
                            boolean name = Integer.valueOf(6).equals(field) || "TYPE_NAME".equals(field);
                            boolean type = Integer.valueOf(5).equals(field) || "DATA_TYPE".equals(field);
                            if (name && ("getString".equals(method.getName()) || "getObject".equals(method.getName()))) {
                                return typeName;
                            }
                            if (type && ("getInt".equals(method.getName()) || "getObject".equals(method.getName()))) {
                                return jdbcType;
                            }
                        }
                        try {
                            return method.invoke(columns, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }
    }
}
