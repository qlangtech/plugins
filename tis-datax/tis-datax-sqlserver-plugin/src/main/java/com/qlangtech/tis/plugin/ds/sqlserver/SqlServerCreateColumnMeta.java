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

import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory.CreateColumnMeta;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-22 22:43
 **/
public class SqlServerCreateColumnMeta extends CreateColumnMeta {

    public static final String KEY_MS_DESCRIPTION = "MS_Description";

    private final static String SQL_SELECT_COLUMN_COMMENT =
            "SELECT   col.name AS ColumnName, \n" +
                    "    ep.value AS Description \n" +
                    "FROM \n" +
                    "    sys.columns col \n" +
                    "LEFT JOIN \n" +
                    "    sys.extended_properties ep ON ep.major_id = col.object_id AND ep.minor_id = col.column_id \n" +
                    "JOIN \n" +
                    "    sys.tables tbl ON col.object_id = tbl.object_id \n" +
                    "JOIN \n" +
                    "    sys.schemas sch ON tbl.schema_id = sch.schema_id \n" +
                    "JOIN \n" +
                    "    sys.databases db ON db.database_id = DB_ID()\n" +
                    "WHERE \n" +
                    "    db.name = ?" +
                    "    AND sch.name = ?" +
                    "    AND tbl.name = ?" +
                    "    AND ep.name = '" + KEY_MS_DESCRIPTION + "'";

    private final JDBCConnection conn;
    private final EntityName table;

    private Map<String, String> _col2Comment;

    public SqlServerCreateColumnMeta(EntityName table, Set<String> pkCols, ResultSet columns1, JDBCConnection conn) {
        super(pkCols, columns1);
        this.conn = conn;
        this.table = table;
    }

    private Map<String, String> getColumnComment() {
        if (_col2Comment == null) {
            _col2Comment = Maps.newTreeMap();
            try (PreparedStatement statement = conn.preparedStatement(SQL_SELECT_COLUMN_COMMENT)) {
                statement.setString(1, conn.getCatalog());
                statement.setString(2, conn.getSchema());
                statement.setString(3, table.getTableName());
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        _col2Comment.put(result.getString(1), result.getString(2));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return this._col2Comment;
    }

    @Override
    public ColumnMetaData create(String colName, int index) throws SQLException {
        ColumnMetaData colMetaData = super.create(colName, index);
        colMetaData.setComment(getColumnComment().get(colName));
        return colMetaData;
    }
}
