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

package com.qlangtech.plugins.incr.flink.cdc.kingbase.source;

import com.zaxxer.hikari.pool.HikariProxyConnection;
import com.qlangtech.tis.plugin.ds.kingbase.KingBaseColumnTypes;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-19 10:42
 **/
public class KingBaseConnection extends PostgresConnection {

    public KingBaseConnection(
            JdbcConfiguration config
            , PostgresValueConverterBuilder valueConverterBuilder
            , String connectionUsage
            , ConnectionFactory factory) {
        super(config, valueConverterBuilder, connectionUsage, () -> KingBaseConnection.class.getClassLoader(), factory);
    }

    private ResultSet metadataRows;
    private TableId metadataTable;
    private Map<String, KingBaseColumnTypes.ColumnType> actualColumnTypes;

    private synchronized ResultSet correctedColumns(ResultSet columns, TableId table) throws SQLException {
        // Scope the cache to one metadata result set, so later schema refreshes see DDL.
        if (metadataRows != columns || !Objects.equals(metadataTable, table)) {
            actualColumnTypes = KingBaseColumnTypes.read(connection(), table.schema(), table.table());
            metadataRows = columns;
            metadataTable = table;
        }
        KingBaseColumnTypes.ColumnType actual = actualColumnTypes.get(columns.getString(4));
        return actual == null ? columns : actual.correct(columns);
    }

    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columns, TableId table,
            Tables.ColumnNameFilter filter) throws SQLException {
        return super.readTableColumn(correctedColumns(columns, table), table, filter);
    }

    @Override
    public Optional<Column> readColumnForDecoder(ResultSet columns, TableId table,
            Tables.ColumnNameFilter filter) throws SQLException {
        return super.readColumnForDecoder(correctedColumns(columns, table), table, filter);
    }

    @Override
    public synchronized Connection connection() throws SQLException {
        Connection conn = connection(true);
        if (conn instanceof HikariProxyConnection) {
            // assuming HikariCP use org.postgresql.jdbc.PgConnection
            /**
             * @see DispatchConnection dis_con
             * @see KbConnection _con
             */
            return conn.unwrap(com.kingbase8.core.BaseConnection.class);
        }
        return conn;
    }
}
