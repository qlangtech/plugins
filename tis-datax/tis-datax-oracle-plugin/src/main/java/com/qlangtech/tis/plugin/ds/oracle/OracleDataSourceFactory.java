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
import com.google.common.collect.Maps;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static oracle.jdbc.OracleTypes.BIGINT;
import static oracle.jdbc.OracleTypes.BINARY_DOUBLE;
import static oracle.jdbc.OracleTypes.BINARY_FLOAT;
import static oracle.jdbc.OracleTypes.BIT;
import static oracle.jdbc.OracleTypes.CHAR;
import static oracle.jdbc.OracleTypes.DATE;
import static oracle.jdbc.OracleTypes.DECIMAL;
import static oracle.jdbc.OracleTypes.DOUBLE;
import static oracle.jdbc.OracleTypes.FLOAT;
import static oracle.jdbc.OracleTypes.INTEGER;
import static oracle.jdbc.OracleTypes.LONGVARCHAR;
import static oracle.jdbc.OracleTypes.NUMERIC;
import static oracle.jdbc.OracleTypes.PLSQL_BOOLEAN;
import static oracle.jdbc.OracleTypes.REAL;
import static oracle.jdbc.OracleTypes.SMALLINT;
import static oracle.jdbc.OracleTypes.TIME;
import static oracle.jdbc.OracleTypes.TIMESTAMP;
import static oracle.jdbc.OracleTypes.TINYINT;
import static oracle.jdbc.OracleTypes.VARCHAR;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-24 13:42
 **/
@Public
public class OracleDataSourceFactory extends BasicDataSourceFactory implements DataSourceFactory.ISchemaSupported {

    public static final String ORACLE = "Oracle";

    @FormField(validate = Validator.require, ordinal = 2)
    public ConnEntity connEntity;

    @FormField(ordinal = 8, validate = {Validator.require})
    public Authorized allAuthorized;

    @Override
    public String getDBSchema() {
        return StringUtils.trimToNull(allAuthorized.getSchema());
    }

    @Override
    public Optional<String> getEscapeChar() {
        return Optional.of("\"");
    }

    @Override
    public String getDbName() {
        return Objects.requireNonNull(this.connEntity, "connEntity can not be null").getConnName();
    }

    @Override
    protected void addRefectedTable(TableInDB tabs, JDBCConnection conn, ResultSet resultSet) throws SQLException {
        allAuthorized.addRefectedTable(tabs, conn, resultSet);
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        //        String jdbcUrl = "jdbc:oracle:thin:@" + ip + ":" + this.port + (this.asServiceName ? "/" : ":") +
        //        dbName;
        //        return jdbcUrl;
        return this.connEntity.buidJdbcUrl(ip, this.port);
    }

    @Override
    protected String getRefectTablesSql() {
        return allAuthorized.getRefectTablesSql();
    }

    @Override
    public String toString() {
        return "{" +
                // "asServiceName=" + asServiceName +
                ", allAuthorized=" + allAuthorized + ", name='" + name + '\'' + ", dbName='" + dbName + '\'' + ", " + "userName='" + userName + '\'' + ", password='********" + '\'' + ", nodeDesc='" + nodeDesc + '\'' + ", port=" + port + ", encode='" + encode + '\'' + ", extraParams='" + extraParams + '\'' + '}';
    }

    @Override
    protected ResultSet getColumnsMeta(EntityName table, DatabaseMetaData metaData1) throws SQLException {
        return getColRelevantMeta(table, (tab) -> {
            try {
                return metaData1.getColumns(null, tab.owner.isPresent() ? tab.owner.get() : null, tab.tabName, null);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected ResultSet getPrimaryKeys(EntityName table, DatabaseMetaData metaData1) throws SQLException {

        return getColRelevantMeta(table, (tab) -> {
            try {

                return metaData1.getPrimaryKeys(null, tab.owner.isPresent() ? tab.owner.get() : null, tab.tabName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ResultSet getColRelevantMeta(EntityName table, Function<OracleTab, ResultSet> containSchema) throws SQLException {
        try {
            return containSchema.apply(OracleTab.create(table.getFullName()));
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    //


    private int convert2JdbcType(int dbColType) {
        switch (dbColType) {
            //            case OracleTypes.ARRAY:
            //            case OracleTypes.BIGINT:
            case BIT:// = -7;
                return Types.BIT;
            case TINYINT: // = -6;
                return Types.TINYINT;
            case SMALLINT: // = 5;
                return Types.SMALLINT;
            case INTEGER: // = 4;
                return Types.INTEGER;
            case BIGINT: // = -5;
                return Types.BIGINT;
            case FLOAT: // = 6;
                return Types.FLOAT;
            case REAL: // = 7;
            case BINARY_FLOAT:
                return Types.REAL;
            case DOUBLE: // = 8;
            case BINARY_DOUBLE:
                return Types.DOUBLE;
            case NUMERIC: // = 2;
            case DECIMAL: // = 3;
                //https://wenku.baidu.com/view/7f973fd54593daef5ef7ba0d4a7302768e996f35.html
                return Types.DECIMAL;
            case CHAR: // = 1;
                return Types.CHAR;
            case VARCHAR: // = 12;
                return Types.VARCHAR;
            case LONGVARCHAR: // = -1;
                return Types.LONGVARCHAR;
            case DATE: // = 91;
                return Types.DATE;
            case TIME: //= 92;
                return Types.TIME;
            case TIMESTAMP: // = 93;
                return Types.TIMESTAMP;
            case PLSQL_BOOLEAN: // = 252;
                return Types.BOOLEAN;
            default:
                return dbColType;
        }
    }

    @Override
    protected CreateColumnMeta createColumnMetaBuilder(EntityName table, ResultSet columns1, Set<String> pkCols, JDBCConnection conn) {

        return new CreateColumnMeta(pkCols, columns1) {
            private Map<String, String> _col2Comment;
            /**
             * https://github.com/CodePhiliaX/Chat2DB/blob/main/chat2db-server/chat2db-plugins/chat2db-oracle/src/main/java/ai/chat2db/plugin/oracle/OracleMetaData.java
             */
            private static final String TABLE_COMMENT_SQL = "select owner, table_name, comments from ALL_TAB_COMMENTS where OWNER = ?  and TABLE_NAME = ?";

            private static final String TABLE_COLUMN_COMMENT_SQL =
                    " SELECT owner, table_name, column_name, comments "
                            + "             FROM all_col_comments "
                            + "   WHERE  owner = ? and table_name = ? and comments is not null ";

            @Override
            protected DataType createColDataType(
                    String colName, String typeName, int dbColType, int colSize, int decimalDigits) throws SQLException {
                // 类似oracle驱动内部有一套独立的类型 oracle.jdbc.OracleTypes,有需要可以在具体的实现类里面去实现
                // return DataType.create(convert2JdbcType(dbColType), typeName, colSize);

                return super.createColDataType(colName, typeName, convert2JdbcType(dbColType), colSize, decimalDigits);
            }

            @Override
            public ColumnMetaData create(String colName, int index) throws SQLException {
                ColumnMetaData metaData = super.create(colName, index);
                metaData.setComment(getColumnComment().get(colName));
                return metaData;
            }

            private Map<String, String> getColumnComment() {
                if (_col2Comment == null) {
                    _col2Comment = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                    try (PreparedStatement statement = conn.preparedStatement(TABLE_COLUMN_COMMENT_SQL)) {

                        statement.setString(1, StringUtils.defaultString(removeEscapeChar(table.getDbName()), conn.getSchema()));
                        statement.setString(2, removeEscapeChar(table.getTableName()));
                        try (ResultSet result = statement.executeQuery()) {
                            while (result.next()) {
                                _col2Comment.put(result.getString(3), result.getString(4));
                            }
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                return this._col2Comment;
            }

            @Override
            protected DataType getDataType(String keyName) throws SQLException {

                //        int columnCount = cols.getMetaData().getColumnCount();
                //        String colName = null;
                //        for (int i = 1; i <= columnCount; i++) {
                //            colName = cols.getMetaData().getColumnName(i);
                //            System.out.print(colName + ":" + cols.getString(colName) + ",");
                //        }
                //        System.out.println();

                DataType type = super.getDataType(keyName);
                // Oracle会将int，smallint映射到Oracle数据库都是number类型，number类型既能表示浮点和整型，所以这里要用进度来鉴别是整型还是浮点
                if (type.getJdbcType() == JDBCTypes.DECIMAL || type.getJdbcType() == JDBCTypes.NUMERIC) {
                    int decimalDigits = type.getDecimalDigits();// cols.getInt("decimal_digits");
                    if (decimalDigits < 1) {
                        return DataType.create(type.getColumnSize() > 8 ? Types.BIGINT : Types.INTEGER,
                                type.typeName, type.getColumnSize());
                    }
                }

                // 当MySQL中的Date类型映射到Oracle中时，Oracle作为Sink端应该作为Date类型 https://github.com/qlangtech/tis/issues/192
                //if (inSink && "DATE".equalsIgnoreCase(type.typeName)) {
                if ("DATE".equalsIgnoreCase(type.typeName)) {
                    // 由于Oracle的Date类型在实际上是精确到秒的，不能简单输出成Date类型
                    return DataType.create(Types.DATE, type.typeName, type.getColumnSize());
                }


                return type;
            }
        };
    }

//    @Override
//    public List<ColumnMetaData> wrapColsMeta(boolean inSink, EntityName table, ResultSet columns1,
//                                             Set<String> pkCols,JDBCConnection conn) throws SQLException, TableNotFoundException {
//        return this.wrapColsMeta(inSink, table, columns1, );
//    }


    @Override
    public JDBCConnection createConnection(String jdbcUrl, boolean verify) throws SQLException {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        Connection conn = DriverManager.getConnection(jdbcUrl, StringUtils.trimToNull(this.userName),
                StringUtils.trimToNull(password));

        String dbSchema = null;
        if (StringUtils.isNotEmpty(dbSchema = getDBSchema())) {
            try (Statement statement = conn.createStatement()) {
                statement.execute("ALTER SESSION SET CURRENT_SCHEMA = " + dbSchema);
            }
        }

        return new JDBCConnection(conn, jdbcUrl);
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
        public EndType getEndType() {
            return EndType.Oracle;
        }

        @Override
        public boolean validateExtraParams(IFieldErrorHandler msgHandler, Context context, String fieldName,
                                           String value) {
            return true;
        }
    }
}
