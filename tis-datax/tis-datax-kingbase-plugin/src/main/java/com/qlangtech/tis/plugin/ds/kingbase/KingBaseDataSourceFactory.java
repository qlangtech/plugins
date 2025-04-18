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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.kingbase.KingBaseCompatibleMode;
import com.qlangtech.tis.plugin.datax.kingbase.KingBaseDispatch;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.postgresql.PGLikeDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

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
    public static final String JDBC_SCHEMA_TYPE = "kingbase8";
    public static final String FIELD_DB_MODE = "dbMode";
    @FormField(ordinal = 8, validate = {Validator.require})
    public KingBaseCompatibleMode dbMode;

    @FormField(ordinal = 9, validate = {Validator.require})
    public KingBaseDispatch dispatch;

    @Override
    protected java.sql.Driver createDriver() {
        return new com.kingbase8.Driver();
    }

    @Override
    protected java.util.Properties extractSetJdbcProps(java.util.Properties props) {
        Objects.requireNonNull(this.dispatch, "dispatch can not be null").extractSetJdbcProps(props);
        return props;
    }

    @Override
    protected CreateColumnMeta createColumnMetaBuilder(EntityName table, ResultSet columns1, Set<String> pkCols, JDBCConnection conn) {
        return new CreateColumnMeta(pkCols, columns1) {
            @Override
            protected DataType createColDataType(String colName, String typeName, int dbColType, int colSize, int decimalDigits) throws SQLException {
                DataType type = super.createColDataType(colName, typeName, dbColType, colSize, decimalDigits);
                DataType fix = type.accept(new DataType.DefaultTypeVisitor<DataType>() {
                    @Override
                    public DataType bitType(DataType type) {
                        if (StringUtils.lastIndexOfIgnoreCase(type.typeName, "bool") > -1) {
                            return DataType.create(Types.BOOLEAN, type.typeName, type.getColumnSize());
                        }
                        return null;
                    }

                    @Override
                    public DataType timestampType(DataType type) {
                        if (StringUtils.equalsIgnoreCase(type.typeName, "date")) {
                            return DataType.create(Types.DATE, type.typeName, type.getColumnSize());
                        }
                        return null;
                    }
                });
                return fix != null ? fix : type;
            }
        };
    }


    @Override
    public Optional<String> getEscapeChar() {
        return Objects.requireNonNull(dbMode, "dbMode can not be null").getEscapeChar();
    }

    @Override
    protected String getDBType() {
        return JDBC_SCHEMA_TYPE;
    }


    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        return super.buidJdbcUrl(db, ip, dbName);
    }

    @Override
    protected String getCharSetKeyName() {
        return "characterEncoding";
    }

    @Override
    public JDBCConnection createConnection(
            String jdbcUrl, Optional<Properties> props, boolean verify) throws SQLException {
        final JDBCConnection conn = super.createConnection(jdbcUrl, props, verify);
        if (!verify) {
            validateEndTypeMatch(this, conn, (realDBMode) -> {
                throw TisException.create("real dbMode is ："
                        + realDBMode + ",but dbMode in dataSource configuration is " + dbMode.getEndType() + " ,shall be consistent");
            });
        }
        return conn;
    }

    private static void validateEndTypeMatch(KingBaseDataSourceFactory dataSource, JDBCConnection conn, Consumer<String> notifyNotMatch) {
        try {
            // 校验是否
            boolean hasResult = conn.query("SHOW database_mode", (result) -> {
                final String dbMode = result.getString("database_mode");
                if (!dataSource.dbMode.isEndTypeMatch(dbMode)) {
                    // msgHandler.addFieldError(context, FIELD_DB_MODE, "DB实际模式为：" + dbMode);
                    notifyNotMatch.accept(dbMode);
                }

                return false;
            });

            if (!hasResult) {
                throw new IllegalStateException(" has not execute 'SHOW database_mode' success");
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            return validateDSFactory(msgHandler, context, postFormVals.newInstance());

            // return super.validateAll(msgHandler, context, postFormVals);
        }
        @Override
        public Optional<String> getDefaultDataXReaderDescName() {
            return Optional.of(KingBaseDataSourceFactory.KingBase_NAME);
        }

        @Override
        protected boolean validateConnection(JDBCConnection conn
                , BasicDataSourceFactory dsFactory, IControlMsgHandler msgHandler, Context context) throws TisException {
            KingBaseDataSourceFactory dataSource = (KingBaseDataSourceFactory) dsFactory;
            Objects.requireNonNull(dataSource.dbMode, "dbMode can not be null");

            validateEndTypeMatch(dataSource, conn, (realDBMode) -> {
                msgHandler.addFieldError(context, FIELD_DB_MODE, "DB实际模式为：" + realDBMode);
            });

            if (context.hasErrors()) {
                return false;
            }
            return super.validateConnection(conn, dsFactory, msgHandler, context);
        }

        @Override
        protected String getConnectionSchema(JDBCConnection c) throws SQLException {
            return c.getSchema();
        }
    }

}
