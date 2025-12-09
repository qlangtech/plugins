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
import com.kingbase8.KBProperty;
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
public abstract class BasicKingBaseDataSourceFactory extends PGLikeDataSourceFactory {
    public static final String KingBase_NAME = "KingBase";
    public static final String KingBase_Ver8 = "-V8.x";
    public static final String KingBase_Ver9 = "-V9.x";
    public static final String JDBC_SCHEMA_TYPE_V9 = "kingbase8";
    public static final String JDBC_SCHEMA_TYPE_V8 = JDBC_SCHEMA_TYPE_V9;
    public static final String FIELD_DB_MODE = "dbMode";
    @FormField(ordinal = 8, validate = {Validator.require})
    public KingBaseCompatibleMode dbMode;

    @FormField(ordinal = 9, validate = {Validator.require})
    public KingBaseDispatch dispatch;


    @Override
    protected java.util.Properties extractSetJdbcProps(java.util.Properties props) {
        Objects.requireNonNull(this.dispatch, "dispatch can not be null").extractSetJdbcProps(props);
        if (StringUtils.isEmpty(this.encode)) {
            throw new IllegalStateException("param encode can not be empty");
        }
        props.setProperty(KBProperty.CLIENT_ENCODING.getName(), this.encode);
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
                        // if (StringUtils.lastIndexOfIgnoreCase(type.typeName, "bool") > -1) {
                        return DataType.create(Types.BOOLEAN, type.typeName, type.getColumnSize());
                        //}
                        //return null;
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

//    @Override
//    protected String getDBType() {
//        return JDBC_SCHEMA_TYPE_V8;
//    }


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

    private static void validateEndTypeMatch(BasicKingBaseDataSourceFactory dataSource, JDBCConnection conn, Consumer<String> notifyNotMatch) {
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

    //  @TISExtension
    public static abstract class BaiscKingBaseDSDescriptor extends BasicPGLikeDefaultDescriptor {


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
            return Optional.of(BasicKingBaseDataSourceFactory.KingBase_NAME);
        }

        @Override
        protected boolean validateConnection(JDBCConnection conn
                , BasicDataSourceFactory dsFactory, IControlMsgHandler msgHandler, Context context) throws TisException {
            BasicKingBaseDataSourceFactory dataSource = (BasicKingBaseDataSourceFactory) dsFactory;
            Objects.requireNonNull(dataSource.dbMode, "dbMode can not be null");

//            validateKingbaseVersion(conn, msgHandler, context, dataSource);
//            if (context.hasErrors()) {
//                return false;
//            }
            validateEndTypeMatch(dataSource, conn, (realDBMode) -> {
                msgHandler.addFieldError(context, FIELD_DB_MODE, "DB实际模式为：" + realDBMode);
            });

            if (context.hasErrors()) {
                return false;
            }
            return super.validateConnection(conn, dsFactory, msgHandler, context);
        }
// 先不对版本进行校验
//        protected void validateKingbaseVersion(JDBCConnection conn, IControlMsgHandler msgHandler, Context context, BasicKingBaseDataSourceFactory dataSource) {
//            // 检查 Kingbase 数据库版本
//            try {
//                boolean hasVersionCheck = conn.query("SELECT version()", (result) -> {
//                    String versionInfo = result.getString(1);
//                    // 解析版本号，例如: "KingbaseES V008R006C008B0014" 或 "KingbaseES V009R001C001B0001"
//                    if (versionInfo != null) {
//                        // 判断数据库版本
//                        boolean isDBV9OrAbove = versionInfo.contains("V009")
//                                || versionInfo.contains("V010")
//                                || versionInfo.contains("V011")
//                                || versionInfo.contains("V012");
//
//                        // 判断当前连接器类型
//                        // 如果 getDBType() 返回 JDBC_SCHEMA_TYPE_V8，说明是 v8 连接器
//                        // 否则认为是 v9+ 连接器
//                        boolean isV8Connector = JDBC_SCHEMA_TYPE_V8.equals(dataSource.getDBType());
//
//                        // 版本不匹配的情况
//                        if (!isDBV9OrAbove && !isV8Connector) {
//                            // 数据库是 v8 或更低，但使用的不是 v8 连接器
//                            msgHandler.addErrorMessage(context, "请使用kingbase v8连接器连接");
//                        }
//                    }
//                    return false;
//                });
//
//                if (!hasVersionCheck) {
//                    // 如果无法通过 version() 获取版本信息，尝试其他方式
//                    conn.query("SHOW server_version", (result) -> {
//                        String versionInfo = result.getString("server_version");
//                        if (versionInfo != null) {
//                            // 解析版本号 例如: "8.6" 或 "9.0"
//                            String[] parts = versionInfo.split("\\.");
//                            if (parts.length > 0) {
//                                try {
//                                    int majorVersion = Integer.parseInt(parts[0]);
//                                    boolean isV8Connector = JDBC_SCHEMA_TYPE_V8.equals(dataSource.getDBType());
//
//                                    if (majorVersion < 9 && !isV8Connector) {
//                                        // 数据库版本低于 v9，但使用的不是 v8 连接器
//                                        msgHandler.addErrorMessage(context, "请使用kingbase v8连接器连接");
//                                    }
//                                } catch (NumberFormatException e) {
//                                    // 版本号解析失败，忽略
//                                }
//                            }
//                        }
//                        return false;
//                    });
//                }
//            } catch (Exception e) {
//                // 获取版本信息失败，记录错误但不阻止连接
//                // 可以选择记录日志或忽略
//            }
//        }

        @Override
        protected String getConnectionSchema(JDBCConnection c) throws SQLException {
            return c.getSchema();
        }
    }

}
