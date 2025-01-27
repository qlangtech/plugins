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

package com.qlangtech.tis.plugin.ds.postgresql;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGProperty;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * https://jdbc.postgresql.org/download.html <br/>
 * sehcme 设置办法：<br/>
 * https://blog.csdn.net/sanyuedexuanlv/article/details/84615388 <br/>
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-23 19:03
 **/
@Public
public abstract class PGLikeDataSourceFactory extends BasicDataSourceFactory implements BasicDataSourceFactory.ISchemaSupported {
    // public static final String DS_TYPE_PG = "PG";

    private static final String FIELD_TAB_SCHEMA = "tabSchema";

    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String tabSchema;


    @Override
    public Optional<String> getEscapeChar() {
        return PG_ESCAPE_COL_CHAR;
    }

    @Override
    public String getDBSchema() {
        return this.tabSchema;
    }

    protected abstract String getDBType();


    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        //https://jdbc.postgresql.org/documentation/head/connect.html#connection-parameters
        String jdbcUrl = buildJdbcUrl(this.getDBType(), ip, this.port, dbName);
        // boolean hasParam = false;
        if (StringUtils.isNotEmpty(this.encode)) {
            // hasParam = true;
            jdbcUrl = jdbcUrl + "&charSet=" + this.encode;
        }
        if (StringUtils.isNotEmpty(this.extraParams)) {
            jdbcUrl = jdbcUrl + "&" + this.extraParams;
        }
        return jdbcUrl;
    }

    public static String buildJdbcUrl(String dbType, String ip, int port, String dbName) {
        return "jdbc:" + dbType + "://" + ip + ":" + port + "/" + dbName + "?ssl=false&stringtype" +
                "=unspecified";
    }

    @Override
    protected void refectTableInDB(TableInDB tabs, JDBCConnection conn) throws SQLException {
        Statement statement = null;
        ResultSet result = null;
        try {
            statement = conn.createStatement();
            if (StringUtils.isEmpty(this.tabSchema)) {
                throw new IllegalStateException("prop tabSchema can not be null");
            }
            if (StringUtils.isEmpty(this.dbName)) {
                throw new IllegalStateException("prop dbName can not be null");
            }
            result =
                    statement.executeQuery("SELECT table_name FROM information_schema.tables  WHERE table_schema = " + "'" + this.tabSchema + "' and table_catalog='" + this.dbName + "'");

            //        DatabaseMetaData metaData = conn.getMetaData();
            //        String[] types = {"TABLE"};
            //        ResultSet tablesResult = metaData.getTables(conn.getCatalog(), "public", "%", types);
            while (result.next()) {
                tabs.add(conn.getUrl(), result.getString(1));
            }
        } finally {
            this.closeResultSet(result);
            try {
                statement.close();
            } catch (Throwable e) {
            }
        }
    }

    @Override
    public JDBCConnection createConnection(String jdbcUrl, Optional<Properties> optProps, boolean verify) throws SQLException {
        final java.sql.Driver jdbcDriver = getJDBCDriver();

        java.util.Properties props = optProps.orElseGet(() -> new Properties());
        props.setProperty(PGProperty.CURRENT_SCHEMA.getName(), this.tabSchema);
        props.setProperty(PGProperty.USER.getName(), this.getUserName());
        props.setProperty(PGProperty.PASSWORD.getName(), this.password);

        return new JDBCConnection(jdbcDriver.connect(jdbcUrl, props), jdbcUrl);
    }

    private transient java.sql.Driver driver;

    private java.sql.Driver getJDBCDriver() {
//        try {
//
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }
        if (driver == null) {
            driver = createDriver();
        }
        return driver;
    }

    protected abstract java.sql.Driver createDriver();


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
                });
                return fix != null ? fix : type;
            }
        };
    }

//    @Override
//    public List<ColumnMetaData> wrapColsMeta(boolean inSink, EntityName table, ResultSet columns1,
//                                             Set<String> pkCols, JDBCConnection conn) throws SQLException, TableNotFoundException {
//        return this.wrapColsMeta(inSink, table, columns1, );
//    }


    //    @Override
    //    public DataDumpers getDataDumpers(TISTable table) {
    //        Iterator<IDataSourceDumper> dumpers = null;
    //        return new DataDumpers(1, dumpers);
    //    }

    //  @TISExtension
    public static abstract class BasicPGLikeDefaultDescriptor extends BasicRdbmsDataSourceFactoryDescriptor {


        @Override
        public boolean supportFacade() {
            return false;
        }

        @Override
        public List<String> facadeSourceTypes() {
            return Collections.emptyList();
        }

        @Override
        protected boolean validateConnection(JDBCConnection conn, BasicDataSourceFactory dsFactory, IControlMsgHandler msgHandler, Context context) throws TisException {
            try {
                PGLikeDataSourceFactory ds = (PGLikeDataSourceFactory) dsFactory;
                String schema = getConnectionSchema(conn);
                if (!StringUtils.equals(ds.tabSchema, schema)) {
                    msgHandler.addFieldError(context, FIELD_TAB_SCHEMA, "Invalid table Schema valid");
                    return false;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return super.validateConnection(conn, dsFactory, msgHandler, context);
        }

//        @Override
//        protected boolean validateDSFactory(IControlMsgHandler msgHandler, Context context,
//                                            BasicDataSourceFactory dsFactory) {
//            try {
//                AtomicBoolean valid = new AtomicBoolean(true);
//                PGLikeDataSourceFactory ds = (PGLikeDataSourceFactory) dsFactory;
//                dsFactory.visitFirstConnection((c) -> {
//                    String schema = getConnectionSchema(c);
//                    if (!StringUtils.equals(ds.tabSchema, schema)) {
//                        msgHandler.addFieldError(context, FIELD_TAB_SCHEMA, "Invalid table Schema valid");
//                        valid.set(false);
//                    }
//                });
//
//                if (!valid.get()) {
//                    return false;
//                }
//                //  List<String> tables = dsFactory.getTablesInDB();
//                // msgHandler.addActionMessage(context, "find " + tables.size() + " table in db");
//            } catch (Exception e) {
//                //logger.warn(e.getMessage(), e);
//                msgHandler.addErrorMessage(context, TisException.getErrMsg(e).getMessage());
//                return false;
//            }
//            return true;
//        }

        protected abstract String getConnectionSchema(JDBCConnection c) throws SQLException;


    }

}
