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

package com.qlangtech.tis.plugin.ds.clickhouse;

import com.alibaba.datax.common.ck.ClickHouseCommon;
import com.google.common.collect.Sets;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;

import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-09 14:38
 **/
@Public
public class ClickHouseDataSourceFactory extends BasicDataSourceFactory {

    private static final String JDBC_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseDataSourceFactory.class);


    public static final String DS_TYPE_CLICK_HOUSE = "ClickHouse";
//    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
//    public String name;
//
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String jdbcUrl;
//    // 必须要有用户名密码，不然datax执行的时候校验会失败
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String username;
//    @FormField(ordinal = 3, type = FormFieldType.PASSWORD, validate = {Validator.require})
//    public String password;


    @Override
    public Optional<String> getEscapeChar() {
        return Optional.of("`");
    }

    @Override
    protected void refectTableInDB(TableInDB tabs, JDBCConnection conn) throws SQLException {
        DatabaseMetaData metaData = conn.getConnection().getMetaData();

        ResultSet tablesResult = metaData.getTables(null, this.dbName, null, new String[]{"TABLE"});

        while (tablesResult.next()) {
            // System.out.println(tablesResult.getString(2) + "," + tablesResult.getString(3));
            if (!StringUtils.equals(this.dbName, tablesResult.getString(2))) {
                continue;
            }
            tabs.add(conn.getUrl(), tablesResult.getString(3));
        }
    }

    @Override
    protected HashSet<String> createAddedCols(EntityName table) {
        // 这样就可以将'__cc_ck_sign' 字端过滤掉了
        HashSet<String> addedCols = Sets.newHashSet(ClickHouseCommon.KEY_CLICKHOUSE_CK);
        return addedCols;
    }

    @Override
    protected CreateColumnMeta createColumnMetaBuilder(
            EntityName table, ResultSet columns1, Set<String> pkCols, JDBCConnection conn) {
        return new CreateColumnMeta(pkCols, columns1) {
            @Override
            protected DataType createColDataType(
                    String colName, String typeName, int dbColType, int colSize, int decimalDigits) throws SQLException {
                if (Types.VARCHAR == dbColType) {
                    if (colSize < 1) {
                        colSize = Short.MAX_VALUE;
                    }
                }
                return super.createColDataType(colName, typeName, dbColType, colSize, decimalDigits);
                // return  DataType.create(dbColType, typeName, colSize);
            }
        };
    }

//    @Override
//    public List<ColumnMetaData> wrapColsMeta(
//            boolean inSink, EntityName table, ResultSet columns1, Set<String> pkCols, JDBCConnection conn) throws SQLException, TableNotFoundException {
//        return this.wrapColsMeta(inSink, table, columns1, );
//    }


    public final String getJdbcUrl() {
        for (String jdbcUrl : this.getJdbcUrls()) {
            return jdbcUrl;
        }
        throw new IllegalStateException("can not find jdbcURL");
    }

//    @Override
//    public List<String> getTablesInDB() {
//
//        List<String> tables = Lists.newArrayList();
//        validateConnection(this.jdbcUrl, (conn) -> {
//
//            DatabaseMetaData metaData = conn.getMetaData();
//
//            ResultSet tablesResult = metaData.getTables(conn.getCatalog(), null, null, new String[]{"TABLE"});
//
//            while (tablesResult.next()) {
//                //System.out.println(tablesResult.getString(2) + "," + tablesResult.getString(3));
//                if (!"default".equalsIgnoreCase(tablesResult.getString(2))) {
//                    continue;
//                }
//                tables.add(tablesResult.getString(3));
//            }
//
//        });
//        return tables;
//    }

    @Override
    public JDBCConnection createConnection(String jdbcUrl, Optional<Properties> properties, boolean verify) throws SQLException {

        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        // return super.getConnection(jdbcUrl, username, password);
        return new JDBCConnection(DriverManager.getConnection(jdbcUrl, StringUtils.trimToNull(this.userName), StringUtils.trimToNull(password)), jdbcUrl);
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        //"jdbc:clickhouse://192.168.28.200:8123/tis",
        String jdbcUrl = "jdbc:clickhouse://" + ip + ":" + this.port + "/" + dbName;
//        if (StringUtils.isNotEmpty(this.encode)) {
//            jdbcUrl = jdbcUrl + "&characterEncoding=" + this.encode;
//        }
//        if (StringUtils.isNotEmpty(this.extraParams)) {
//            jdbcUrl = jdbcUrl + "&" + this.extraParams;
//        }
        return jdbcUrl;
    }

//    @Override
//    public List<ColumnMetaData> getTableMetadata(String table) {
//        return parseTableColMeta(table, this.jdbcUrl);
//    }

    @TISExtension
    public static class DefaultDescriptor extends BasicRdbmsDataSourceFactoryDescriptor {
        @Override
        protected String getDataSourceName() {
            return DS_TYPE_CLICK_HOUSE;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.Clickhouse;
        }

        // private static Pattern PatternClickHouse = Pattern.compile("jdbc:clickhouse://(.+):\\d+/.*");

//        public boolean validateJdbcUrl(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//            Matcher matcher = PatternClickHouse.matcher(value);
//            if (!matcher.matches()) {
//                msgHandler.addFieldError(context, fieldName, "不符合格式规范:" + PatternClickHouse);
//                return false;
//            }
////            File rootDir = new File(value);
////            if (!rootDir.exists()) {
////                msgHandler.addFieldError(context, fieldName, "path:" + rootDir.getAbsolutePath() + " is not exist");
////                return false;
////            }
//            return true;
//        }

//        @Override
//        protected boolean validateDSFactory(IControlMsgHandler msgHandler, Context context, DataSourceFactory dsFactory) {
//            return super.validateDSFactory(msgHandler, context, dsFactory);
//        }
//
//        @Override
//        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//
//            ParseDescribable<DataSourceFactory> ds = this.newInstance((IPluginContext) msgHandler, postFormVals.rawFormData, Optional.empty());
//
//            try {
//                List<String> tables = ds.instance.getTablesInDB();
//                // msgHandler.addActionMessage(context, "find " + tables.size() + " table in db");
//            } catch (Exception e) {
//                logger.warn(e.getMessage(), e);
//                msgHandler.addErrorMessage(context, e.getMessage());
//                return false;
//            }
//
//            return true;
//        }
    }
}
