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

package com.qlangtech.tis.hive;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.hive.HiveUserToken;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.hive.IHiveUserTokenVisitor;
import com.qlangtech.tis.config.hive.impl.IKerberosUserToken;
import com.qlangtech.tis.config.hive.impl.IUserNamePasswordHiveUserToken;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.kerberos.KerberosCfg;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hive.jdbc.Utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-12-14 09:33
 * @see DefaultHiveConnGetter
 **/
public class Hiveserver2DataSourceFactory extends BasicDataSourceFactory implements JdbcUrlBuilder, IHiveConnGetter, DataSourceFactory.ISchemaSupported {
    private static final String NAME_HIVESERVER2 = "Hiveserver2";

//    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
//    public String name;

    // 数据库名称
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
//    public String dbName;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String metaStoreUrls;

    // "192.168.28.200:10000";
    @FormField(ordinal = 3, validate = {Validator.require, Validator.host})
    public String hiveAddress;

    @FormField(ordinal = 5, validate = {Validator.require})
    public HiveUserToken userToken;

    @Override
    public String getDBSchema() {
        return this.dbName;
    }

    @Override
    public String getJdbcUrl() {
        for (String jdbcUrl : this.getJdbcUrls()) {
            return jdbcUrl;
        }
        throw new IllegalStateException("jdbcUrl can not be empty");
    }

    @Override
    public final String getEscapeChar() {
        return "`";
    }

    @Override
    public String getMetaStoreUrls() {
        return this.metaStoreUrls;
    }

    @Override
    public IHiveMetaStore createMetaStoreClient() {
        IHiveMetaStore hiveMetaStore = DefaultHiveConnGetter.getiHiveMetaStore(this.metaStoreUrls, this.userToken);
        return hiveMetaStore;
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        return IHiveConnGetter.HIVE2_JDBC_SCHEMA + this.hiveAddress + "/" + dbName;
    }

    @Override
    public HiveUserToken getUserToken() {
        return this.userToken;
    }

    @Override
    public Connection getConnection(String jdbcUrl) throws SQLException {
        return getConnection(jdbcUrl, false);
    }

    @Override
    public Connection getConnection(String jdbcUrl, boolean usingPool) throws SQLException {
        try {

            if (usingPool) {
                return HiveDBUtils.getInstance(this.hiveAddress, this.dbName, getUserToken()).createConnection();
            } else {
                HiveDriver hiveDriver = new HiveDriver();
                Properties props = new Properties();
                StringBuffer jdbcUrlBuffer = new StringBuffer(jdbcUrl);
                userToken.accept(new IHiveUserTokenVisitor() {
                    @Override
                    public void visit(IUserNamePasswordHiveUserToken ut) {
                        props.setProperty(Utils.JdbcConnectionParams.AUTH_USER, ut.getUserName());
                        props.setProperty(Utils.JdbcConnectionParams.AUTH_PASSWD, ut.getPassword());
                    }

                    @Override
                    public void visit(IKerberosUserToken token) {
                        KerberosCfg kerberosCfg = (KerberosCfg) token.getKerberosCfg();
                        jdbcUrlBuffer.append(";principal=")
                                .append(kerberosCfg.principal)
                                .append(";sasl.qop=").append(kerberosCfg.getKeyTabPath().getAbsolutePath());
                    }
                });
                return hiveDriver.connect(jdbcUrl, props);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DBConfig getDbConfig() {

        final DBConfig dbConfig = new DBConfig(this);
        dbConfig.setName(this.dbName);
        String[] addressSplit = StringUtils.split(this.hiveAddress, ":");
        dbConfig.setDbEnum(Collections.singletonMap(addressSplit[0], Collections.singletonList(this.dbName)));
        return dbConfig;
    }

    @Override
    public void visitFirstConnection(IConnProcessor connProcessor) {
        final String hiveJdbcUrl = createHiveJdbcUrl();
        try (Connection conn = this.getConnection(hiveJdbcUrl)) {
            connProcessor.vist(new JDBCConnection(conn, hiveJdbcUrl));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String createHiveJdbcUrl() {
        return HiveDBUtils.createHiveJdbcUrl(this.hiveAddress, this.dbName, getUserToken());
    }

    @Override
    protected void refectTableInDB(TableInDB tabs, String jdbcUrl, Connection conn) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public final TableInDB getTablesInDB() {
        String hiveJdbcUrl = createHiveJdbcUrl();
        try (IHiveMetaStore hiveMetaStore = DefaultHiveConnGetter.getiHiveMetaStore(this.metaStoreUrls, this.userToken)) {
            TableInDB tabs = TableInDB.create(this);
            List<HiveTable> tables = hiveMetaStore.getTables(this.dbName);
            tables.stream().map((t) -> t.getTableName()).forEach((tab) -> tabs.add(hiveJdbcUrl, tab));
            return tabs;
        } catch (Exception e) {
            throw new TisException("不正确的MetaStoreUrl:" + this.metaStoreUrls, e);
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDataSourceFactory.BasicRdbmsDataSourceFactoryDescriptor {
        @Override
        protected String getDataSourceName() {
            return NAME_HIVESERVER2;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }

        @Override
        public List<String> facadeSourceTypes() {
            return Collections.emptyList();
        }

//        public boolean validateLoadUrl(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//
//            try {
//                List<String> loadUrls = getLoadUrls(value);
//                if (loadUrls.size() < 1) {
//                    msgHandler.addFieldError(context, fieldName, "请填写至少一个loadUrl");
//                    return false;
//                }
//
//                for (String loadUrl : loadUrls) {
//                    if (!Validator.host.validate(msgHandler, context, fieldName, loadUrl)) {
//                        return false;
//                    }
//                }
//
//            } catch (Exception e) {
//                msgHandler.addFieldError(context, fieldName, e.getMessage());
//                return false;
//            }
//
//            return true;
//        }

        @Override
        protected boolean validateDSFactory(IControlMsgHandler msgHandler, Context context, BasicDataSourceFactory dsFactory) {
            boolean valid = super.validateDSFactory(msgHandler, context, dsFactory);

            if (valid) {
                dsFactory.visitFirstConnection((c) -> {
                    Connection conn = c.getConnection();
                    try (Statement statement = conn.createStatement()) {
                        try (ResultSet result = statement.executeQuery("select 1")) {
                            if (!result.next()) {
                                throw new TisException("create jdbc connection faild");
                            }
                            result.getInt(1);
                        }
                    }
                });
            }
//            try {
//                if (valid) {
//                    int[] hostCount = new int[1];
//                    DBConfig dbConfig = ((DorisSourceFactory) dsFactory).getDbConfig();
//                    dbConfig.vistDbName((config, ip, dbName) -> {
//                        hostCount[0]++;
//                        return false;
//                    });
//                    if (hostCount[0] != 1) {
//                        msgHandler.addFieldError(context, FIELD_KEY_NODEDESC, "只能定义一个节点");
//                        return false;
//                    }
//                }
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
            return valid;
        }

    }
}
