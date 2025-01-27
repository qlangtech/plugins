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
import com.qlangtech.tis.config.authtoken.UserToken;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;

import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.JdbcUrlBuilder;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-12-14 09:33
 * @see DefaultHiveConnGetter
 **/
public class Hiveserver2DataSourceFactory extends BasicDataSourceFactory implements JdbcUrlBuilder, IHiveConnGetter,
        DataSourceFactory.ISchemaSupported {
    private static final Logger logger = LoggerFactory.getLogger(Hiveserver2DataSourceFactory.class);
    //public static final String NAME_HIVESERVER2 = "Hiveserver2";
    private static final String FIELD_META_STORE_URLS = "metaStoreUrls";
    //    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require,
    //    Validator.identity})
    //    public String name;

    // 数据库名称
    //    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    //    public String dbName;

    @FormField(ordinal = 2, validate = {Validator.require})
    public HiveMeta metadata;

    // "192.168.28.200:10000";
    @FormField(ordinal = 3, validate = {Validator.require})
    public Hms hms;

    //    @FormField(ordinal = 5, validate = {Validator.require})
    //    public UserToken userToken;

    @Override
    public String getDBSchema() {
        return this.dbName;
    }

    @Override
    protected HashSet<String> createAddedCols(EntityName table) throws TableNotFoundException {
        // 去除表的pt键
        HiveTable t = metadata.createMetaStoreClient().getTable(this.dbName, table.getTableName());
        if (t == null) {
            throw new TableNotFoundException(this, "table:" + table.getTableName() + " is not exist");
        }
        return new HashSet<>(t.getPartitionKeys());
    }

    @Override
    public String getJdbcUrl() {
        for (String jdbcUrl : this.getJdbcUrls()) {
            return jdbcUrl;
        }
        throw new IllegalStateException("jdbcUrl can not be empty");
    }

    @Override
    public final Optional<String> getEscapeChar() {
        return Optional.of("`");
    }

    @Override
    public String getMetaStoreUrls() {
        return this.metadata.metaStoreUrls;
    }

    @Override
    public IHiveMetaStore createMetaStoreClient() {
        return metadata.createMetaStoreClient();
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        return IHiveConnGetter.HIVE2_JDBC_SCHEMA + this.hms.hiveAddress + "/" + dbName;
    }

    @Override
    public UserToken getUserToken() {
        return this.hms.userToken;
    }

    @Override
    public JDBCConnection createConnection(String jdbcUrl, Optional<Properties> properties, boolean verify) throws SQLException {
        return getConnection((jdbcUrl), properties, false, verify);
    }

    @Override
    public JDBCConnection getConnection(String jdbcUrl, Optional<Properties> properties, boolean usingPool, boolean verify) throws SQLException {
        return this.hms.getConnection(jdbcUrl, this.dbName, usingPool);
    }

//    @Override
//    public List<ColumnMetaData> wrapColsMeta(
//            boolean inSink, EntityName table, ResultSet columns1, Set<String> pkCols) throws SQLException, TableNotFoundException {
//        return this.wrapColsMeta(inSink, table, columns1, new HiveColumnMetaCreator(pkCols, columns1));
//    }

    @Override
    protected CreateColumnMeta createColumnMetaBuilder(EntityName table, ResultSet columns1, Set<String> pkCols, JDBCConnection conn) {
        return new HiveColumnMetaCreator(pkCols, columns1);
    }

    @Override
    public DBConfig getDbConfig() {

        final DBConfig dbConfig = new DBConfig(this);
        dbConfig.setName(this.dbName);
        String[] addressSplit = StringUtils.split(this.hms.hiveAddress, ":");
        dbConfig.setDbEnum(Collections.singletonMap(addressSplit[0], Collections.singletonList(this.dbName)));
        return dbConfig;
    }

    @Override
    public void visitFirstConnection(IConnProcessor connProcessor) {
        final String hiveJdbcUrl = createHiveJdbcUrl();
        try (JDBCConnection conn = this.getConnection((hiveJdbcUrl), Optional.empty(), false)) {
            connProcessor.vist(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String createHiveJdbcUrl() {
        if (this.hms == null) {
            throw new IllegalStateException("hms can not be null");
        }
        return HiveDBUtils.createHiveJdbcUrl(this.hms.hiveAddress, this.dbName);
    }

    @Override
    protected void refectTableInDB(TableInDB tabs, JDBCConnection conn) throws SQLException {
        throw new UnsupportedOperationException(conn.getUrl());
    }

    @Override
    protected void fillTableInDB(TableInDB tabs) {
        // super.fillTableInDB(tabs);
        String hiveJdbcUrl = createHiveJdbcUrl();
        try (IHiveMetaStore hiveMetaStore = metadata.createMetaStoreClient()) {
            //            TableInDB tabs = TableInDB.create(this);
            List<HiveTable> tables = hiveMetaStore.getTables(this.dbName);
            tables.stream().map((t) -> t.getTableName()).forEach((tab) -> tabs.add(hiveJdbcUrl, tab));
            //  return tabs;
        } catch (Exception e) {
            throw TisException.create("不正确的MetaStoreUrl:" + this.metadata.metaStoreUrls, e);
        }
        // return tabs;
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
        public EndType getEndType() {
            return EndType.HiveMetaStore;
        }

        @Override
        public List<String> facadeSourceTypes() {
            return Collections.emptyList();
        }


        @Override
        protected boolean validateConnection(JDBCConnection c, BasicDataSourceFactory dsFactory, IControlMsgHandler msgHandler, Context context) throws TisException {
            Connection conn = c.getConnection();
            try (Statement statement = conn.createStatement()) {
                try (ResultSet result = statement.executeQuery("select 1")) {
                    if (!result.next()) {
                        throw TisException.create("create jdbc connection faild");
                    }
                    result.getInt(1);
                }
            } catch (TisException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        @Override
        protected boolean validateDSFactory(IControlMsgHandler msgHandler, Context context,
                                            BasicDataSourceFactory dsFactory) {
            boolean valid = super.validateDSFactory(msgHandler, context, dsFactory);

            if (valid) {
                Hiveserver2DataSourceFactory ds = (Hiveserver2DataSourceFactory) dsFactory;
                try (IHiveMetaStore meta = ds.createMetaStoreClient()) {
                    meta.getTables(ds.getDbName());
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                    msgHandler.addFieldError(context, FIELD_META_STORE_URLS, e.getMessage());
                    // throw new RuntimeException(e);
                    return false;
                }
            }
            return valid;
        }

    }
}
