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

package com.qlangtech.tis.plugin.ds.mangodb;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.mongo.MongoColumnMetaData;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.JDBCConnectionPool;
import com.qlangtech.tis.plugin.ds.JdbcUrlBuilder;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 15:25
 **/
@Public
public class MangoDBDataSourceFactory extends DataSourceFactory {

    private static final String DS_TYPE_MONGO_DB = "MongoDB";
    private static final Logger logger = LoggerFactory.getLogger(MangoDBDataSourceFactory.class);

    @FormField(ordinal = 1, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String address;
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.forbid_start_with_number, Validator.none_blank})
    public String dbName;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String authMechanism;

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
    public String username;
    @FormField(ordinal = 7, type = FormFieldType.PASSWORD, validate = {})
    public String password;

    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String userSource;

    @FormField(ordinal = 10, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer inspectRowCount;


    public String getDbName() {
        return this.dbName;
    }

    public boolean isContainCredential() {
        return StringUtils.isNotBlank(this.username) && StringUtils.isNotBlank(this.password);
    }

    @Override
    public void refresh() {

    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface)) {
            throw new IllegalStateException(" is not wrapper for :" + iface.getName());
        }
        return (T) createMongoClient();
    }


    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == MongoClient.class;
    }

    public String getUserName() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    @Override
    public DataDumpers getDataDumpers(TISTable table) {
        return DataDumpers.create(Collections.singletonList(this.address), table);
    }

    @Override
    public TableInDB getTablesInDB() {
        //  MongoClient mongoClient = null;
        TableInDB tabs = TableInDB.create(this);

        return vistMongoClient((mongoClient) -> {
            MongoDatabase database = mongoClient.getDatabase(this.dbName);
            for (String tab : database.listCollectionNames()) {
                tabs.add(this.address, tab);
            }
            //  Lists.newArrayList(database.listCollectionNames());
            return tabs;
        });
    }

    protected final <RESULT> RESULT vistMongoClient(Function<MongoClient, RESULT> consumer) {
        try (MongoClient mongoClient = createMongoClient()) {
            return consumer.apply(mongoClient);
        }
    }

    @Override
    protected JDBCConnection createConnection(String jdbcUrl, Optional<Properties> properties, boolean verify) throws SQLException {
        // return super.createConnection(jdbcUrl, verify);
        return new MongoJDBCConnection(createMongoClient());
    }

    @Override
    public void visitFirstConnection(IConnProcessor connProcessor) {

        /**
         * 为了支持mongoCDC 单元测试用
         */
        this.vistMongoClient((mongoClient) -> {
            try {
                connProcessor.vist(new MongoJDBCConnection(mongoClient));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    @Override
    public String toString() {
        return "MangoDBDataSourceFactory{" +
                "address='" + address + '\'' +
                ", dbName='" + dbName + '\'' +
                ", authMechanism='" + authMechanism + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", userSource='" + userSource + '\'' +
                ", inspectRowCount=" + inspectRowCount +
                '}';
    }

    public static class MongoJDBCConnection extends JDBCConnection {
        private final MongoClient mongoClient;

        public MongoJDBCConnection(MongoClient mongoClient) {
            super(null, null);
            this.mongoClient = mongoClient;
        }

        @Override
        public Connection getConnection() {
            // return super.getConnection();
            throw new UnsupportedOperationException();
        }

        public final MongoClient getMongoClient() {
            return this.mongoClient;
        }

        @Override
        public void close() throws SQLException {
            //   super.close();
            this.mongoClient.close();
        }
    }


    @Override
    public DBConfig getDbConfig() {
        // throw new UnsupportedOperationException("getDbConfig");
        JdbcUrlBuilder jdbcUrlBuilder = new JdbcUrlBuilder() {
            @Override
            public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
                return ip + ":" + dbName;
            }

            @Override
            public String identityValue() {
                return MangoDBDataSourceFactory.this.identityValue();
            }
        };
        DBConfig dbConfig = new DBConfig(jdbcUrlBuilder);
        dbConfig.addDbName(this.address, this.dbName);
        dbConfig.setName(this.dbName);
        return dbConfig;
    }

//    @Override
//    public List<ColumnMetaData> getTableMetadata(boolean inSink, EntityName table) {
//        throw new UnsupportedOperationException("table:" + table.getFullName() + " get meta is not supported");
//    }

    @Override
    public List<ColumnMetaData> getTableMetadata(JDBCConnection conn, boolean inSink, EntityName table) throws TableNotFoundException {
        MongoJDBCConnection mongoConn = JDBCConnectionPool.getJDBCConnectionFromPool(conn);
        return getMongoColumnMetaData(mongoConn.getMongoClient(), table);
    }

    @Override
    public final List<ColumnMetaData> getTableMetadata(boolean inSink, EntityName table) throws TableNotFoundException {
        try (MongoClient mongoClient = Objects.requireNonNull(this.unwrap(MongoClient.class), " mongoClient can not "
                + "be null ")) {
            return getMongoColumnMetaData(mongoClient, table);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<ColumnMetaData> getMongoColumnMetaData(MongoClient mongoClient, EntityName table) {

        MangoDBDataSourceFactory plugin = this;
        Map<String, MongoColumnMetaData> colsSchema = Maps.newHashMap();
        int inspectedRows = 0;

        MongoDatabase database = mongoClient.getDatabase(plugin.getDbName());
        final CodecRegistry codecRegistry = database.getCodecRegistry();
        MongoCollection<BsonDocument> user = database.getCollection(table.getTableName(), BsonDocument.class);

        for (BsonDocument doc : user.find().limit(Objects.requireNonNull(inspectRowCount,
                "inspectRowCount can not " + "be" + " null"))) {
            MongoColumnMetaData.parseMongoDocTypes(colsSchema, doc, codecRegistry);
            inspectedRows++;
        }

        if (inspectedRows < 1 || MapUtils.isEmpty(colsSchema)) {
            throw new IllegalStateException(
                    "has not found any cols meta info for:" + table.getFullName()
                            + ",inspectedRows:" + inspectedRows + ",colsSchema.size:" + colsSchema.size());
        }
        List<ColumnMetaData> result = MongoColumnMetaData.reorder(colsSchema);
        return result;
    }


    private MongoClient createMongoClient() {
        List<ServerAddress> serverAddresses = parseServerAddress(getAddressList(this.address));
        MongoClientSettings.Builder setting = MongoClientSettings.builder();
        setting.applyToClusterSettings((builder) -> builder.hosts(serverAddresses));


        if (StringUtils.isNotBlank(this.username) && StringUtils.isNotBlank(this.password)) {
            MongoCredential credential = null;

            if (usernamePasswordAuthMethod.getValue().equals(this.authMechanism)) {

                credential = MongoCredential.createCredential(this.username, this.userSource, password.toCharArray());
                logger.info("create credential by username&password");
            } else {
                AuthenticationMechanism aMechanism = AuthenticationMechanism.fromMechanismName(this.authMechanism);
                switch (aMechanism) {
                    case PLAIN:
                        credential = MongoCredential.createPlainCredential(this.username, this.userSource,
                                password.toCharArray());
                        break;
                    case GSSAPI:
                        credential = MongoCredential.createGSSAPICredential(this.username);
                        break;
//                    case MONGODB_CR:
//                        credential = MongoCredential.createMongoCRCredential(this.username, this.userSource,
//                                password.toCharArray());
//                        break;
                    case SCRAM_SHA_1:
                        credential = MongoCredential.createScramSha1Credential(this.username, this.userSource,
                                password.toCharArray());
                        break;
                    case MONGODB_X509:
                        credential = MongoCredential.createMongoX509Credential(this.username);
                        break;
                    default:
                        throw new IllegalStateException("illegal authMechanism:" + aMechanism);
                }
                logger.info("create credential by " + aMechanism);
            }
            setting.credential(Objects.requireNonNull(credential, "credential can not be null"));
        }
        // mongoClient.close();
        return MongoClients.create(setting.build());
    }

    private static final Option usernamePasswordAuthMethod = new Option("USERNAME & PASSWORD", "usernamePasswordAuthMethod");

    public static String dftAuthMechanism() {
        return (String) usernamePasswordAuthMethod.getValue();
    }

    public static List<Option> allAuthMechanism() {

        List<Option> authMethod = Lists.newArrayList(usernamePasswordAuthMethod);
        authMethod.addAll(Arrays.stream(AuthenticationMechanism.values()).map((e) -> new Option(e.getMechanismName(),
                e.getMechanismName())).collect(Collectors.toList()));
        return authMethod;
    }


    public static List<String> getAddressList(String address) {
        return Lists.newArrayList(StringUtils.split(address, ";"));
    }


    private static List<ServerAddress> parseServerAddress(List<String> rawAddressList) {
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        for (String address : rawAddressList) {
            String[] tempAddress = StringUtils.split(address, ":");// .split(":");
            ServerAddress sa = new ServerAddress(tempAddress[0], Integer.valueOf(tempAddress[1]));
            addressList.add(sa);
        }
        return addressList;
    }


    @TISExtension
    public static class DefaultDescriptor extends DataSourceFactory.BaseDataSourceFactoryDescriptor<MangoDBDataSourceFactory> {
        @Override
        protected String getDataSourceName() {
            return DS_TYPE_MONGO_DB;
        }

        @Override
        public EndType getEndType() {
            return EndType.MongoDB;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }

        public boolean validateAddress(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                List<String> addressList = getAddressList(value);
                for (String address : addressList) {
                    if (!Validator.host.validate(msgHandler, context, fieldName, address)) {
                        return false;
                    }
                }

                List<ServerAddress> serverAddresses = parseServerAddress(addressList);
                if (serverAddresses.size() < 1) {
                    msgHandler.addFieldError(context, fieldName, "请填写");
                    return false;
                }
            } catch (Throwable e) {
                msgHandler.addFieldError(context, fieldName, "格式有误");
                return false;
            }
            return true;
        }

        @Override
        protected boolean validateDSFactory(IControlMsgHandler msgHandler, Context context,
                                            MangoDBDataSourceFactory dsFactory) {

            try {
                TableInDB tabs = dsFactory.getTablesInDB();
            } catch (Exception e) {
                throw TisException.create(e.getMessage(), e);
            }

            return true;
        }
    }
}
