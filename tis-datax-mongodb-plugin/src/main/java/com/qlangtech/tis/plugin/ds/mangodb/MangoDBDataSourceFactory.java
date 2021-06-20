/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.ds.mangodb;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 15:25
 **/
public class MangoDBDataSourceFactory extends DataSourceFactory {

    private static final String DS_TYPE_MONGO_DB = "MongoDB";


    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String address;
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String dbName;
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String username;
    @FormField(ordinal = 4, type = FormFieldType.PASSWORD, validate = {})
    public String password;

    public String getDbName() {
        return this.dbName;
    }

    public boolean isContainCredential() {
        return StringUtils.isNotBlank(this.username) && StringUtils.isNotBlank(this.password);
    }

    public String getUserName() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    @Override
    public DataDumpers getDataDumpers(TISTable table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getTablesInDB() {
        MongoClient mongoClient = null;
        try {
            mongoClient = createMongoClient();
            MongoDatabase database = mongoClient.getDatabase(this.dbName);
            return Lists.newArrayList(database.listCollectionNames());
        } finally {
            try {
                mongoClient.close();
            } catch (Throwable e) {
            }
        }
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(String table) {
//        MongoClient mongoClient = null;
//        try {
//            mongoClient = createMongoClient();
//            MongoDatabase database = mongoClient.getDatabase(this.dbName);
//            MongoCollection collection = database.getCollection(table);
//            //collection.getReadConcern()
//            //  collection.find().map()
//        } finally {
//            try {
//                mongoClient.close();
//            } catch (Throwable e) {
//            }
//        }
        throw new UnsupportedOperationException();
    }

    private MongoClient createMongoClient() {
        MongoClient mongoClient = null;
        List<String> addressList = getAddressList(this.address); //conf.getList(KeyConstant.MONGO_ADDRESS);
        // try {
        if (StringUtils.isNotBlank(this.username) && StringUtils.isNotBlank(this.password)) {
            MongoCredential credential = MongoCredential.createCredential(this.username, this.dbName, password.toCharArray());
            mongoClient = new MongoClient(parseServerAddress(addressList), Arrays.asList(credential));
        } else {
            mongoClient = new MongoClient(parseServerAddress(addressList));
        }
        // mongoClient.close();
        return mongoClient;
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


    @Override
    protected Connection getConnection(String jdbcUrl, String username, String password) throws SQLException {
        //  return DriverManager.getConnection(jdbcUrl, StringUtils.trimToNull(username), StringUtils.trimToNull(password));
        throw new UnsupportedOperationException("getConnection is not support");
    }

    @TISExtension
    public static class DefaultDescriptor extends DataSourceFactory.BaseDataSourceFactoryDescriptor {
        @Override
        protected String getDataSourceName() {
            return DS_TYPE_MONGO_DB;
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

    }
}
