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
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.authtoken.IKerberosUserToken;
import com.qlangtech.tis.config.authtoken.IOffUserToken;
import com.qlangtech.tis.config.authtoken.IUserNamePasswordUserToken;
import com.qlangtech.tis.config.authtoken.IUserTokenVisitor;
import com.qlangtech.tis.config.authtoken.UserToken;
import com.qlangtech.tis.config.authtoken.impl.OffUserToken;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveMetaStore;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * please use Hiveserver2DataSourceFactory instead
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-28 10:50
 * @see Hiveserver2DataSourceFactory
 **/
@Deprecated()
@Public
public class DefaultHiveConnGetter extends ParamsConfig implements IHiveConnGetter {
    private static Pattern latestPattern = Pattern.compile("((_([a-zA-Z0-9]_?)*)|([a-zA-Z0-9](_?[a-zA-Z0-9])*_?))\\s*([=><]{1,2})\\s*" + HiveTable.KEY_PT_LATEST);


    public static String replaceLastestPtCriteria(String latestFilter
            , java.util.function.Function<String, Pair<Boolean /*值是否是String类型*/, String>> latest) {
        Matcher matcher = latestPattern.matcher(latestFilter);
        if (!matcher.find()) {
            throw new IllegalStateException("param latestFilter:" + latestFilter + " is not match pattern " + latestPattern);
        }
        Pair<Boolean /*值是否是String类型*/, String> matchedPt = latest.apply(matcher.group(1));
        return matcher.replaceFirst("$1 $6 " + createPtVal(matchedPt));
    }

    private static String createPtVal(Pair<Boolean /*值是否是String类型*/, String> matchedPt) {
        final String embellish = matchedPt.getKey() ? "'" : StringUtils.EMPTY;
        return embellish + matchedPt.getValue() + embellish;
    }

    private static final Logger logger = LoggerFactory.getLogger(DefaultHiveConnGetter.class);

    public static final String KEY_HIVE_ADDRESS = "hiveAddress";
    // public static final String KEY_USE_USERTOKEN = "useUserToken";
//    public static final String KEY_USER_NAME = "userName";
//    public static final String KEY_PASSWORD = "password";
    public static final String KEY_META_STORE_URLS = "metaStoreUrls";
    public static final String KEY_DB_NAME = "dbName";

    @FormField(ordinal = 0, validate = {Validator.require, Validator.identity}, identity = true)
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.db_col_name})
    public String dbName;
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String metaStoreUrls;

    // "192.168.28.200:10000";
    @FormField(ordinal = 3, validate = {Validator.require, Validator.host})
    public String hiveAddress;

    @FormField(ordinal = 5, validate = {Validator.require})
    public UserToken userToken;

    private static boolean validateHiveAvailable(IControlMsgHandler msgHandler, Context context, DefaultHiveConnGetter params) {
//        String hiveAddress = postFormVals.getField(KEY_HIVE_ADDRESS);
//        String dbName = postFormVals.getField(KEY_DB_NAME);

        String hiveAddress = params.hiveAddress;
        String dbName = params.dbName;

//        boolean useUserToken = Boolean.parseBoolean(postFormVals.getField(DefaultHiveConnGetter.KEY_USE_USERTOKEN));
//        HiveUserToken userToken = null;
//        if (useUserToken) {
//            userToken = new HiveUserToken(
//                    postFormVals.getField(DefaultHiveConnGetter.KEY_USER_NAME), postFormVals.getField(DefaultHiveConnGetter.KEY_PASSWORD));
//            if (StringUtils.isBlank(userToken.userName)) {
//                msgHandler.addFieldError(context, DefaultHiveConnGetter.KEY_USER_NAME, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
//                return false;
//            }
//        }

        JDBCConnection conn = null;
        try {

            conn = HiveDBUtils.getInstance(hiveAddress, dbName, params.getUserToken()).createConnection();
        } catch (Throwable e) {
            Throwable[] throwables = ExceptionUtils.getThrowables(e);
            for (Throwable t : throwables) {
                if (StringUtils.indexOf(t.getMessage(), "refused") > -1) {
                    msgHandler.addFieldError(context, KEY_HIVE_ADDRESS, "连接地址不可用，请确保连接Hive服务地址可用");
                    return false;
                }
                if (StringUtils.indexOf(t.getMessage(), "NoSuchDatabaseException") > -1) {
                    msgHandler.addFieldError(context, KEY_DB_NAME, "dbName:" + dbName + " is not exist ,please create");
                    return false;
                }
            }
            throw e;
        } finally {
            try {
                conn.close();
            } catch (Throwable e) {
            }
        }
        return true;
    }

    @Override
    public String identityValue() {
        return this.name;
    }


    @Override
    public String getDbName() {
        return this.dbName;
    }

    @Override
    public String getMetaStoreUrls() {
        return this.metaStoreUrls;
    }

    @Override
    public JDBCConnection createConfigInstance() {
        try {
            return HiveDBUtils.getInstance(this.hiveAddress, this.dbName, getUserToken()).createConnection();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IHiveMetaStore createMetaStoreClient() {
        return getiHiveMetaStore(this.metaStoreUrls, this.getUserToken());
    }

    public static IHiveMetaStore getiHiveMetaStore(String metaStoreUrls, UserToken userToken) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(DefaultHiveConnGetter.class.getClassLoader());
            HiveConf hiveCfg = new HiveConf();

            hiveCfg.set(HiveConf.ConfVars.METASTOREURIS.varname, metaStoreUrls);

            //   HiveUserToken userToken = getUserToken();
            //if (userToken.isPresent()) {
            // HiveUserToken hiveToken = userToken.get();
            return userToken.accept(new IUserTokenVisitor<IHiveMetaStore>() {
                @Override
                public IHiveMetaStore visit(IUserNamePasswordUserToken token) throws Exception {
                    throw new UnsupportedOperationException();
                }

                @Override
                public IHiveMetaStore visit(IOffUserToken token) throws Exception {
                    UserGroupInformation.setConfiguration(hiveCfg);
                    return createHiveMetaStore();
                }

                @Override
                public IHiveMetaStore visit(IKerberosUserToken token) {
                    // 有例子： https://blog.csdn.net/weixin_48231806/article/details/120007737
                    hiveCfg.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, token.getKerberosCfg().getPrincipal());
                    hiveCfg.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
                    hiveCfg.setVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, "600s");
                    hiveCfg.setVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, "5s");

                    return HdfsFileSystemFactory.setConfiguration(
                            token.getKerberosCfg(), DefaultHiveConnGetter.class, hiveCfg, () -> createHiveMetaStore());
                    //  token.getKerberosCfg().setConfiguration(hiveCfg);
                }

                private IHiveMetaStore createHiveMetaStore() {
                    try {
                        // final IMetaStoreClient storeClient = Hive.get(hiveCfg, true).getMSC();
                        final IMetaStoreClient storeClient = Hive.getWithFastCheck(hiveCfg, false).getMSC();
                        return new DefaultHiveMetaStore(hiveCfg, storeClient, metaStoreUrls);
                    } catch (Exception e) {
//                        if (ExceptionUtils.indexOfThrowable(e, java.net.ConnectException.class) > -1) {
//                            throw TisException.create(metaStoreUrls, e);
//                        }
                        // throw new RuntimeException(metaStoreUrls, e);
                        throw TisException.create("please check:" + metaStoreUrls, e);
                    }
                }
            });
            //}


        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    // @Override
    public UserToken getUserToken() {
//        return this.useUserToken
//                ? Optional.of(new HiveUserToken(this.userName, this.password)) : Optional.empty();
        if (this.userToken == null) {
            // throw new IllegalStateException("hive userToken can not be null");
            return new OffUserToken();
        }
        return this.userToken;
    }


    @Override
    public String getJdbcUrl() {
        return IHiveConnGetter.HIVE2_JDBC_SCHEMA + this.hiveAddress;
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicParamsConfigDescriptor implements IEndTypeGetter {
        public DefaultDescriptor() {
            super(PLUGIN_NAME);
            // this.registerSelectOptions(HiveFlatTableBuilder.KEY_FIELD_NAME, () -> TIS.getPluginStore(FileSystemFactory.class).getPlugins());
        }

        @Override
        public EndType getEndType() {
            return EndType.HiveMetaStore;
        }

        public boolean validateMetaStoreUrls(IFieldErrorHandler msgHandler, Context context, String fieldName, String metaUrls) {
            Pattern PATTERN_THRIFT_URL = Pattern.compile("thrift://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]");

            Matcher matcher = PATTERN_THRIFT_URL.matcher(metaUrls);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "value:\"" + metaUrls + "\" not match " + PATTERN_THRIFT_URL);
                return false;
            }

            return true;
        }

//        @Override
//        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            return this.verify(msgHandler, context, postFormVals);
//        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            DefaultHiveConnGetter params = (DefaultHiveConnGetter) postFormVals.newInstance();

//            String metaUrls = postFormVals.getField(KEY_META_STORE_URLS);
//            String dbName = postFormVals.getField(KEY_DB_NAME);
            if (!this.validateMetaStoreUrls(msgHandler, context, KEY_META_STORE_URLS, params.getMetaStoreUrls())) {
                return false;
            }


            final ClassLoader currentLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(DefaultHiveConnGetter.class.getClassLoader());
                HiveConf conf = new HiveConf(new Configuration(false), HiveConf.class);
                conf.set(HiveConf.ConfVars.METASTOREURIS.varname, params.getMetaStoreUrls());
                Hive hive = Hive.get(conf);
                Database database = hive.getDatabase(params.getDbName());
                if (database == null) {
                    msgHandler.addFieldError(context, KEY_DB_NAME, "DB:" + params.getDbName() + " 请先在库中创建");
                    return false;
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                msgHandler.addFieldError(context, KEY_META_STORE_URLS, "请检查地址是否可用，" + e.getMessage());
                return false;
            } finally {
                Thread.currentThread().setContextClassLoader(currentLoader);
                try {
                    Hive.closeCurrent();
                } catch (Throwable e) {
                }
            }


            if (!validateHiveAvailable(msgHandler, context, params)) {
                return false;
            }
            return super.verify(msgHandler, context, postFormVals);
        }

        @Override
        public String getDisplayName() {
            return PLUGIN_NAME;
        }
    }


}
