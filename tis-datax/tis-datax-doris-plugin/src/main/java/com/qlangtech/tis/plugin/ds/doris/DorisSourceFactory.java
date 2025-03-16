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

package com.qlangtech.tis.plugin.ds.doris;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.ConfigFileContext;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.manage.common.PostFormStreamProcess;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;

import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataType.DefaultTypeVisitor;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-07 09:44
 **/
@Public
public class DorisSourceFactory extends BasicDataSourceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DorisSourceFactory.class);

    public static final String NAME_DORIS = "Doris";
    public static final String FIELD_KEY_NODEDESC = "nodeDesc";
    private static final String FIELD_KEY_LOAD_URL = "loadUrl";

    private static final com.mysql.jdbc.Driver mysql5Driver;

    static {
        try {
            mysql5Driver = new com.mysql.jdbc.Driver();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @FormField(ordinal = 8, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String loadUrl;


    public List<String> getLoadUrls() {
        return DorisSourceFactory.getLoadUrls(this.loadUrl);
    }

    @Override
    public Optional<String> getEscapeChar() {
        return Optional.of("`");
    }

    @Override
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        StringBuffer jdbcUrl = new StringBuffer();
        jdbcUrl.append("jdbc:mysql://").append(ip).append(":").append(this.port);

        if (StringUtils.isNotEmpty(dbName)) {
            jdbcUrl.append("/").append(dbName);
        }
        return jdbcUrl.toString();
    }


    @Override
    public JDBCConnection createConnection(String jdbcUrl, Optional<Properties> properties, boolean verify) throws SQLException {
        Properties props = properties.orElse(new Properties());
        props.put("useSSL", "false");
        props.put("user", StringUtils.trimToEmpty(this.userName));
        if (StringUtils.isNotEmpty(this.password)) {
            props.put("password", StringUtils.trimToEmpty(this.password));
        }
        try {
            return new JDBCConnection(mysql5Driver.connect(jdbcUrl, props), jdbcUrl);
        } catch (SQLException e) {
            throw TisException.create(e.getMessage() + ",jdbcUrl:" + jdbcUrl + ",props:" + props.toString(), e);
        }
    }

    final static TreeMap<String, DataTypeFixer> dateTypeMapper = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    static {
        dateTypeMapper.put("datev2", (colSize, decimalDigits) -> DataType.getType(JDBCTypes.DATE));
        dateTypeMapper.put("date", (colSize, decimalDigits) -> DataType.getType(JDBCTypes.DATE));
        dateTypeMapper.put("datetime", (colSize, decimalDigits) -> DataType.getType(JDBCTypes.TIMESTAMP));
        dateTypeMapper.put("datetimev2", (colSize, decimalDigits) -> DataType.getType(JDBCTypes.TIMESTAMP));
        dateTypeMapper.put("decimal", (colSize, decimalDigits) -> new DataType(JDBCTypes.DECIMAL, colSize).setDecimalDigits(decimalDigits));
        dateTypeMapper.put("decimalv3", (colSize, decimalDigits) -> new DataType(JDBCTypes.DECIMAL, colSize).setDecimalDigits(decimalDigits));
    }

//    @Override
//    public List<ColumnMetaData> wrapColsMeta(boolean inSink, EntityName table, ResultSet columns1,
//                                             Set<String> pkCols) throws SQLException, TableNotFoundException {
//
//
//        return this.wrapColsMeta(inSink, table, columns1, );
//    }

    @Override
    protected CreateColumnMeta createColumnMetaBuilder(EntityName table, ResultSet columns1, Set<String> pkCols, JDBCConnection conn) {
        return new CreateColumnMeta(pkCols, columns1) {

            @Override
            protected DataType getDataType(String colName) throws SQLException {
                DataType type = super.getDataType(colName);
                DataType fixType = type.accept(new DefaultTypeVisitor<DataType>() {
                    @Override
                    public DataType varcharType(DataType type) {
                        // 支持Doris的json类型
                        if (isJSONColumnType(type)) {
                            DataType jsonType = DataType.create(JDBCTypes.VARCHAR.getType(), type.typeName, 1000); //.createVarChar(1000);
                            return jsonType;
                        }
                        return null;
                    }
                });
                return fixType != null ? fixType : type;
            }

            @Override
            protected DataType createColDataType(String colName, String typeName, int dbColType, int colSize, int decimalDigits) throws SQLException {
                DataTypeFixer dateType = null;
                if ((dateType = dateTypeMapper.get(typeName)) != null) {
                    return dateType.create(colSize, decimalDigits);
                } else {
                    //  return DataType.create(dbColType, typeName, colSize);
                    return super.createColDataType(colName, typeName, dbColType, colSize, decimalDigits);
                }
            }
        };
    }

    @FunctionalInterface
    private interface DataTypeFixer {
        DataType create(int colSize, int decimalDigits);
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicRdbmsDataSourceFactoryDescriptor {
        @Override
        protected String getDataSourceName() {
            return NAME_DORIS;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.Doris;
        }

        @Override
        public List<String> facadeSourceTypes() {
            return Collections.emptyList();
        }

        public boolean validateLoadUrl(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            try {
                List<String> loadUrls = getLoadUrls(value);
                if (loadUrls.size() < 1) {
                    msgHandler.addFieldError(context, fieldName, "请填写至少一个loadUrl");
                    return false;
                }

                for (String loadUrl : loadUrls) {
                    if (!Validator.host.validate(msgHandler, context, fieldName, loadUrl)) {
                        return false;
                    }
                }

            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }

            return true;
        }

        @Override
        protected boolean validateDSFactory(final IControlMsgHandler msgHandler, final Context context, BasicDataSourceFactory dsFactory) {
            boolean valid = super.validateDSFactory(msgHandler, context, dsFactory);
            try {
                if (valid) {

                    final DorisSourceFactory dorisDS = (DorisSourceFactory) dsFactory;

                    for (String feLoadHost : dorisDS.getLoadUrls()) {
                        //利用doris的clusterAction： https://doris.apache.org/zh-CN/docs/1.2/admin-manual/http-actions/fe/cluster-action
                        // 对:{"msg":"success","code":0,"data":{"http":["192.168.28.200:8030"],"mysql":["192.168.28.200:9030"]},"count":0}
                        StringBuffer clusterInfoApiUrl = new StringBuffer("http://");
                        clusterInfoApiUrl.append(feLoadHost).append("/rest/v2/manager/cluster/cluster_info/conn_info");
                        try {
                            Boolean success = HttpUtils.get(new URL(clusterInfoApiUrl.toString()), new PostFormStreamProcess<Boolean>() {
                                @Override
                                public ContentType getContentType() {
                                    return ContentType.JSON;
                                }

                                @Override
                                public void preSet(HttpURLConnection conn) throws IOException {
                                    super.preSet(conn);
                                    ConfigFileContext.StreamProcess.setAuthorization(conn, dorisDS.getUserName(), dorisDS.getPassword());
                                }

                                @Override
                                public Boolean p(int status, InputStream stream, Map<String, List<String>> headerFields) {
                                    try {
                                        JSONObject result = JSONObject.parseObject(IOUtils.toString(stream, TisUTF8.get()));
                                        final String msg = result.getString("msg");
                                        if (!"success".equals(msg)) {
                                            msgHandler.addFieldError(context, FIELD_KEY_LOAD_URL, msg);
                                            return false;
                                        }
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return true;
                                }

                                @Override
                                public void error(int status, InputStream errstream, IOException e) throws Exception {
                                    logger.warn(e.getMessage(), e);
                                    msgHandler.addFieldError(context, FIELD_KEY_LOAD_URL, IOUtils.toString(errstream, TisUTF8.get()));
                                }
                            });
                            if (success == null || !success) {
                                break;
                            }
                        } catch (TisException e) {
                            logger.warn(e.getMessage(), e);
                            msgHandler.addFieldError(context, FIELD_KEY_LOAD_URL, "host:" + feLoadHost + "有误");
                        }
                    }
                    int[] hostCount = new int[1];
                    DBConfig dbConfig = dsFactory.getDbConfig();
                    dbConfig.vistDbName((config, jdbcUrl, ip, dbName) -> {
                        hostCount[0]++;
                        return false;
                    });
                    if (hostCount[0] != 1) {
                        msgHandler.addFieldError(context, FIELD_KEY_NODEDESC, "只能定义一个节点");
                        return false;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return valid && !context.hasErrors();
        }


    }

    private static List<String> getLoadUrls(String value) {
        return JSONArray.parseArray(value, String.class);
    }

}
