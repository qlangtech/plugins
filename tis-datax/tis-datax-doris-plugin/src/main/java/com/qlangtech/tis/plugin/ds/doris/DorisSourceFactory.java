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

package com.qlangtech.tis.plugin.ds.doris;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-07 09:44
 **/
public class DorisSourceFactory extends BasicDataSourceFactory {

    public static final String NAME_DORIS = "Doris";
    public static final String FIELD_KEY_NODEDESC = "nodeDesc";

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
    public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
        StringBuffer jdbcUrl = new StringBuffer();
        jdbcUrl.append("jdbc:mysql://").append(ip).append(":").append(this.port).append("/").append(dbName);
        return jdbcUrl.toString();
    }


    @Override
    public Connection getConnection(String jdbcUrl) throws SQLException {
        Properties props = new Properties();
        props.put("user", StringUtils.trimToEmpty(this.userName));
        props.put("password", StringUtils.trimToEmpty(password));
        try {
            return mysql5Driver.connect(jdbcUrl, props);
        } catch (SQLException e) {
            throw new RuntimeException("jdbcUrl:" + jdbcUrl + ",props:" + props.toString(), e);
        }
        // return DriverManager.getConnection(jdbcUrl, StringUtils.trimToNull(this.userName), StringUtils.trimToNull(this.password));
    }

    @Override
    public String identityValue() {
        return this.name;
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
        protected boolean validateDSFactory(IControlMsgHandler msgHandler, Context context, BasicDataSourceFactory dsFactory) {
            boolean valid = super.validateDSFactory(msgHandler, context, dsFactory);
            try {
                if (valid) {
                    int[] hostCount = new int[1];
                    DBConfig dbConfig = ((DorisSourceFactory) dsFactory).getDbConfig();
                    dbConfig.vistDbName((config, ip, dbName) -> {
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
            return valid;
        }

    }

    private static List<String> getLoadUrls(String value) {
        return JSONArray.parseArray(value, String.class);
    }

}
