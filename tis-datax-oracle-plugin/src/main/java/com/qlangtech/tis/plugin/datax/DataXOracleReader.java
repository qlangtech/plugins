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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.reader.oraclereader.OracleReader
 **/
public class DataXOracleReader extends BasicDataXRdbmsReader<OracleDataSourceFactory> {
    //private static final String DATAX_NAME = "Oracle";
//    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String jdbcUrl;
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String username;
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String password;
//    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String table;
//    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String column;

    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {})
    public Boolean splitPk;

    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer fetchSize;

    @FormField(ordinal = 8, type = FormFieldType.TEXTAREA, validate = {})
    public String session;


    @Override
    protected RdbmsReaderContext createDataXReaderContext(
            String jobName, SelectedTab tab, IDataSourceDumper dumper) {
        OracleReaderContext context = new OracleReaderContext(jobName, tab.getName(), dumper, this);
        return context;
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXOracleReader.class, "DataXOracleReader-tpl.json");
    }


    @TISExtension()
    public static class DefaultDescriptor extends BasicDataXRdbmsReaderDescriptor {
        public DefaultDescriptor() {
            super();
        }

        public boolean validateSession(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return DataXOracleReader.validateSession(msgHandler, context, fieldName, value);
        }

        @Override
        public String getDisplayName() {
            return OracleDataSourceFactory.ORACLE;
        }

    }

    protected static boolean validateSession(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
        try {
            JSONArray cfgs = JSON.parseArray(value);
            for (int i = 0; i < cfgs.size(); i++) {
                Object o = cfgs.get(i);
                if (!(o instanceof String)) {
                    msgHandler.addFieldError(context, fieldName, "第" + (i + 1) + "个属性必须为'String'类型");
                    return false;
                }
            }
        } catch (Exception e) {
            msgHandler.addFieldError(context, fieldName, "JsonArray 格式有误");
            return false;
        }

        return true;
    }
}
