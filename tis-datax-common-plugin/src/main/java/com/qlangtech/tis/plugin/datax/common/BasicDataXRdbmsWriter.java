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

package com.qlangtech.tis.plugin.datax.common;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-23 12:07
 **/
public abstract class BasicDataXRdbmsWriter<DS extends DataSourceFactory> extends DataxWriter {
    public static final String KEY_DB_NAME_FIELD_NAME = "dbName";

    @FormField(identity = false, ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {})
    public String preSql;

    @FormField(ordinal = 6, type = FormFieldType.TEXTAREA, validate = {})
    public String postSql;

    @FormField(ordinal = 9, type = FormFieldType.TEXTAREA, validate = {})
    public String session;

    @FormField(ordinal = 12, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer batchSize;

    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    public boolean autoCreateTable;

    @FormField(ordinal = 15, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    @Override
    public final String getTemplate() {
        return this.template;
    }

    protected DS getDataSourceFactory() {
        if (StringUtils.isBlank(this.dbName)) {
            throw new IllegalStateException("prop dbName can not be null");
        }
        return getDs(this.dbName);
    }

    private static <DS> DS getDs(String dbName) {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(dbName));
        return (DS) dsStore.getPlugin();
    }


    @Override
    protected Class<RdbmsWriterDescriptor> getExpectDescClass() {
        return RdbmsWriterDescriptor.class;
    }

    protected static abstract class RdbmsWriterDescriptor extends BaseDataxWriterDescriptor {
        @Override
        public final boolean isRdbms() {
            return true;
        }
        /**
         * 是否支持自动创建
         *
         * @return
         */
        public boolean isSupportTabCreate() {
            return !this.isRdbms();
        }

        /**
         *
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param val
         * @return
         */
        public boolean validateBatchSize(IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int batchSize = Integer.parseInt(val);
            if (batchSize < 1) {
                msgHandler.addFieldError(context, fieldName, "必须大于0");
                return false;
            }
            int maxVal = getMaxBatchSize();
            if (batchSize > maxVal) {
                msgHandler.addFieldError(context, fieldName, "不能大于" + maxVal);
                return false;
            }
            return true;
        }

        protected int getMaxBatchSize() {
            return 2024;
        }

        public boolean validateDbName(IFieldErrorHandler msgHandler, Context context, String fieldName, String dbName) {
            BasicDataSourceFactory ds = getDs(dbName);
            if (ds.getJdbcUrls().size() > 1) {
                msgHandler.addFieldError(context, fieldName, "不支持分库数据源，目前无法指定数据路由规则，请选择单库数据源");
                return false;
            }
            return true;
        }
    }

}
