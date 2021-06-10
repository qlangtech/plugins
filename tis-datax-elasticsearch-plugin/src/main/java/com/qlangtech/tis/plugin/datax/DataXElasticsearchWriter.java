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
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXElasticsearchWriter extends DataxWriter implements IDataxContext {
    private static final String DATAX_NAME = "Elasticsearch";
    private static final String FIELD_ENDPOINT = "endpoint";

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String endpoint;

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String index;
    @FormField(ordinal = 16, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String type;

    @FormField(ordinal = 17, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String column;

    @FormField(ordinal = 20, type = FormFieldType.ENUM, validate = {})
    public Boolean cleanup;

    @FormField(ordinal = 24, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer batchSize;

    @FormField(ordinal = 28, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer trySize;
    @FormField(ordinal = 32, type = FormFieldType.INPUTTEXT, validate = {})
    public Integer timeout;

    @FormField(ordinal = 36, type = FormFieldType.ENUM, validate = {})
    public Boolean discovery;
    @FormField(ordinal = 40, type = FormFieldType.ENUM, validate = {})
    public Boolean compression;
    @FormField(ordinal = 44, type = FormFieldType.ENUM, validate = {})
    public Boolean multiThread;
    @FormField(ordinal = 48, type = FormFieldType.ENUM, validate = {})
    public Boolean ignoreWriteError;
    @FormField(ordinal = 52, type = FormFieldType.ENUM, validate = {})
    public Boolean ignoreParseError;

    @FormField(ordinal = 56, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String alias;
    @FormField(ordinal = 60, type = FormFieldType.ENUM, validate = {})
    public String aliasMode;
    @FormField(ordinal = 64, type = FormFieldType.TEXTAREA, validate = {})
    public String settings;

    @FormField(ordinal = 68, type = FormFieldType.INPUTTEXT, validate = {})
    public String splitter;

    @FormField(ordinal = 75, type = FormFieldType.ENUM, validate = {})
    public Boolean dynamic;


    @FormField(ordinal = 79, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public IAliyunToken getToken() {
        return IAliyunToken.getToken(this.endpoint);
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(
                DataXElasticsearchWriter.class, "DataXElasticsearchWriter-tpl.json");
    }

    public static String getDftColumn() {
        JSONArray cols = new JSONArray();
        JSONObject col = null;
        DataxReader reader = DataxWriter.dataReaderThreadlocal.get();
        // Objects.requireNonNull(reader, "reader plugin can not be null");
        if (reader == null) {
            return "[]";
        }
        try {
            List<ISelectedTab> selectedTabs = reader.getSelectedTabs();
            Optional<ISelectedTab> first = selectedTabs.stream().findFirst();
            if (!first.isPresent()) {
                throw new IllegalStateException("can not find any selectedTabs");
            }

            for (ISelectedTab.ColMeta colMeta : first.get().getCols()) {
                col = new JSONObject();
                col.put("name", colMeta.getName());
                col.put("type", colMeta.getType().getLiteria());
                cols.add(col);
            }

            return JsonUtil.toString(cols);
        } finally {
            DataxWriter.dataReaderThreadlocal.remove();
        }
    }

    @Override
    public String getTemplate() {
        return this.template;
    }


    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        return new ESContext(this);
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {
        public DefaultDescriptor() {
            super();
            registerSelectOptions(FIELD_ENDPOINT, () -> ParamsConfig.getItems(IAliyunToken.class));
        }

        public boolean validateSplitter(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            String splitter = StringEscapeUtils.unescapeJava(value);
            if (StringUtils.length(splitter) != 1) {
                msgHandler.addFieldError(context, fieldName, "字符串长度必须为1");
                return false;
            }
            return true;
        }

        public boolean validateSettings(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            try {
                JSON.parseObject(value);
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, "json解析有错误:" + e.getMessage());
                return false;
            }

            return true;
        }

        public boolean validateColumn(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                JSONArray fields = JSON.parseArray(value);
                JSONObject field = null;
                if (fields.size() < 1) {
                    msgHandler.addFieldError(context, fieldName, "请设置column");
                    return false;
                }

                String name = null;
                String type = null;
                StringBuffer err = new StringBuffer();
                for (int i = 0; i < fields.size(); i++) {
                    field = fields.getJSONObject(i);
                    name = field.getString("name");
                    type = field.getString("type");
                    if (StringUtils.isEmpty(name) || StringUtils.isEmpty(type)) {
                        err.append("第").append(i + 1).append("个name或者type为空,");
                    }
                }
                if (err.length() > 0) {
                    msgHandler.addFieldError(context, fieldName, err.toString());
                    return false;
                }
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, "json解析有错误:" + e.getMessage());
                return false;
            }

            return true;
        }

        @Override
        public boolean isSupportMultiTable() {
            return false;
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
