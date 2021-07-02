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
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.runtime.module.misc.impl.DefaultFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXFtpReader extends DataxReader {
    public static final String DATAX_NAME = "FTP";
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String protocol;
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String host;
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer port;
    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer timeout;
    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {})
    public String connectPattern;
    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String username;
    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String password;
    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String path;
    @FormField(ordinal = 8, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String column;
    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String fieldDelimiter;
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {})
    public String compress;
    @FormField(ordinal = 11, type = FormFieldType.ENUM, validate = {})
    public String encoding;
    @FormField(ordinal = 12, type = FormFieldType.ENUM, validate = {})
    public Boolean skipHeader;
    @FormField(ordinal = 13, type = FormFieldType.INPUTTEXT, validate = {})
    public String nullFormat;
    @FormField(ordinal = 14, type = FormFieldType.INT_NUMBER, validate = {})
    public String maxTraversalLevel;
    @FormField(ordinal = 15, type = FormFieldType.TEXTAREA, validate = {})
    public String csvReaderConfig;

    @FormField(ordinal = 16, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXFtpReader.class, "DataXFtpReader-tpl.json");
    }

    @Override
    public boolean hasMulitTable() {
        return false;
    }

    @Override
    public List<ParseColsResult.DataXReaderTabMeta> getSelectedTabs() {
        DefaultContext context = new DefaultContext();
        ParseColsResult parseColsResult = ParseColsResult.parseColsCfg(new DefaultFieldErrorHandler(), context, StringUtils.EMPTY, this.column);
        if (!parseColsResult.success) {
            throw new IllegalStateException("parseColsResult must be success");
        }
        return Collections.singletonList(parseColsResult.tabMeta);
    }

    @Override
    public Iterator<IDataxReaderContext> getSubTasks() {
        IDataxReaderContext readerContext = new DataXFtpReaderContext(this);
        return Collections.singletonList(readerContext).iterator();
    }

    @Override
    public String getTemplate() {
        return template;
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor {
        public DefaultDescriptor() {
            super();
        }

        public boolean validateColumn(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return ParseColsResult.parseColsCfg(msgHandler, context, fieldName, value).success;
        }

        public boolean validateCsvReaderConfig(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                JSONObject cfg = JSON.parseObject(value);
                Set<String> avalibleKeys = Sets.newHashSet();
                avalibleKeys.add("caseSensitive");
                avalibleKeys.add("textQualifier");
                avalibleKeys.add("trimWhitespace");
                avalibleKeys.add("useTextQualifier");
                avalibleKeys.add("delimiter");
                avalibleKeys.add("recordDelimiter");
                avalibleKeys.add("comment");
                avalibleKeys.add("useComments");
                avalibleKeys.add("escapeMode");
                avalibleKeys.add("safetySwitch");
                avalibleKeys.add("skipEmptyRecords");
                avalibleKeys.add("captureRawRecord");
                for (String key : cfg.keySet()) {
                    if (!avalibleKeys.contains(key)) {
                        msgHandler.addFieldError(context, fieldName, "key'" + key + "'是不可接受的");
                        return false;
                    }
                }
            } catch (Throwable e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
            return true;
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
