/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.PluginFieldValidators;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.runtime.module.misc.impl.DefaultFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
@Public
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
    @FormField(ordinal = 6, type = FormFieldType.PASSWORD, validate = {Validator.require})
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

    @FormField(ordinal = 16, type = FormFieldType.TEXTAREA,advance = true , validate = {Validator.require})
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
    public IGroupChildTaskIterator getSubTasks() {
        IDataxReaderContext readerContext = new DataXFtpReaderContext(this);
        return IGroupChildTaskIterator.create(readerContext);
        //return Collections.singletonList(readerContext).iterator();
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
            return PluginFieldValidators.validateCsvReaderConfig(msgHandler, context, fieldName, value);
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
