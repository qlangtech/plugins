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

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

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

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String index;
    @FormField(ordinal = 16, type = FormFieldType.INPUTTEXT, validate = {})
    public String type;

    @FormField(ordinal = 17, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String column;

    @FormField(ordinal = 20, type = FormFieldType.INPUTTEXT, validate = {})
    public String cleanup;

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
    @FormField(ordinal = 52, type = FormFieldType.INPUTTEXT, validate = {})
    public Boolean ignoreParseError;

    @FormField(ordinal = 56, type = FormFieldType.INPUTTEXT, validate = {})
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

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(
                DataXElasticsearchWriter.class, "DataXElasticsearchWriter-tpl.json");
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
