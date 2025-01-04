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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.json;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSON;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.Options;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.TISFlinkProp;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader.StructuredRecord;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.canal.CanalJsonFormatFactory;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

/**
 * 以Source 端使用的
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-28 17:10
 **/
public class SourceJsonFormatFactory extends FormatFactory {


    @FormField(ordinal = 1, type = FormFieldType.ENUM, advance = true)
    public Boolean failOnMissingField;
    @FormField(ordinal = 2, type = FormFieldType.ENUM, advance = true)
    public Boolean ignoreParseErrors;
    @FormField(ordinal = 3, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public String nullKeyMode;
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, advance = true)
    public String nullKeyLiteral;
    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, advance = true)
    public String timestampFormat;

    @FormField(ordinal = 6, type = FormFieldType.ENUM, advance = true)
    public Boolean encodeDecimalAsPlanNumber;

    @FormField(ordinal = 7, type = FormFieldType.ENUM, advance = true)
    public Boolean encodeJsonParserEnabled;

    @Override
    public boolean acceptMultipleTable() {
        /**
         * 由于
         */
        return false;
    }

    @Override
    public String getNullFormat() {
        return this.nullKeyLiteral;
    }

    @Override
    protected String getTimestampFormat() {
        return this.timestampFormat;
    }


    @Override
    public KafkaStructuredRecord parseRecord(KafkaStructuredRecord reuse, byte[] record) {

        HashMap jsonObject = JSON.parseObject(record, HashMap.class);
        reuse.setTabName(StructuredRecord.DEFAUTL_TABLE_NAME);
        reuse.setVals(jsonObject);
        return reuse;
    }

    @Override
    public final boolean validateFormtField(IControlMsgHandler msgHandler, Context context, String fieldName, DataxReader dataxReader) {
        List<ISelectedTab> tabs = dataxReader.getSelectedTabs();
        if (tabs.size() > 1) {
            msgHandler.addFieldError(context, fieldName
                    , "由于‘" + org.apache.flink.formats.json.JsonFormatFactory.IDENTIFIER
                            + "'格式不支持Table名称标记，至多只能选一张表,请使用其他format，例如：'" + CanalJsonFormatFactory.IDENTIFIER + "'");
            return false;
        }
        return true;
    }

//    @Override
//    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(String targetTabName) {
//        return createFormat(targetTabName, (factory, cfg) -> {
//            return factory.createDecodingFormat(null, cfg);
//        });
//    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(String targetTabName) {
//        org.apache.flink.formats.json.JsonFormatFactory formatFactory = new org.apache.flink.formats.json.JsonFormatFactory();
//        SourceDescriptor desc = (SourceDescriptor) this.getDescriptor();
//        Configuration cfg = desc.options.createFlinkCfg(this);
//        return formatFactory.createEncodingFormat(null, cfg.set(JsonFormatOptions.TARGET_TABLE_NAME, targetTabName));

        return createFormat(targetTabName, (factory, cfg) -> {
            return factory.createEncodingFormat(null, cfg);
        });
    }

    protected <T> T createFormat(String targetTabName
            , BiFunction<org.apache.flink.formats.json.JsonFormatFactory, Configuration, T> formatCreator) {
        org.apache.flink.formats.json.JsonFormatFactory formatFactory = new org.apache.flink.formats.json.JsonFormatFactory();

        SourceDescriptor desc = (SourceDescriptor) this.getDescriptor();
        return formatCreator.apply(formatFactory
                , desc.options.createFlinkCfg(this)
                        .set(JsonFormatOptions.TARGET_TABLE_NAME, targetTabName));

//        return canalFormatFactory.createEncodingFormat(null
//                , desc.options.createFlinkCfg(this).set(JsonFormatOptions.TARGET_TABLE_NAME, targetTabName));
    }


    @TISExtension
    public static class SourceDescriptor extends BasicFormatDescriptor {


        public SourceDescriptor() {
            super();
        }

        @Override
        public boolean validateTabEntities(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            String[] tabs = splitTabEntities(value);
            if (tabs.length < 1) {
                msgHandler.addFieldError(context, fieldName, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
                return false;
            }

            if (tabs.length > 1) {
                msgHandler.addFieldError(context, fieldName, "设置不能超过一个实体表名称");
                return false;
            }

            return super.validateTabEntities(msgHandler, context, fieldName, value);
        }

        @Override
        public EndType getEndType() {
            return EndType.SOURCE;
        }

        @Override
        protected void appendOptionCfgs(Options options) {
            options.add("failOnMissingField", TISFlinkProp.create(JsonFormatOptions.FAIL_ON_MISSING_FIELD));
            options.add("ignoreParseErrors", TISFlinkProp.create(JsonFormatOptions.IGNORE_PARSE_ERRORS));

            addNullKeyOptCfg(options);

//            options.add("mapNullKeyMode", TISFlinkProp.create(JsonFormatOptions.MAP_NULL_KEY_MODE));
//            options.add("mapNullKeyLiteral", TISFlinkProp.create(JsonFormatOptions.MAP_NULL_KEY_LITERAL));
            options.add("timestampFormat", TISFlinkProp.create(JsonFormatOptions.TIMESTAMP_FORMAT));
            options.add("encodeDecimalAsPlanNumber", TISFlinkProp.create(JsonFormatOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER));
            options.add("encodeJsonParserEnabled", TISFlinkProp.create(JsonFormatOptions.DECODE_JSON_PARSER_ENABLED));
        }

        @Override
        public final String getDisplayName() {
            return org.apache.flink.formats.json.JsonFormatFactory.IDENTIFIER;
        }
    }
}
