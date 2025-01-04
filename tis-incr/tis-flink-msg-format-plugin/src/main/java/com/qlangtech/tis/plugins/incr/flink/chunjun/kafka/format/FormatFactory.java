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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.Options;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.TISFlinkProp;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.format.guesstype.IGuessColTypeFormatConfig;
import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader;
import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader.StructuredRecord;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;
import org.apache.flink.formats.json.canal.CanalJsonFormatOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.qlangtech.tis.plugin.annotation.Validator.db_col_name;
import static com.qlangtech.tis.plugin.annotation.Validator.require;
import static org.apache.flink.formats.common.TimeFormats.ISO8601_TIMESTAMP_FORMAT;

/**
 * 内容传输格式
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/formats/overview/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-04-15 12:24
 * @see com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.canaljson.TISCanalJsonFormatFactory
 * @see com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.debeziumjson.TISSinkDebeziumJsonFormatFactory
 * @see com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.json.SourceJsonFormatFactory
 **/
public abstract class FormatFactory implements Describable<FormatFactory>, IGuessColTypeFormatConfig, Serializable {

    public static final String KEY_FIELD_FORMAT = "format";
    public static final String KEY_FIELD_TAB_ENTITIES = "tabEntities";
    /**
     * 可以不填写，这样就采用自动分析的方式
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {})
    public String tabEntities;

    private static final ThreadLocal<SimpleDateFormat> dataFormatLocal = new ThreadLocal<>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    @Override
    public boolean isDateFormat(String literiaVal) {
        try {
            dataFormatLocal.get().parse(literiaVal);
            return true;
        } catch (ParseException e) {
        }
        return false;
    }

    protected abstract String getTimestampFormat();

    /**
     * @param literiaVal
     * @return
     * @see org.apache.flink.formats.json.JsonParserToRowDataConverters# convertToTimestamp(JsonParser)
     */
    @Override
    public final boolean isTimeStampFormat(String literiaVal) {
        final String timestampFormat = getTimestampFormat();
        org.apache.flink.formats.common.TimestampFormat timestampOption
                = JsonFormatOptionsUtil.getTimestampFormat(Configuration.fromMap(Collections.singletonMap(JsonFormatOptions.TIMESTAMP_FORMAT.key(), timestampFormat)));

        try {
            switch (timestampOption) {
                case SQL:
                    org.apache.flink.formats.common.TimeFormats.SQL_TIMESTAMP_FORMAT.parse(literiaVal);
                    return true;
                case ISO_8601:
                    ISO8601_TIMESTAMP_FORMAT.parse(literiaVal);
                    return true;
                default:
                    throw new IllegalStateException(
                            String.format(
                                    "Unsupported timestamp format '%s'. Validator should have checked that.",
                                    timestampFormat));
            }
        } catch (Throwable e) {
            // throw new RuntimeException(e);
        }

        return false;
    }

    /**
     * 预测kafka消息流中的一条消息解析出数据结构
     *
     * @param record
     * @return
     */
    public abstract KafkaStructuredRecord parseRecord(KafkaStructuredRecord reuse, byte[] record);

    /**
     * 校验format属性
     *
     * @param dataxReader
     * @return
     */
    public abstract boolean validateFormtField(
            IControlMsgHandler msgHandler, Context context, String fieldName, DataxReader dataxReader);

    /**
     * 是否支持多个表
     *
     * @return
     */
    public abstract boolean acceptMultipleTable();

    /**
     * kafka 反序列化工具
     *
     * @return
     */
    // public abstract DeserializationSchema<DTO> createDecodingFormat();
    //  public abstract DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(final String targetTabName);

    /**
     * @param targetTabName 目标表名称
     * @return
     */
    public abstract EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(final String targetTabName);

    @Override
    public final Descriptor<FormatFactory> getDescriptor() {
        Descriptor<FormatFactory> desc = Describable.super.getDescriptor();
        if (!BasicFormatDescriptor.class.isAssignableFrom(desc.getClass())) {
            throw new IllegalStateException("class:" + desc.getClass()
                    + " must extend from " + BasicFormatDescriptor.class.getSimpleName());
        }
        return desc;
    }

    /**
     * 通过遍历消息获得目标表
     */
    public List<String> parseTargetTabsEntities() {
        return Lists.newArrayList(splitTabEntities(this.tabEntities));
    }

    protected static String[] splitTabEntities(String value) {
        return StringUtils.split(value, ",");
    }

    public static abstract class BasicFormatDescriptor extends Descriptor<FormatFactory> {
        public FlinkPropAssist.Options options;

        public BasicFormatDescriptor() {
            super();
            this.options = FlinkPropAssist.createOpts(this);
            if (this.getEndType().sinkSupport) {
                // sink端不需要设置targetTable
                this.options.add(KEY_FIELD_TAB_ENTITIES
                        , TISFlinkProp.create(ConfigOptions.key(KEY_FIELD_TAB_ENTITIES).stringType().defaultValue(null)).disable());
            }
            this.appendOptionCfgs(options);
        }

        protected void addNullKeyOptCfg(Options options) {
            OverwriteProps nullKeyMode = new OverwriteProps();
            nullKeyMode.setEnumOpts(Lists.newArrayList(new Option("FAIL"), new Option("DROP"), new Option("LITERAL")));
            options.add("nullKeyMode", TISFlinkProp.create(CanalJsonFormatOptions.JSON_MAP_NULL_KEY_MODE)
                    .setOverwriteProp(nullKeyMode));
            options.add("nullKeyLiteral", TISFlinkProp.create(CanalJsonFormatOptions.JSON_MAP_NULL_KEY_LITERAL));
        }

        public abstract EndType getEndType();

        /**
         * 校验目标表格式
         *
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param value
         * @return
         */
        public boolean validateTabEntities(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            //Set<String> tabs = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            String[] tabs = splitTabEntities(value);
            if (tabs.length < 1) {
                // msgHandler.addFieldError(context, fieldName, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
                return true;
            }

            for (String tab : tabs) {
                if (!require.validate(msgHandler, context, fieldName, tab)) {
                    return false;
                }
                if (!db_col_name.validate(msgHandler, context, fieldName, tab)) {
                    return false;
                }
            }

            return true;
        }

        /**
         * 添加配置项
         *
         * @param options
         */
        protected abstract void appendOptionCfgs(Options options);
    }

    public enum EndType {
        SOURCE(true, false) //
        , SINK(false, true); //
        // , SOURCE_OR_SINK(true, true);

        public final boolean sourceSupport;
        public final boolean sinkSupport;

        EndType(boolean sourceSupport, boolean sinkSupport) {
            this.sourceSupport = sourceSupport;
            this.sinkSupport = sinkSupport;
        }
    }

}
