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

package com.qlangtech.tis.plugin.datax.hudi.keygenerator.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.plugin.writer.hudi.IPropertiesBuilder;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import com.qlangtech.tis.org.apache.hudi.keygen.constant.KeyGeneratorType;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.hudi.keygenerator.HudiKeyGenerator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ref:KeyGeneratorType
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-04-25 07:31
 * see org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator
 **/
@Public
public class HudiTimestampBasedKeyGenerator extends HudiKeyGenerator {

    private static final Logger logger = LoggerFactory.getLogger(HudiTimestampBasedKeyGenerator.class);

    public static final String KEY_TIMESTAMP_TYPE = "timestampType";
    public static final String KEY_INPUT_DATE_FORMAT = "inputDateformat";
    public static final String KEY_OUTPUT_DATE_FORMAT = "outputDateformat";

    public KeyGeneratorType getKeyGeneratorType() {
        return KeyGeneratorType.TIMESTAMP;
    }

    @Override
    public List<String> getRecordFields() {
        return Collections.singletonList(this.recordField);
    }

    @Override
    public List<String> getPartitionPathFields() {
        return Collections.singletonList(this.partitionPathField);
    }

    //    // One value from TimestampType above
//    public static final String TIMESTAMP_TYPE_FIELD_PROP = "hoodie.deltastreamer.keygen.timebased.timestamp.type";
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String timestampType;
//    public static final String INPUT_TIME_UNIT =
//            "hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit";
//    //This prop can now accept list of input date formats.
//    public static final String TIMESTAMP_INPUT_DATE_FORMAT_PROP =
//            "hoodie.deltastreamer.keygen.timebased.input.dateformat";

    // format 可以填写多个，并且用逗号分割
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {})
    public String inputDateformat;

    //    public static final String TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP = "hoodie.deltastreamer.keygen.timebased.input.dateformat.list.delimiter.regex";
//    public static final String TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.input.timezone";
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String inputTimezone;

    //    public static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP =
//            "hoodie.deltastreamer.keygen.timebased.output.dateformat";
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String outputDateformat;
    //    //still keeping this prop for backward compatibility so that functionality for existing users does not break.
//    public static final String TIMESTAMP_TIMEZONE_FORMAT_PROP =
//            "hoodie.deltastreamer.keygen.timebased.timezone";
    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String timezone;
//    public static final String TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.output.timezone";

    //    static final String DATE_TIME_PARSER_PROP = "hoodie.deltastreamer.keygen.datetime.parser.class";
    @Override
    public void setProps(IPropertiesBuilder props) {
        props.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP, this.timestampType);
        props.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, this.inputDateformat);
        props.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, this.outputDateformat);
        props.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, this.timezone);
    }

    public static void addFieldDesc(Descriptor desc) {
        List<Option> timestampTypes
                = Arrays.stream(TimestampBasedAvroKeyGenerator.TimestampType.values()).map((e) -> new Option(e.name())).collect(Collectors.toList());
        desc.addFieldDescriptor(KEY_TIMESTAMP_TYPE
                , TimestampBasedAvroKeyGenerator.TimestampType.EPOCHMILLISECONDS.name(), "时间字段类型", Optional.of(timestampTypes));

        List<Option> timeZones = Arrays.stream(TimeZone.getAvailableIDs()).map((zid) -> new Option(zid)).collect(Collectors.toList());
        desc.addFieldDescriptor("timezone", TimeZone.getDefault().getID(), "格式化时间时区", Optional.of(timeZones));
    }

//    @TISExtension
//    public static class DefaultDescriptor extends Descriptor<HudiKeyGenerator> {
//        public DefaultDescriptor() {
//            super();
//            addFieldDesc(this);
//        }
//
//        @Override
//        public PluginFormProperties getPluginFormPropertyTypes(Optional<IPropertyType.SubFormFilter> subFormFilter) {
//            return super.getPluginFormPropertyTypes(Optional.empty());
//        }
//
//
//        @Override
//        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            ParseDescribable<Describable> i = this.newInstance((IPluginContext) msgHandler, postFormVals.rawFormData, Optional.empty());
//            HudiTimestampBasedKeyGenerator keyGenerator = i.getInstance();
//
//            return validateForm(msgHandler, context, keyGenerator);
//        }
//
//
//        @Override
//        public String getDisplayName() {
//            return KeyGeneratorType.TIMESTAMP.name();
//        }
//    }

    public static boolean validateForm(IControlMsgHandler msgHandler, Context context, HudiTimestampBasedKeyGenerator keyGenerator) {
        TimestampBasedAvroKeyGenerator.TimestampType timestampType
                = TimestampBasedAvroKeyGenerator.TimestampType.valueOf(keyGenerator.timestampType);
        if (timestampType == TimestampBasedAvroKeyGenerator.TimestampType.DATE_STRING) {
            if (StringUtils.isEmpty(keyGenerator.inputDateformat)) {
                msgHandler.addFieldError(context, KEY_INPUT_DATE_FORMAT, ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
                return false;
            }
        }

        if (StringUtils.isNotEmpty(keyGenerator.inputDateformat)) {
            try {
                DateTimeFormatter.ofPattern(keyGenerator.inputDateformat);
            } catch (Throwable e) {
                logger.warn("field:" + KEY_INPUT_DATE_FORMAT, e);
                msgHandler.addFieldError(context, KEY_INPUT_DATE_FORMAT, "日期格式有误");
                return false;
            }
        }

        if (StringUtils.isNotEmpty(keyGenerator.outputDateformat)) {
            try {
                DateTimeFormatter.ofPattern(keyGenerator.outputDateformat);
            } catch (Throwable e) {
                logger.warn("field:" + KEY_OUTPUT_DATE_FORMAT, e);
                msgHandler.addFieldError(context, KEY_OUTPUT_DATE_FORMAT, "日期格式有误");
                return false;
            }
        }


        return true;
    }
}
