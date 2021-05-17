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
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * https://github.com/alibaba/DataX/blob/master/osswriter/doc/osswriter.md
 *
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXOssWriter extends DataxWriter {
    private static final Logger logger = LoggerFactory.getLogger(DataXOssWriter.class);
    private static final String DATAX_NAME = "OSS";
    public static final Pattern PATTERN_OSS_WRITER_OBJECT_NAME = Pattern.compile("([\\w\\d]+/)*([\\w\\d]+)");

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String endpoint;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String bucket;
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String object;
    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {Validator.require})
    public String writeMode;
    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
    public String fieldDelimiter;
    @FormField(ordinal = 7, type = FormFieldType.ENUM, validate = {})
    public String encoding;
    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
    public String nullFormat;
    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {})
    public String dateFormat;
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {})
    public String fileFormat;
    @FormField(ordinal = 11, type = FormFieldType.TEXTAREA, validate = {})
    public String header; // json格式
    @FormField(ordinal = 12, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer maxFileSize;

    @FormField(ordinal = 13, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXOssWriter.class, "DataXOssWriter-tpl.json");
    }


    @Override
    public String getTemplate() {
        return this.template;
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        OSSWriterContext writerContext = new OSSWriterContext(this);
        return writerContext;
    }

    public IAliyunToken getOSSConfig() {
        return DataXOssReader.getiAliyunToken(this.endpoint);
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {


        public DefaultDescriptor() {
            super();
            registerSelectOptions(DataXOssReader.FIELD_ENDPOINT, () -> ParamsConfig.getItems(IAliyunToken.class));
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        public boolean validateObject(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            Matcher matcher = PATTERN_OSS_WRITER_OBJECT_NAME.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "必须符合格式：" + PATTERN_OSS_WRITER_OBJECT_NAME.toString());
                return false;
            }
            return true;
            // return DataXOssReader.validateOSSObject(msgHandler, context, fieldName, value);
        }

        @Override
        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return DataXOssReader.verifyFormOSSRelative(msgHandler, context, postFormVals);
        }

        public boolean validateDateFormat(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                return true;
            }
            try {
                SimpleDateFormat format = new SimpleDateFormat(value);
                format.format(new Date());
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, "确认format格式是否正确");
                return false;
            }
            return true;
        }

        public boolean validateHeader(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            if (StringUtils.isEmpty(value)) {
                return true;
            }

            try {
                JSONArray headCols = JSON.parseArray(value);
                if (headCols.size() < 1) {
                    msgHandler.addFieldError(context, fieldName, "请填写Head中可能出现的列枚举");
                    return false;
                }
                Object col = null;
                for (int i = 0; i < headCols.size(); i++) {
                    col = headCols.get(i);
                    if (!(col instanceof String)) {
                        msgHandler.addFieldError(context, fieldName, "Json数组下标为：" + i + "的元素类型必须为String");
                        return false;
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, "请确认Json格式是否正确，Err:" + e.getMessage());
                return false;
            }

            //return DataXOssReader.validateFileDelimiter(msgHandler, context, fieldName, value);
            return true;
        }

        public boolean validateFieldDelimiter(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return DataXOssReader.validateFileDelimiter(msgHandler, context, fieldName, value);
        }

        public boolean validateBucket(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return DataXOssReader.validateOSSBucket(msgHandler, context, fieldName, value);
        }


        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
