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
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.Bucket;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.aliyun.AliyunEndpoint;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.PluginFieldValidators;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXOssReader extends DataxReader {

    private static final Logger logger = LoggerFactory.getLogger(DataXOssReader.class);

    private static final String DATAX_NAME = "OSS";
    public static final Pattern PATTERN_OSS_OBJECT_NAME = Pattern.compile("([\\w\\d]+/)*([\\w\\d]+|(\\*)){1}");
    public static final Pattern pattern_oss_bucket = Pattern.compile("[a-zA-Z]{1}[\\da-zA-Z_\\-]+");

    public static final String FIELD_ENDPOINT = "endpoint";
    public static final String FIELD_BUCKET = "bucket";


    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String endpoint;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String bucket;
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String object;
    @FormField(ordinal = 5, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String column;
    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String fieldDelimiter;
    @FormField(ordinal = 7, type = FormFieldType.ENUM, validate = {})
    public String compress;
    @FormField(ordinal = 8, type = FormFieldType.ENUM, validate = {})
    public String encoding;
    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {})
    public String nullFormat;
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {})
    public Boolean skipHeader;
    @FormField(ordinal = 11, type = FormFieldType.TEXTAREA, validate = {})
    public String csvReaderConfig;

    @FormField(ordinal = 12, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXOssReader.class, "DataXOssReader-tpl.json");
    }


    @Override
    public Iterator<IDataxReaderContext> getSubTasks() {
        IDataxReaderContext readerContext = new OSSReaderContext(this);
        return Collections.singleton(readerContext).iterator();
    }

    @Override
    public String getTemplate() {
        return template;
    }

    @Override
    public boolean hasMulitTable() {
        return false;
    }

    @Override
    public List<ParseColsResult.DataXReaderTabMeta> getSelectedTabs() {
        DefaultContext context = new DefaultContext();
        ParseColsResult parseOSSColsResult = ParseColsResult.parseColsCfg(new MockFieldErrorHandler(), context, StringUtils.EMPTY, this.column);
        if (!parseOSSColsResult.success) {
            throw new IllegalStateException("parseOSSColsResult must be success");
        }
        return Collections.singletonList(parseOSSColsResult.tabMeta);

    }


    @Override
    public List<String> getTablesInDB() {
        throw new UnsupportedOperationException();
    }

    public IAliyunToken getOSSConfig() {
        return IAliyunToken.getToken(this.endpoint);
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor {
        public DefaultDescriptor() {
            super();
            registerSelectOptions(FIELD_ENDPOINT, () -> ParamsConfig.getItems(IAliyunToken.class));
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        public boolean validateFieldDelimiter(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateFileDelimiter(msgHandler, context, fieldName, value);
        }

        public boolean validateCsvReaderConfig(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return PluginFieldValidators.validateCsvReaderConfig(msgHandler, context, fieldName, value);
        }

        public boolean validateColumn(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            return ParseColsResult.parseColsCfg(msgHandler, context, fieldName, value).success;
        }

        public boolean validateBucket(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateOSSBucket(msgHandler, context, fieldName, value);
        }

        public boolean validateObject(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateOSSObject(msgHandler, context, fieldName, value);
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            //  return super.validate(msgHandler, context, postFormVals);
            return verifyFormOSSRelative(msgHandler, context, postFormVals);
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }


    public static boolean verifyFormOSSRelative(IControlMsgHandler msgHandler, Context context, Descriptor.PostFormVals postFormVals) {
        String endpoint = postFormVals.getField(FIELD_ENDPOINT);
        String bucket = postFormVals.getField(FIELD_BUCKET);
        AliyunEndpoint end = ParamsConfig.getItem(endpoint, AliyunEndpoint.class);

        try {
            OSS ossClient = new OSSClientBuilder().build(end.getEndpoint(), end.getAccessKeyId(), end.getAccessKeySecret());
            List<Bucket> buckets = ossClient.listBuckets();
            if (buckets.size() < 1) {
                msgHandler.addErrorMessage(context, "buckets不能为空");
                return false;
            }
            Optional<Bucket> bucketFind = buckets.stream().filter((b) -> StringUtils.equals(bucket, b.getName())).findFirst();
            if (!bucketFind.isPresent()) {
                //  msgHandler.addErrorMessage(context, );
                msgHandler.addFieldError(context, FIELD_BUCKET, "还未创建bucket:" + bucket);
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public static boolean validateFileDelimiter(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
        value = StringEscapeUtils.unescapeJava(value);
        if (value.length() > 1) {
            logger.error(fieldName + " value:{}", value);
            msgHandler.addFieldError(context, fieldName, "分割符必须为char类型");
            return false;
        }
        return true;
    }

    public static boolean validateOSSBucket(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
        Matcher matcher = pattern_oss_bucket.matcher(value);
        if (!matcher.matches()) {
            msgHandler.addFieldError(context, fieldName, "必须符合格式：" + pattern_oss_bucket.toString());
            return false;
        }
        return true;
    }

    public static boolean validateOSSObject(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
        Matcher m = PATTERN_OSS_OBJECT_NAME.matcher(value);
        if (!m.matches()) {
            msgHandler.addFieldError(context, fieldName, "必须符合格式：" + PATTERN_OSS_OBJECT_NAME.toString());
            return false;
        }
        return true;
    }
}
