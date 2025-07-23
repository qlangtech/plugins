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

package com.qlangtech.tis.plugin.datax.format;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.plugin.unstructuredstorage.Compress;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore.AfterPluginSaved;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.guesstype.GuessFieldType;
import com.qlangtech.tis.plugin.datax.format.guesstype.IGuessColTypeFormatConfig;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 13:49
 **/
public abstract class BasicPainFormat extends FileFormat implements IGuessColTypeFormatConfig, AfterPluginSaved {
    private static final Logger logger = LoggerFactory.getLogger(BasicPainFormat.class);
    @FormField(ordinal = 13, type = FormFieldType.INPUTTEXT, advance = true, validate = {Validator.require})
    public String dateFormat;

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String nullFormat;

    public static String defaultNullFormat() {
        return TimeFormat.DATA_FORMAT;
    }

    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {Validator.require})
    public String fieldDelimiter;

    @FormField(ordinal = 16, type = FormFieldType.ENUM, validate = {Validator.require})
    public boolean header;


    @FormField(ordinal = 10, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public String compress;

    @FormField(ordinal = 11, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public String encoding;

    @Override
    public final Function<String, Column> buildColValCreator(CMeta cmeta) {

        Function<String, Column> colValCreator = cmeta.getType().accept(new DataType.PartialTypeVisitor<Function<String, Column>>() {
            @Override
            public Function<String, Column> bigInt(DataType type) {
                return (val) -> new LongColumn(val);
            }

            @Override
            public Function<String, Column> doubleType(DataType type) {
                return (val) -> new DoubleColumn(val);
            }

            @Override
            public Function<String, Column> dateType(DataType type) {
                final SimpleDateFormat dateFormat = getDateFormat();
                return (val) -> {
                    try {
                        if (val != null) {
                            return new DateColumn(dateFormat.parse(val));
                        } else {
                            return new DateColumn((Date) null);
                        }
                    } catch (ParseException e) {
                        throw new RuntimeException("date val:" + val, e);
                    }
                };
            }

            @Override
            public Function<String, Column> timestampType(DataType type) {
                final SimpleDateFormat dateFormat = getTimeStampFormat();
                return (val) -> {
                    try {
                        if (val != null) {
                            return new DateColumn(dateFormat.parse(val));
                        } else {
                            return new DateColumn((Date) null);
                        }
                    } catch (ParseException e) {
                        throw new RuntimeException("date val:" + val + " by format:" + dateFormat.toPattern(), e);
                    }
                };
            }

            @Override
            public Function<String, Column> bitType(DataType type) {
                return (val) -> {
                    return new LongColumn(val);
                };
            }

            @Override
            public Function<String, Column> blobType(DataType type) {
                return (val) -> {
                    return new BytesColumn((val != null) ? val.getBytes() : null);
                };
            }

            @Override
            public Function<String, Column> varcharType(DataType type) {
                return (val) -> {
                    return new StringColumn(val);
                };
            }
        });

        if (StringUtils.isNotEmpty(this.nullFormat)) {
            return colValCreator.compose((val) -> (StringUtils.isBlank(val) || StringUtils.equals(nullFormat, val)) ? null : (val));
        } else {
            return colValCreator;
        }
    }

    private transient SimpleDateFormat _dateFormat;// = new ThreadLocal<>();

    private transient SimpleDateFormat _timestampFormat;

    @Override
    public boolean isDateFormat(String literiaVal) {

        try {
            getDateFormat().parse(literiaVal);
            return true;
        } catch (ParseException e) {

        }
        return false;
    }

    @Override
    public boolean isTimeStampFormat(String literiaVal) {
        try {
            getTimeStampFormat().parse(literiaVal);
            return true;
        } catch (ParseException e) {
            // throw new RuntimeException(e);
        }
        return false;
    }

    // @Override
    protected final SimpleDateFormat getDateFormat() {
        if (_dateFormat == null) {
            this._dateFormat = parseFormat(this.dateFormat);
        }
        return _dateFormat;
    }

    private final SimpleDateFormat getTimeStampFormat() {
        if (_timestampFormat == null) {
            this._timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        return _timestampFormat;
    }

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this._timestampFormat = null;
        this._dateFormat = null;
    }

    @Override
    public String getNullFormat() {
        return this.nullFormat;
    }

    @Override
    public final boolean containHeader() {
        return this.header;
    }

    public static SimpleDateFormat parseFormat(String dateFormat) {
        return new SimpleDateFormat(dateFormat);
    }

    @Override
    public final char getFieldDelimiter() {
        return (Delimiter.parse(this.fieldDelimiter).val);
    }

    @Override
    public final UnstructuredWriter createWriter(OutputStream output) {
        try {
            BufferedWriter writer = Compress.parse(compress).decorate(output, encoding);
            return this.createWriter(writer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract UnstructuredWriter createWriter(Writer writer);


    @Override
    public Descriptor<FileFormat> getDescriptor() {
        Descriptor<FileFormat> descriptor = super.getDescriptor();
        if (!BasicPainFormatDescriptor.class.isAssignableFrom(descriptor.getClass())) {
            throw new IllegalStateException(descriptor.getClass().getName() + " must extend form " + BasicPainFormatDescriptor.class.getName());
        }
        return descriptor;
    }

    public static List<Option> supportCompress() {
        return Arrays.stream(Compress.values()).map((c) -> new Option(c.name(), c.token)).collect(Collectors.toList());
    }

    public static class BasicPainFormatDescriptor extends Descriptor<FileFormat> {

        public static List<? extends Descriptor> supportedFormat(boolean reader, List<? extends Descriptor> descs) {
            if (CollectionUtils.isEmpty(descs)) {
                return Collections.emptyList();
            }
            return descs.stream().filter((desc) -> {
                return reader ^ ((BasicPainFormatDescriptor) desc).isSupportWriterConnector();
            }).collect(Collectors.toList());
        }

        /**
         * 是否支持writer的fromat，由于reader需要支持guessSupport功能
         *
         * @return
         */
        public boolean isSupportWriterConnector() {
            return true;
        }

        public boolean validateDateFormat(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                SimpleDateFormat dateFormat = BasicPainFormat.parseFormat(value);
                dateFormat.format(new Date());
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, "format格式不正确");
                return false;
            }
            return true;
        }

    }

}
