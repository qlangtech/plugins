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
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.plugin.unstructuredstorage.Compress;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.guesstype.GuessFieldType;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 13:49
 **/
public abstract class BasicPainFormat extends FileFormat {
    private static final Logger logger = LoggerFactory.getLogger(BasicPainFormat.class);
    @FormField(ordinal = 13, type = FormFieldType.INPUTTEXT, advance = true, validate = {Validator.require})
    public String dateFormat;

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String nullFormat;

    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {Validator.require})
    public String fieldDelimiter;

    @FormField(ordinal = 16, type = FormFieldType.ENUM, validate = {Validator.require})
    public boolean header;

    @FormField(ordinal = 17, validate = {Validator.require})
    public GuessFieldType guessFieldType;


    @FormField(ordinal = 10, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public String compress;

    @FormField(ordinal = 11, type = FormFieldType.ENUM, advance = true, validate = {Validator.require})
    public String encoding;

    @Override
    public final Function<String, Column> buildColValCreator(CMeta cmeta) {

        Function<String, Column> colValCreator = cmeta.getType().accept(new DataType.TypeVisitor<Function<String, Column>>() {
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

    public static SimpleDateFormat getTimeStampFormat() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public final boolean containHeader() {
        return this.header;
    }

    public final SimpleDateFormat getDateFormat() {
        return parseFormat(this.dateFormat);
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
    public UnstructuredReader createReader(InputStream input, List<CMeta> sourceCols) {
        try {
            return createReader(Compress.parse(compress).decorate(input, encoding), sourceCols);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract UnstructuredReader createReader(BufferedReader reader, List<CMeta> sourceCols) throws IOException;

    @Override
    public final FileHeader readHeader(InputStream input) throws IOException {
        return readHeader(createReader(input, Collections.emptyList()));
    }

    // protected abstract FileHeader readHeader(UnstructuredReader reader) throws IOException;

    @Override
    public Descriptor<FileFormat> getDescriptor() {
        Descriptor<FileFormat> descriptor = super.getDescriptor();
        if (!BasicPainFormatDescriptor.class.isAssignableFrom(descriptor.getClass())) {
            throw new IllegalStateException(descriptor.getClass().getName() + " must extend form " + BasicPainFormatDescriptor.class.getName());
        }
        return descriptor;
    }

    // @Override
    protected final FileHeader readHeader(UnstructuredReader reader) throws IOException {
        UnstructuredReader textFormat = reader;
        String[] header = textFormat.getHeader();
        int colCount;
        if (header == null) {
            if (textFormat.hasNext()) {
                colCount = textFormat.next().length;
            } else {
                throw new IllegalStateException("can not read content from textFormat");
            }
        } else {
            colCount = header.length;
        }

        // guess all col types
        DataType[] types = new DataType[colCount];
        Objects.requireNonNull(this.guessFieldType, "guessFieldType can not be null").processGuess(types, this, textFormat);
        return new FileHeader(colCount, header == null ? null : Lists.newArrayList(header), Lists.newArrayList(types));
    }

    public static List<Option> supportCompress() {
        return Arrays.stream(Compress.values()).map((c) -> new Option(c.name(), c.token)).collect(Collectors.toList());
    }

    protected static class BasicPainFormatDescriptor extends Descriptor<FileFormat> {


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
