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
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 13:49
 **/
public abstract class BasicPainFormat extends FileFormat {
    private static final Logger logger = LoggerFactory.getLogger(BasicPainFormat.class);
    @FormField(ordinal = 13, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String dateFormat;

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String nullFormat;

    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {Validator.require})
    public String fieldDelimiter;

    @FormField(ordinal = 16, type = FormFieldType.ENUM, validate = {})
    public boolean header;

    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    public String compress;

    @FormField(ordinal = 11, type = FormFieldType.ENUM, validate = {Validator.require})
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
            return colValCreator.compose((val) -> StringUtils.equals(nullFormat, val) ? null : val);
        } else {
            return colValCreator;
        }
    }

    protected static SimpleDateFormat getTimeStampFormat() {
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
    public final UnstructuredReader createReader(InputStream input) {
        try {
            return createReader(Compress.parse(compress).decorate(input, encoding));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract UnstructuredReader createReader(BufferedReader reader) throws IOException;

    @Override
    public final FileHeader readHeader(InputStream input) throws IOException {
        return readHeader(createReader(input));
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
        String[] row = null;
        DataType guessType = null;
        final int maxLineReview = 100;
        int lineIndex = 0;
        while (textFormat.hasNext() && lineIndex++ < maxLineReview) {
            row = textFormat.next();
            for (int i = 0; i < colCount; i++) {
                // 猜测类型
                guessType = guessType(row[i]);
                if (guessType != null) {
                    if (types[i] == null) {
                        types[i] = guessType;
                    } else {
                        DataType type = types[i];
                        // 针对String类型 如果碰到更长的字符串长度 将 字符串长度变成
                        if ((type.type == Types.VARCHAR) && guessType.getColumnSize() > type.getColumnSize()) {
                            types[i] = guessType;
                        }
                    }
                }
            }
            // 判断是否已经全部类型已经判断出了
            if (isAllTypeJudged(types, Optional.empty())) {
                break;
            }
        }

        // 最后将空缺的类型补充上
        isAllTypeJudged(types, Optional.of(DataType.createVarChar(32)));

        return new FileHeader(colCount, header == null ? null : Lists.newArrayList(header), Lists.newArrayList(types));
    }

    private boolean isAllTypeJudged(DataType[] types, Optional<DataType> dftType) {
        for (int i = 0; i < types.length; i++) {
            if (types[i] == null) {
                if (!dftType.isPresent()) {
                    return false;
                } else {
                    types[i] = dftType.get();
                }
            }
        }
        return true;
    }

    /**
     * 通过文本内容猜测文本类型
     *
     * @param colVal
     * @return
     */
    private DataType guessType(String colVal) {
        if (org.apache.commons.lang3.StringUtils.isEmpty(colVal) || org.apache.commons.lang3.StringUtils.equals(nullFormat, colVal)) {
            return null;
        }
        try {
            Integer.parseInt(colVal);
            return DataTypeMeta.getDataTypeMeta(Types.INTEGER).getType();
        } catch (Exception e) {

        }

        try {
            Long.parseLong(colVal);
            return DataTypeMeta.getDataTypeMeta(Types.BIGINT).getType();
        } catch (Exception e) {

        }

        try {
            Float.parseFloat(colVal);
            return DataTypeMeta.getDataTypeMeta(Types.FLOAT).getType();
        } catch (Exception e) {

        }

        try {
            Double.parseDouble(colVal);
            return DataTypeMeta.getDataTypeMeta(Types.DOUBLE).getType();
        } catch (Exception e) {

        }

        try {
            this.getDateFormat().parse(colVal);
            return DataTypeMeta.getDataTypeMeta(Types.DATE).getType();
        } catch (Exception e) {

        }

        try {
            getTimeStampFormat().parse(colVal);
            return DataTypeMeta.getDataTypeMeta(Types.TIMESTAMP).getType();
        } catch (Exception e) {

        }

        return DataType.createVarChar(org.apache.commons.lang3.StringUtils.length(colVal) * 2);
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
