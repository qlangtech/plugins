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

package com.qlangtech.tis.plugin.datax.format.guesstype;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.google.common.collect.Maps;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader.StructuredRecord;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.ds.JDBCTypes.VARCHAR;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-13 10:41
 **/
public class GuessOn extends GuessFieldType {
    private static final Logger logger = LoggerFactory.getLogger(GuessOn.class);
    @FormField(ordinal = 13, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxInspectLine;


    @Override
    public Map<String, Map<String, DataType>> processStructGuess(
            IGuessColTypeFormatConfig textFormat, StructuredReader<StructuredRecord> reader) throws IOException {
        Map<String, Map<String, PriorityDataType>> result = Maps.newHashMap();
        Map<String, PriorityDataType> priorityResult = null;
        //  priorityResult = Maps.newHashMap();
        int lineIndex = 0;
        StructuredRecord row = null;
//        Map<String, Object> rowVals;
//        PriorityDataType guessType = null;
        String tabName = null;
        while (reader.hasNext() && lineIndex++ < maxInspectLine) {
            row = reader.next();
            if (row == null) {
                continue;
            }
            tabName = row.tabName;// StringUtils.defaultString(, DEFAUTL_TABLE_NAME);
            if (StringUtils.isEmpty(tabName)) {
                throw new IllegalStateException("tableName can not be empty");
            }
            if ((priorityResult = result.get(tabName)) == null) {
                priorityResult = Maps.newHashMap();
                result.put(tabName, priorityResult);
            }
            parseStructedRecordColType(textFormat, row, priorityResult);
        }
        if (lineIndex < 1 || MapUtils.isEmpty(result)) {
            throw TisException.create("has not find any record, can not invoke guess field types process,priorityResult.size:"
                    + result.size() + ",lineIndex:" + lineIndex);
        }
        return result.entrySet().stream().collect(Collectors.toMap((e) -> e.getKey()
                , (e) -> {
                    return e.getValue().entrySet().stream().collect(Collectors.toMap((col) -> col.getKey(), (col) -> {
                        DataType type = null;
                        if ((type = col.getValue().type) != null) {
                            return type;
                        } else {
                            return defaultDataTypeForNullVal();
                        }
                    }));
                }));
    }

    private void parseStructedRecordColType(
            IGuessColTypeFormatConfig textFormat, StructuredRecord row, Map<String, PriorityDataType> priorityResult) {
        Map<String, Object> rowVals;
        PriorityDataType guessType;
        if (MapUtils.isEmpty(rowVals = row.vals)) {
            return;
        }
        for (Map.Entry<String, Object> entry : rowVals.entrySet()) {
            guessType = GuessType.JSON_LIKE.guess.apply(textFormat, entry.getValue());
            if (guessType != null) {
                PriorityDataType type = null;
                if ((type = priorityResult.get(entry.getKey())) == null) {
                    priorityResult.put(entry.getKey(), guessType);
                } else {
                    if (guessType.isHigherThan(type)) {
                        priorityResult.put(entry.getKey(), guessType);
                    }
                }
            } else {
                if (priorityResult.get(entry.getKey()) == null) {
                    // 添加一个占位
                    priorityResult.put(entry.getKey(), new PriorityDataType(null, Integer.MIN_VALUE));
                }
            }
        }
    }

    @Override
    public void processUnStructGuess(DataType[] types, IGuessColTypeFormatConfig textFormat, UnstructuredReader reader) throws IOException {

        PriorityDataType[] result = new PriorityDataType[types.length];
        int colCount = types.length;
        String[] row = null;
        PriorityDataType guessType = null;
        // final int maxLineReview = 100;
        int lineIndex = 0;
        while (reader.hasNext() && lineIndex++ < maxInspectLine) {
            row = reader.next();
            for (int i = 0; i < colCount; i++) {
                // 猜测类型
                guessType = GuessType.PlAIN_TEXT.guess.apply(textFormat, row[i]);// guessType(textFormat, row[i]);
                if (guessType != null) {
                    if (result[i] == null) {
                        result[i] = guessType;
                    } else {
                        PriorityDataType type = result[i];
                        if (guessType.isHigherThan(type)) {
                            result[i] = guessType;
                        }
                    }
                }
            }
        }

        for (int i = 0; i < types.length; i++) {
            if (result[i] != null) {
                types[i] = result[i].type;
            }
        }

        super.processUnStructGuess(types, textFormat, reader);
    }

    private static class PriorityDataType {
        final DataType type;
        final Integer priority;

        public PriorityDataType(DataType type, Integer priority) {
            this.type = type;
            this.priority = priority;
        }

        public boolean isHigherThan(PriorityDataType guessType) {
            if (this.priority > guessType.priority) {
                return true;
            }

            // 针对String类型 如果碰到更长的字符串长度 将 字符串长度变成
            if ((this.type.getJdbcType() == VARCHAR) && type.getColumnSize() > guessType.type.getColumnSize()) {
                return true;
            }
            return false;
        }
    }

    public enum GuessType {

        JSON_LIKE((textFormat, colVal) -> {

            if (colVal == null) {
                return null;
            }

            /**
             *
             Boolean. TYPE, Character. TYPE, Byte. TYPE, Short. TYPE, Integer. TYPE, Long. TYPE, Float. TYPE, Double. TYPE, Void. TYPE
             */

            Class<?> clazz = colVal.getClass();
            // if (clazz.isPrimitive()) {
            if (clazz == Boolean.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.BOOLEAN).getType(), -2);
            } else if (clazz == Character.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.CHAR).getType(), 7);
            } else if (clazz == Byte.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.BINARY).getType(), 0);
            } else if (clazz == Short.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.SMALLINT).getType(), 0);
            } else if (clazz == Integer.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.INTEGER).getType(), 1);
            } else if (clazz == Long.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.BIGINT).getType(), 2);
            } else if (clazz == Float.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.FLOAT).getType(), 3);
            } else if (clazz == Double.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.DOUBLE).getType(), 4);
            } else if (clazz == BigInteger.class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.BIGINT).getType(), 2);
            } else if (clazz == BigDecimal.class) {
                final BigDecimal decimal = (BigDecimal) colVal;
                try {
                    DataType decimalType = DataTypeMeta.getDataTypeMeta(JDBCTypes.DECIMAL).getType();
                    return new PriorityDataType(decimalType.clone().setDecimalDigits(decimal.scale()), 2);
                } catch (CloneNotSupportedException e) {
                    throw new RuntimeException("decimalVal:" + decimal.toPlainString(), e);
                }
            }
//            else {
//                throw new UnsupportedOperationException("unsupported type:" + clazz + ",val:" + colVal);
//            }
            //}

            if (clazz == byte[].class) {
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.BLOB).getType(), 4);
            }

            if (colVal instanceof String) {
                String strColVal = (String) colVal;
                if (StringUtils.isBlank(strColVal)) {
                    return null;
                }
                try {
                    if (textFormat.isDateFormat(strColVal)) {
                        return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.DATE).getType(), 5);
                    }
                } catch (Exception e) {

                }

                try {
                    if (textFormat.isTimeStampFormat(strColVal)) {
                        return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.TIMESTAMP).getType(), 6);
                    }
                } catch (Exception e) {

                }

                return new PriorityDataType(DataType.createVarChar(org.apache.commons.lang3.StringUtils.length(strColVal) * 2), 7);
            }
            logger.warn("unsupported type:" + clazz + ",val:" + colVal);
            // throw new UnsupportedOperationException("unsupported type:" + clazz + ",val:" + colVal);
            return null;
        }) //
        , PlAIN_TEXT((textFormat, val) -> {
            String colVal = (String) val;
            if (org.apache.commons.lang3.StringUtils.isEmpty(colVal)
                    || org.apache.commons.lang3.StringUtils.equals(textFormat.getNullFormat(), colVal)) {
                return null;
            }
            try {
                Integer.parseInt(colVal);
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.INTEGER).getType(), 1);
            } catch (Exception e) {

            }

            try {
                Long.parseLong(colVal);
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.BIGINT).getType(), 2);
            } catch (Exception e) {

            }

            try {
                Float.parseFloat(colVal);
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.FLOAT).getType(), 3);
            } catch (Exception e) {

            }

            try {
                Double.parseDouble(colVal);
                return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.DOUBLE).getType(), 4);
            } catch (Exception e) {

            }

            try {
                if (textFormat.isDateFormat(colVal)) {
                    return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.DATE).getType(), 5);
                }
            } catch (Exception e) {

            }

            try {
                if (textFormat.isTimeStampFormat(colVal)) {
                    return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.TIMESTAMP).getType(), 6);
                }
            } catch (Exception e) {

            }

            return new PriorityDataType(DataType.createVarChar(org.apache.commons.lang3.StringUtils.length(colVal) * 2), 7);
        });
        private BiFunction<IGuessColTypeFormatConfig, Object, PriorityDataType> guess;

        private GuessType(BiFunction<IGuessColTypeFormatConfig, Object, PriorityDataType> guess) {
            this.guess = guess;
        }
    }

    /**
     * 通过文本内容猜测文本类型
     *
     * @param colVal
     * @return
     */
    private PriorityDataType guessType(IGuessColTypeFormatConfig textFormat, String colVal) {
        if (org.apache.commons.lang3.StringUtils.isEmpty(colVal) || org.apache.commons.lang3.StringUtils.equals(textFormat.getNullFormat(), colVal)) {
            return null;
        }
        try {
            Integer.parseInt(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.INTEGER).getType(), 1);
        } catch (Exception e) {

        }

        try {
            Long.parseLong(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.BIGINT).getType(), 2);
        } catch (Exception e) {

        }

        try {
            Float.parseFloat(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.FLOAT).getType(), 3);
        } catch (Exception e) {

        }

        try {
            Double.parseDouble(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.DOUBLE).getType(), 4);
        } catch (Exception e) {

        }

        try {
            textFormat.isDateFormat(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.DATE).getType(), 5);
        } catch (Exception e) {

        }

        try {
            textFormat.isTimeStampFormat(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(JDBCTypes.TIMESTAMP).getType(), 6);
        } catch (Exception e) {

        }

        return new PriorityDataType(DataType.createVarChar(org.apache.commons.lang3.StringUtils.length(colVal) * 2), 7);
    }

    @TISExtension
    public static class DftDesc extends Descriptor<GuessFieldType> {

        public boolean validateMaxInspectLine(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            int maxInspectLine = Integer.parseInt(value);
            final int min = 500;
            if (maxInspectLine < min) {
                msgHandler.addFieldError(context, fieldName, "不能小于" + min);
                return false;
            }
            final int max = 50000;
            if (maxInspectLine > max) {
                msgHandler.addFieldError(context, fieldName, "不能大于" + max);
                return false;
            }


            return true;
        }

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
