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
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.format.BasicPainFormat;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.io.IOException;
import java.sql.Types;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-13 10:41
 **/
public class GuessOn extends GuessFieldType {

    @FormField(ordinal = 13, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxInspectLine;

    @Override
    public void processGuess(DataType[] types, BasicPainFormat textFormat, UnstructuredReader reader) throws IOException {

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
                guessType = guessType(textFormat, row[i]);
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
            // 判断是否已经全部类型已经判断出了
//            if (isAllTypeJudged(types, Optional.empty())) {
//                break;
//            }
        }

        for (int i = 0; i < types.length; i++) {
            if (result[i] != null) {
                types[i] = result[i].type;
            }
        }

        super.processGuess(types, textFormat, reader);
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
            if ((this.type.type == Types.VARCHAR) && type.getColumnSize() > guessType.type.getColumnSize()) {
                return true;
            }
            return false;
        }
    }

    /**
     * 通过文本内容猜测文本类型
     *
     * @param colVal
     * @return
     */
    private PriorityDataType guessType(BasicPainFormat textFormat, String colVal) {
        if (org.apache.commons.lang3.StringUtils.isEmpty(colVal) || org.apache.commons.lang3.StringUtils.equals(textFormat.nullFormat, colVal)) {
            return null;
        }
        try {
            Integer.parseInt(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(Types.INTEGER).getType(), 1);
        } catch (Exception e) {

        }

        try {
            Long.parseLong(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(Types.BIGINT).getType(), 2);
        } catch (Exception e) {

        }

        try {
            Float.parseFloat(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(Types.FLOAT).getType(), 3);
        } catch (Exception e) {

        }

        try {
            Double.parseDouble(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(Types.DOUBLE).getType(), 4);
        } catch (Exception e) {

        }

        try {
            textFormat.getDateFormat().parse(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(Types.DATE).getType(), 5);
        } catch (Exception e) {

        }

        try {
            BasicPainFormat.getTimeStampFormat().parse(colVal);
            return new PriorityDataType(DataTypeMeta.getDataTypeMeta(Types.TIMESTAMP).getType(), 6);
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
