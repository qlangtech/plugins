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

package com.qlangtech.plugins.incr.flink.cdc;


import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-23 10:37
 * @see org.apache.flink.cdc.runtime.serializer.data.writer.BinaryWriter#write
 **/
public class FlinkCDCPipelineEventProcess extends BiFunction {

    private final org.apache.flink.cdc.common.types.DataType dataType;
    private final BiFunction fieldProcessDelegate;

    public FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataType dataType, BiFunction fieldProcessDelegate) {
        this.dataType = dataType;
        this.fieldProcessDelegate = fieldProcessDelegate;
    }

    @Override
    public Object apply(Object o) {
        return fieldProcessDelegate.apply(o);
    }

    public org.apache.flink.cdc.common.types.DataType getDataType() {
        return this.dataType;
    }


    public static class FlinkPipelineStringConvert extends BiFunction {
        @Override
        public Object deApply(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object apply(Object o) {
            if (o instanceof java.nio.ByteBuffer) {
                return org.apache.flink.cdc.common.data.binary.BinaryStringData.fromBytes(((java.nio.ByteBuffer) o).array());
            }
            return org.apache.flink.cdc.common.data.binary.BinaryStringData.fromString(String.valueOf(o));
        }
    }



    public static class FlinkPipelineDecimalConvert extends BiFunction {
        //  private final DataType type;

        final int precision;// = type.columnSize;
        final int scale;// = type.getDecimalDigits();

        public FlinkPipelineDecimalConvert(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public Object deApply(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.flink.cdc.common.data.DecimalData apply(Object o) {
            if (!(o instanceof BigDecimal)) {
                Number number = (Number) o;
                return org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal(
                        BigDecimal.valueOf(number.longValue()), precision, scale);
            }
            return org.apache.flink.cdc.common.data.DecimalData.fromBigDecimal((BigDecimal) o, precision, scale);
        }
    }

    public static class FlinkCDCPipelineEventTimestampDataConvert extends FlinkCol.DateTimeProcess {
        @Override
        public Object apply(Object o) {
            LocalDateTime v = (LocalDateTime) super.apply(o);
            return org.apache.flink.cdc.common.data.TimestampData.fromLocalDateTime(v);
        }
    }
}
