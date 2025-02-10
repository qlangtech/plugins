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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.utils.DateTimeUtils;

import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-25 12:06
 * @see org.apache.flink.table.data.util.DataFormatConverters
 **/
public class RowFieldGetterFactory {

    public static RowData.FieldGetter intGetter(String colName, int colIndex) {
        return new IntGetter(colName, colIndex);
    }

    public static RowData.FieldGetter smallIntGetter(String colName, int colIndex) {
        return new SmallIntGetter(colName, colIndex);
    }

    public static class StringGetter extends BasicGetter {
        public StringGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        protected Object getObject(GenericRowData rowData) {

            Object val = rowData.getField(colIndex);
            if (val instanceof StringData) {
                return ((StringData) val).toString();
            }
            if (val instanceof byte[]) {
                return new String((byte[]) val);
            }
            return String.valueOf(val);
        }
    }

    public static class BlobGetter extends BasicGetter {
        public BlobGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        protected Object getObject(GenericRowData rowData) {
            Object val = rowData.getField(colIndex);
            if (val instanceof BinaryStringData) {
                return ((BinaryStringData) val).toBytes();
            }
            if (val instanceof Boolean) {
                return new byte[]{(byte) (((Boolean) val) ? 1 : 0)};
            }

            return rowData.getBinary(colIndex);
        }
    }

    public static class BoolGetter extends BasicGetter {
        public BoolGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {

            Object val = rowData.getField(colIndex);
            if (val instanceof java.lang.Byte) {
                return ((java.lang.Byte) val).shortValue() > 0;
            }

            return (Boolean) val; // rowData.getBoolean(colIndex);
        }
    }

    public static class DateGetter extends BasicGetter {

        public DateGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {
            Object val = rowData.getField(colIndex);
            LocalDate localDate = null;
            if (val instanceof org.apache.flink.table.data.TimestampData) {
                localDate = ((TimestampData) val).toLocalDateTime().toLocalDate();
            } else {
                localDate = LocalDate.ofEpochDay(rowData.getInt(colIndex));
            }
            return Date.valueOf(localDate);
        }
    }

    public static class TimestampGetter extends BasicGetter {

        public TimestampGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {

            Object val = rowData.getField(colIndex);
            if (val instanceof java.lang.Integer) {
                LocalDate localDate = LocalDate.of(0, 1, 1);

                LocalTime localTime = DateTimeUtils.toLocalTime((Integer) val);
                return Timestamp.valueOf(LocalDateTime.of(localDate, localTime));
            }

            return ((TimestampData) val).toTimestamp();
            //  return rowData.getTimestamp(colIndex, -1).toTimestamp();
        }
    }


    public static class DoubleGetter extends BasicGetter {
        public DoubleGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {
            // org.apache.flink.table.data.DecimalData or Double
            Object val = rowData.getField(colIndex);
            if (val instanceof DecimalData) {
                return ((DecimalData) val).toBigDecimal().doubleValue();
            }
            return ((Number) val).doubleValue();
//            rowData.getDecimal()
//            return rowData.getDouble(colIndex);
        }
    }

    public static class DecimalGetter extends BasicGetter {
        public DecimalGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {

            Object val = rowData.getField(colIndex);
            if (val instanceof Number) {
                return DecimalData.fromUnscaledLong(((Number) val).longValue(), 15, 0);
            }

            return (DecimalData) val;// rowData.getDecimal(colIndex, -1, -1);
        }
    }


    public static class BigIntGetter extends BasicGetter {
        public BigIntGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {
            Object val = rowData.getField(colIndex);
            if (val instanceof DecimalData) {
                return ((DecimalData) val).toBigDecimal().longValue();
            }
            return ((Number) val).longValue();
        }
    }

    public static class TimeGetter extends BasicGetter {
        public TimeGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Nullable
        @Override
        public Object getObject(GenericRowData rowData) {
            return Time.valueOf(DateTimeUtils.toLocalTime((rowData.getInt(colIndex))));
        }
    }


    public static class FloatGetter extends BasicGetter {
        public FloatGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {
            Object val = rowData.getField(colIndex);
            return ((Number) val).floatValue();//.getFloat(colIndex);
        }
    }


    public static class ByteGetter extends BasicGetter {
        public ByteGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {
            Object val = rowData.getField(colIndex);
            if (val instanceof Boolean) {
                return (byte) (((Boolean) val) ? 1 : 0);
            }
            if (val instanceof Short) {
                return ((Short) val).byteValue();
            }
            return rowData.getByte(colIndex);
        }
    }

    static class SmallIntGetter extends BasicGetter {
        public SmallIntGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {
            Object val = rowData.getField(colIndex);
            if (val instanceof java.lang.Byte) {
                return ((java.lang.Byte) val).shortValue();
            }
            return (Short) val; //rowData.getShort(colIndex);
        }
    }

    static class IntGetter extends BasicGetter {

        public IntGetter(String colName, int colIndex) {
            super(colName, colIndex);
        }

        @Override
        public Object getObject(GenericRowData rowData) {
            Object val = rowData.getField(colIndex);
            if (val instanceof Number) {
                return ((Number) val).intValue();
            }
            return rowData.getInt(colIndex);
        }
    }

    public static abstract class BasicGetter implements RowData.FieldGetter {
        public final int colIndex;
        final String colName;

        public BasicGetter(String colName, int colIndex) {
            this.colIndex = colIndex;
            this.colName = colName;
        }

        @Override
        public final Object getFieldOrNull(RowData rowData) {
            return getVal(rowData);
        }

        private Object getVal(RowData rowData) {
            if (rowData.isNullAt(this.colIndex)) {
                return null;
            }
            try {
                return getObject((GenericRowData) rowData);
            } catch (ClassCastException e) {
                throw new RuntimeException("colIdx:" + this.colIndex + ",colName:" + this.colName, e);
            }
        }

        protected abstract Object getObject(GenericRowData rowData);
    }
}
