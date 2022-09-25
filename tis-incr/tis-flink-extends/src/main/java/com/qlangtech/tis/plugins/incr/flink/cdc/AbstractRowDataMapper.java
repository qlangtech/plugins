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

package com.qlangtech.tis.plugins.incr.flink.cdc;

import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-18 12:04
 **/
public abstract class AbstractRowDataMapper implements MapFunction<DTO, RowData>, Serializable {
    protected final List<FlinkCol> cols;


    public AbstractRowDataMapper(List<FlinkCol> cols) {
        if (CollectionUtils.isEmpty(cols)) {
            throw new IllegalArgumentException("param cols can not be empty");
        }
        this.cols = cols;
    }

    public static List<FlinkCol> getAllTabColsMeta(TargetResName dataxName, String tabName) {
        IStreamTableCreator.IStreamTableMeta streamTableMeta = BasicFlinkSourceHandle.getStreamTableMeta(dataxName, tabName);
        // return streamTableMeta.getColsMeta().stream().map((c) -> mapFlinkCol(c)).collect(Collectors.toList());

        return getAllTabColsMeta(streamTableMeta);
    }

    public static List<FlinkCol> getAllTabColsMeta(IStreamTableCreator.IStreamTableMeta streamTableMeta) {
        //IStreamTableCreator.IStreamTableMeta streamTableMeta = BasicFlinkSourceHandle.getStreamTableMeta(dataxName, tabName);
        final AtomicInteger colIndex = new AtomicInteger();
        return streamTableMeta.getColsMeta()
                .stream()
                .map((c) -> mapFlinkCol(c, colIndex.getAndDecrement()))
                .collect(Collectors.toList());
    }

    public static List<FlinkCol> getAllTabColsMeta(List<IColMetaGetter> colsMeta) {
        //IStreamTableCreator.IStreamTableMeta streamTableMeta = BasicFlinkSourceHandle.getStreamTableMeta(dataxName, tabName);

        final AtomicInteger colIndex = new AtomicInteger();
        return colsMeta.stream()
                .map((c) -> mapFlinkCol(c, colIndex.getAndIncrement()))
                .collect(Collectors.toList());
    }

    public static FlinkCol mapFlinkCol(IColMetaGetter meta, int colIndex) {

        final boolean nullable = !meta.isPk();

        return meta.getType().accept(new DataType.TypeVisitor<FlinkCol>() {

            @Override
            public FlinkCol intType(DataType type) {
                return new FlinkCol(meta.getName()
                        , new AtomicDataType(new IntType(nullable)), new IntegerConvert()
                        , (rowData) -> rowData.getInt(colIndex));
            }

            @Override
            public FlinkCol smallIntType(DataType dataType) {
                return new FlinkCol(meta.getName(),
                        new AtomicDataType(new SmallIntType(nullable))
                        //DataTypes.SMALLINT()
                        , new ShortConvert()
                        , new RowShortConvert()
                        , (rowData) -> rowData.getShort(colIndex));
            }

            @Override
            public FlinkCol tinyIntType(DataType dataType) {
                return new FlinkCol(meta.getName(),
                        new AtomicDataType(new TinyIntType(nullable))
                        //         , DataTypes.TINYINT()
                        , new TinyIntConvertByte()
                        , new TinyIntConvertByte()
                        , (rowData) -> rowData.getByte(colIndex));
            }

            @Override
            public FlinkCol floatType(DataType type) {
                return new FlinkCol(meta.getName()
                        , DataTypes.FLOAT()
                        , new FloatDataConvert()
                        , new FloatDataConvert()
                        , (rowData) -> rowData.getFloat(colIndex));
            }

            @Override
            public FlinkCol timeType(DataType type) {
                return new FlinkCol(meta.getName() //
                        , DataTypes.TIME(3) //
                        , new LocalTimeConvert()
                        , (rowData) -> Time.valueOf(LocalTime.ofNanoOfDay(rowData.getInt(colIndex) * 1_000_000L)));
            }

            @Override
            public FlinkCol bigInt(DataType type) {
                return new FlinkCol(meta.getName()
                        , new AtomicDataType(new BigIntType(nullable))
                        // , DataTypes.BIGINT()
                        , new LongConvert()
                        , (rowData) -> {
                    return rowData.getLong(colIndex);
                });
            }

            public FlinkCol decimalType(DataType type) {
                int precision = type.columnSize;
                Integer scale = type.getDecimalDigits();
                if (precision < 1 || precision > 38) {
                    precision = 38;
                }
                try {

                    return new FlinkCol(meta.getName(), DataTypes.DECIMAL(precision, scale)
                            , new DecimalConvert(precision, scale)
                            , FlinkCol.NoOp()
                            , (rowData) -> rowData.getDecimal(colIndex, -1, -1));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + meta.getName() + ",type:" + type.toString() + ",precision:" + precision + ",scale:" + scale, e);
                }
            }

            @Override
            public FlinkCol doubleType(DataType type) {
                return new FlinkCol(meta.getName()
                        , DataTypes.DOUBLE()
                        , (rowData) -> rowData.getDouble(colIndex));
            }

            @Override
            public FlinkCol dateType(DataType type) {
                return new FlinkCol(meta.getName(), DataTypes.DATE()
                        , new DateConvert()
                        , FlinkCol.LocalDate()
                        , (rowData) -> Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(colIndex))));
            }

            @Override
            public FlinkCol timestampType(DataType type) {
                return new FlinkCol(meta.getName(), DataTypes.TIMESTAMP(3)
                        , new TimestampDataConvert()
                        , new FlinkCol.DateTimeProcess()
                        , (rowData) -> rowData.getTimestamp(colIndex, -1).toTimestamp());
            }

            @Override
            public FlinkCol bitType(DataType type) {
                return new FlinkCol(meta.getName(), DataTypes.BINARY(1)
                        , FlinkCol.Byte()
                        , (rowData) -> rowData.getByte(colIndex));
            }

            @Override
            public FlinkCol boolType(DataType dataType) {
                return new FlinkCol(meta.getName(), DataTypes.BOOLEAN()
                        , new FlinkCol.BoolProcess()
                        , (rowData) -> rowData.getBoolean(colIndex));
            }

            @Override
            public FlinkCol blobType(DataType type) {
                return new FlinkCol(meta.getName(), DataTypes.BYTES()
                        , new BinaryRawValueDataConvert()
                        , (rowData) -> rowData.getBinary(colIndex));
            }

            @Override
            public FlinkCol varcharType(DataType type) {
                return new FlinkCol(meta.getName()
                        , new AtomicDataType(new VarCharType(nullable, type.columnSize))
                        //, DataTypes.VARCHAR(type.columnSize)
                        , new StringConvert()
                        , FlinkCol.NoOp()
                        , (rowData) -> String.valueOf(rowData.getString(colIndex)));
            }
        });
    }


    @Override
    public RowData map(DTO dto) throws Exception {
        RowData row = createRowData(dto);

        Map<String, Object> vals
                = (dto.getEventType() == DTO.EventType.DELETE || dto.getEventType() == DTO.EventType.UPDATE_BEFORE)
                ? dto.getBefore() : dto.getAfter();
        if (vals == null) {
            throw new IllegalStateException("incr data of " + dto.getTableName() + " can not be null");
        }
        int index = 0;
        Object val = null;
        for (FlinkCol col : cols) {
            val = vals.get(col.name);
            //col.type
            // row.setField(index++, (val == null) ? null : col.processVal(val));
            setRowDataVal(index++, row, (val == null) ? null : col.processVal(val));
        }
        return row;
    }

    protected abstract void setRowDataVal(int index, RowData row, Object value);
//    {
//        GenericRowData rowData = (GenericRowData) row;
//        rowData.setField(index, value);
//    }

    protected abstract RowData createRowData(DTO dto);
//    {
//        return new GenericRowData(DTO2RowMapper.getKind(dto), cols.size());
//    }

    static class ShortConvert extends BiFunction {
        @Override
        public Object apply(Object o) {
            Short s = (Short) o;
            return s.intValue();
        }
    }

    static class RowShortConvert extends BiFunction {
        @Override
        public Object apply(Object o) {
            if (o instanceof Integer) {
                return ((Integer) o).shortValue();
            }
            Short s = (Short) o;
            return s;
        }
    }

    static class TinyIntConvertByte extends BiFunction {
        @Override
        public Object apply(Object o) {
            Short s = (Short) o;
            return new java.lang.Byte(s.byteValue());
            // return s.intValue();
        }
    }

    static class IntegerConvert extends BiFunction {
        @Override
        public Object apply(Object o) {
            if (o instanceof String) {
                return Integer.parseInt((String) o);
            }
            return o;
        }
    }

    static class DateConvert extends FlinkCol.LocalDateProcess {
        @Override
        public Object apply(Object o) {
            LocalDate localDate = (LocalDate) super.apply(o);
            return (int) localDate.toEpochDay();
        }
    }

    //  private static final ZoneId sysDefaultZone = ZoneId.systemDefault();

    static class TimestampDataConvert extends FlinkCol.DateTimeProcess {
        @Override
        public Object apply(Object o) {
            LocalDateTime v = (LocalDateTime) super.apply(o);
            return TimestampData.fromLocalDateTime(v);
//            ZoneOffset zoneOffset = sysDefaultZone.getRules().getOffset(v);
//            return v.toInstant(zoneOffset).toEpochMilli();
        }
    }

    static class FloatDataConvert extends BiFunction {
        @Override
        public Object apply(Object o) {
            if (o instanceof Number) {
                return ((Number) o).floatValue();
            }
            return o;
//            LocalDateTime v = (LocalDateTime) super.apply(o);
//            return TimestampData.fromLocalDateTime(v);
//            ZoneOffset zoneOffset = sysDefaultZone.getRules().getOffset(v);
//            return v.toInstant(zoneOffset).toEpochMilli();
        }
    }

    static class StringConvert extends BiFunction {
        @Override
        public Object deApply(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object apply(Object o) {
            return StringData.fromString((String) o);
        }
    }

    static class BinaryRawValueDataConvert extends BiFunction {
        @Override
        public Object deApply(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object apply(Object o) {
            java.nio.ByteBuffer buffer = (java.nio.ByteBuffer) o;
            return buffer.array();
        }
    }

    static class DecimalConvert extends BiFunction {
        //  private final DataType type;

        final int precision;// = type.columnSize;
        final int scale;// = type.getDecimalDigits();

        public DecimalConvert(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public Object deApply(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object apply(Object o) {
            return DecimalData.fromBigDecimal((BigDecimal) o, precision, scale);
        }
    }

    public static class LocalTimeConvert extends BiFunction {
        public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

        @Override
        public Object apply(Object o) {
            if (o instanceof String) {
                return LocalTime.parse((String) o, TIME_FORMATTER);
            }
            return (LocalTime) o;
        }
    }

    static class LongConvert extends BiFunction {
        @Override
        public Object deApply(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object apply(Object o) {
            if (o instanceof Number) {
                return ((Number) o).longValue();
            }
//            if (o instanceof Integer) {
//                return ((Integer) o).longValue();
//            }
            return Long.parseLong(String.valueOf(o));
        }
    }

}
