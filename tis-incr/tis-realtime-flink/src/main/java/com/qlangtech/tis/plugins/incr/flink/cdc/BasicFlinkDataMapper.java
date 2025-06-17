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
import com.qlangtech.plugins.incr.flink.cdc.FlinkCDCPipelineEventProcess;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCDCPipelineEventProcess.FlinkCDCPipelineEventTimestampDataConvert;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCDCPipelineEventProcess.FlinkPipelineDecimalConvert;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCDCPipelineEventProcess.FlinkPipelineStringConvert;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol.DTOConvertTo;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol.PipelineBooleanProcess;
import com.qlangtech.plugins.incr.flink.cdc.RowFieldGetterFactory;
import com.qlangtech.plugins.incr.flink.cdc.RowFieldGetterFactory.ByteGetter;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IStreamTableMeta;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 11:11
 **/
public abstract class BasicFlinkDataMapper<IMPLDATA extends DATA, DATA> implements MapFunction<DTO, DATA>, Serializable {

    protected final DTOConvertTo dtoConvert2Type;

    public BasicFlinkDataMapper(DTOConvertTo dtoConvert2Type) {

        this.dtoConvert2Type = Objects.requireNonNull(dtoConvert2Type, "dtoConvert2Type can not be null");
    }

    @Override
    public final DATA map(DTO dto) throws Exception {
        IMPLDATA row = createRowData(dto);
        this.fillRowVals(dto, row);
        return row;
    }

    protected abstract void fillRowVals(DTO dto, IMPLDATA row);


    public static List<FlinkCol> getAllTabColsMeta(TargetResName dataxName, TableAlias tabName) {
        IStreamTableMeta streamTableMeta = BasicFlinkSourceHandle.getStreamTableMeta(dataxName, tabName);
        return getAllTabColsMeta(streamTableMeta);
    }

    public static List<FlinkCol> getAllTabColsMeta(IStreamTableMeta streamTableMeta) {
        final AtomicInteger colIndex = new AtomicInteger();
        return streamTableMeta.getColsMeta()
                .stream()
                .map((c) -> mapFlinkCol(c, colIndex.getAndDecrement()))
                .collect(Collectors.toList());
    }

    public static <T extends IColMetaGetter> List<FlinkCol> getAllTabColsMeta(List<T> colsMeta) {
        return FlinkCol.getAllTabColsMeta(colsMeta, AbstractRowDataMapper::mapFlinkCol);
    }


    public static <T extends IColMetaGetter> FlinkColMapper getAllTabColsMetaMapper(List<T> colsMeta) {
        return getAllTabColsMetaMapper(colsMeta, AbstractRowDataMapper::mapFlinkCol);
    }

    public static <T extends IColMetaGetter> FlinkColMapper getAllTabColsMetaMapper(List<T> colsMeta, IFlinkColCreator<FlinkCol> flinkColCreator) {
        List<FlinkCol> cols = FlinkCol.getAllTabColsMeta(colsMeta, flinkColCreator);
        return new FlinkColMapper(cols.stream().collect(Collectors.toMap((c) -> c.name, (c) -> c)));
    }

    public static class DefaultTypeVisitor implements DataType.TypeVisitor<FlinkCol>, Serializable {
        protected final IColMetaGetter meta;
        protected final int colIndex;
        protected boolean nullable;

        public DefaultTypeVisitor(IColMetaGetter meta, int colIndex) {
            this.meta = meta;
            this.colIndex = colIndex;
            this.nullable = !meta.isPk();
        }

        @Override
        public FlinkCol intType(DataType type) {
            return new FlinkCol(meta, type
                    , new AtomicDataType(new IntType(nullable)) //
                    , new IntegerConvert()
                    , new IntegerConvert()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.INT(), new IntegerConvert())
                    , RowFieldGetterFactory.intGetter(meta.getName(), colIndex));
        }


        @Override
        public FlinkCol smallIntType(DataType dataType) {
            return new FlinkCol(meta, dataType,
                    new AtomicDataType(new SmallIntType(nullable))
                    , new ShortConvert()
                    , new RowShortConvert()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.SMALLINT(), new ShortConvert())
                    , RowFieldGetterFactory.smallIntGetter(meta.getName(), colIndex));
        }


        @Override
        public FlinkCol tinyIntType(DataType dataType) {
            return new FlinkCol(meta, dataType,
                    new AtomicDataType(new TinyIntType(nullable))
                    //         , DataTypes.TINYINT()
                    , new TinyIntConvertByte()
                    , new TinyIntConvertByte()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.TINYINT(), new TinyIntConvertByte())
                    , new RowFieldGetterFactory.ByteGetter(meta.getName(), colIndex));
        }


        @Override
        public FlinkCol floatType(DataType type) {
            return new FlinkCol(meta, type
                    , DataTypes.FLOAT()
                    , new FloatDataConvert()
                    , new FloatDataConvert()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.FLOAT(), new FloatDataConvert())
                    , new RowFieldGetterFactory.FloatGetter(meta.getName(), colIndex));
        }


        /**
         * <pre>
         *
         * 由于报一下错误，将DataTypes.TIME(3) 改成 DataTypes.TIME()
         *
         * Caused by: org.apache.flink.table.api.ValidationException: Type TIME(3) of table field 'time_c' does not match with the physical type TIME(0) of the 'time_c' field of the TableSink consumed type.
         * at org.apache.flink.table.utils.TypeMappingUtils.lambda$checkPhysicalLogicalTypeCompatible$5(TypeMappingUtils.java:190)
         * at org.apache.flink.table.utils.TypeMappingUtils$1.defaultMethod(TypeMappingUtils.java:326)
         * at org.apache.flink.table.utils.TypeMappingUtils$1.defaultMethod(TypeMappingUtils.java:291)
         * at org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor.visit(LogicalTypeDefaultVisitor.java:127)
         * at org.apache.flink.table.types.logical.TimeType.accept(TimeType.java:134)
         * at org.apache.flink.table.utils.TypeMappingUtils.checkIfCompatible(TypeMappingUtils.java:290)
         * at org.apache.flink.table.utils.TypeMappingUtils.checkPhysicalLogicalTypeCompatible(TypeMappingUtils.
         *
         * </pre>
         *
         * @param type
         * @return
         */
        @Override
        public FlinkCol timeType(DataType type) {
            return new FlinkCol(meta //
                    , type
                    // , DataTypes.TIME(3) //
                    , DataTypes.TIME()
                    , new DTOLocalTimeConvert()
                    , new LocalTimeConvert()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.TIME(), new DTOLocalTimeConvert())
                    // , (rowData) -> Time.valueOf(LocalTime.ofNanoOfDay(rowData.getInt(colIndex) * 1_000_000L))
                    , new RowFieldGetterFactory.TimeGetter(meta.getName(), colIndex));
        }


        @Override
        public FlinkCol bigInt(DataType type) {
            return new FlinkCol(meta
                    , type
                    , new AtomicDataType(new BigIntType(nullable))
                    // , DataTypes.BIGINT()
                    , new LongConvert()
                    , new LongConvert()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.BIGINT(), new LongConvert())
                    , new RowFieldGetterFactory.BigIntGetter(meta.getName(), colIndex));
        }


        public FlinkCol decimalType(DataType type) {
            int[] normalize = DataTypeMeta.normalizeDecimalPrecisionAndScale(type);
            int precision = normalize[0];// type.getColumnSize();
            Integer scale = normalize[1];// type.getDecimalDigits();
//            if (precision < 1 || precision > DataTypeMeta.DEFAULT_DECIMAL_PRECISION) {
//                precision = DataTypeMeta.DEFAULT_DECIMAL_PRECISION;
//            }
            try {
                return new FlinkCol(meta, type, //DataTypes.DECIMAL(precision, scale)
                        new AtomicDataType(new DecimalType(nullable, precision, scale))
                        , new DecimalConvert(precision, scale)
                        , FlinkCol.NoOp()
                        , new FlinkCDCPipelineEventProcess(
                        org.apache.flink.cdc.common.types.DataTypes.DECIMAL(precision, scale)
                        , new FlinkPipelineDecimalConvert(precision, scale))
                        , new RowFieldGetterFactory.DecimalGetter(meta.getName(), colIndex));
            } catch (Exception e) {
                throw new RuntimeException("colName:" + meta.getName() + ",type:" + type.toString() + ",precision:" + precision + ",scale:" + scale, e);
            }
        }


        @Override
        public FlinkCol doubleType(DataType type) {
            return new FlinkCol(meta, type
                    , DataTypes.DOUBLE()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.DOUBLE(), FlinkCol.NoOp())
                    , new RowFieldGetterFactory.DoubleGetter(meta.getName(), colIndex));
        }

        @Override
        public FlinkCol dateType(DataType type) {
            return new FlinkCol(meta, type, new AtomicDataType(new DateType(nullable))
                    , new DateConvert()
                    , FlinkCol.LocalDate()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.DATE(), new DateConvert())
                    , new RowFieldGetterFactory.DateGetter(meta.getName(), colIndex));
        }

        @Override
        public FlinkCol timestampType(DataType type) {
            return new FlinkCol(meta, type
                    , new AtomicDataType(new TimestampType(nullable, 3))
                    , new TimestampDataConvert()
                    , new FlinkCol.DateTimeProcess()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(), new FlinkCDCPipelineEventTimestampDataConvert())
                    , new RowFieldGetterFactory.TimestampGetter(meta.getName(), colIndex));
        }

        @Override
        public FlinkCol bitType(DataType type) {
            return new FlinkCol(meta, type, DataTypes.TINYINT()
                    , FlinkCol.Byte()
                    , FlinkCol.NoOp()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.BOOLEAN(), new PipelineBooleanProcess())
                    , new ByteGetter(meta.getName(), colIndex));
        }

        @Override
        public FlinkCol boolType(DataType dataType) {
            FlinkCol fcol = new FlinkCol(meta
                    , dataType //
                    , DataTypes.BOOLEAN() //
                    , new FlinkCol.BoolProcess() //
                    , FlinkCol.NoOp() //
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.BOOLEAN(), new FlinkCol.BoolProcess()) //
                    , new RowFieldGetterFactory.BoolGetter(meta.getName(), colIndex));
            return fcol.setSourceDTOColValProcess(new BiFunction() {
                @Override
                public Object apply(Object o) {
                    if (o instanceof Number) {
                        return ((Number) o).shortValue() > 0;
                    }
                    return (Boolean) o;
                }
            });
        }


        @Override
        public FlinkCol blobType(DataType type) {
            FlinkCol col = new FlinkCol(meta, type, DataTypes.BYTES()
                    , new BinaryRawValueDataConvert()
                    , FlinkCol.NoOp()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.BYTES(), new BinaryRawValueDataConvert())
                    , new RowFieldGetterFactory.BlobGetter(meta.getName(), colIndex));
            return col.setSourceDTOColValProcess(new BinaryRawValueDTOConvert());
        }


        @Override
        public FlinkCol varcharType(DataType type) {
            return new FlinkCol(meta //
                    , type
                    , new AtomicDataType(new VarCharType(nullable, type.getColumnSize()))
                    //, DataTypes.VARCHAR(type.columnSize)
                    , new StringConvert()
                    , FlinkCol.NoOp()
                    , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.VARCHAR(type.getColumnSize()), new FlinkPipelineStringConvert())
                    , new RowFieldGetterFactory.StringGetter(meta.getName(), colIndex));
        }


    }

//    public static FlinkCol mapFlinkCol(IColMetaGetter meta, int colIndex) {
//        return mapFlinkCol(meta, colIndex);
//    }

    public static FlinkCol mapFlinkCol(IColMetaGetter meta, int colIndex) {
        try {
            return meta.getType().accept(new DefaultTypeVisitor(meta, colIndex));
        } catch (Exception e) {
            throw new RuntimeException("col:" + meta.getName() + ",colIndex:" + colIndex + ",type:" + meta.getType().getTypeDesc(), e);
        }
    }





    protected abstract IMPLDATA createRowData(DTO dto);

    static class ShortConvert extends BiFunction {
        @Override
        public Object apply(Object o) {
            if (o instanceof Number) {
                return ((Number) o).shortValue();
            }
            return Short.parseShort(String.valueOf(o));
            // throw new IllegalStateException("val:" + o + ",type:" + o.getClass().getName());
        }
    }

    static class RowShortConvert extends BiFunction {
        @Override
        public Object apply(Object o) {
            if (o instanceof Number) {
                return ((Number) o).shortValue();
            }
            return Short.parseShort(String.valueOf(o));
//            Short s = (Short) o;
//            return s;
        }
    }

    static class TinyIntConvertByte extends BiFunction {
        @Override
        public Object apply(Object o) {
            Number s;
            if (o instanceof Boolean) {
                s = ((Boolean) o) ? (short) 1 : 0;
            } else {
                s = (Number) o;
            }

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

    public static class DateConvert extends FlinkCol.LocalDateProcess {
        @Override
        public Object apply(Object o) {
            LocalDate localDate = (LocalDate) super.apply(o);
            return (int) localDate.toEpochDay();
        }
    }

    //  private static final ZoneId sysDefaultZone = ZoneId.systemDefault();

    public static class TimestampDataConvert extends FlinkCol.DateTimeProcess {
        @Override
        public Object apply(Object o) {
            LocalDateTime v = (LocalDateTime) super.apply(o);
            return TimestampData.fromLocalDateTime(v);
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
            if (o instanceof java.nio.ByteBuffer) {
                return StringData.fromBytes(((java.nio.ByteBuffer) o).array());
            }
            return StringData.fromString(String.valueOf(o));
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

    public static class BinaryRawValueDTOConvert extends BiFunction {

        @Override
        public Object apply(Object o) {
            if (o instanceof java.nio.ByteBuffer) {
                return o;
            } else if (o instanceof java.lang.Short) {
                return shortToByteArray((java.lang.Short) o);
            }
            return java.nio.ByteBuffer.wrap((byte[]) o);
        }

        private static ByteBuffer shortToByteArray(short number) {
            //  byte[] byteArray = new byte[2];
            ByteBuffer buffer = ByteBuffer.allocate(2);
            buffer.putShort(number);
            buffer.flip();
            return buffer;
//            buffer.get(byteArray);
//            return byteArray;
        }
    }

    public static class DecimalConvert extends BiFunction {
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
            if (!(o instanceof BigDecimal)) {
                Number number = (Number) o;
                return DecimalData.fromBigDecimal(BigDecimal.valueOf(number.longValue()), precision, scale);
            }
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

    static class DTOLocalTimeConvert extends LocalTimeConvert {
        @Override
        public Object apply(Object o) {
            LocalTime time = (LocalTime) super.apply(o);
            return DateTimeUtils.toInternal(time);
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