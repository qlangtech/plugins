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

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-18 12:04
 **/
public abstract class AbstractRowDataMapper implements MapFunction<DTO, RowData> {
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
        return streamTableMeta.getColsMeta().stream().map((c) -> mapFlinkCol(c)).collect(Collectors.toList());
    }

    private static FlinkCol mapFlinkCol(HdfsColMeta meta) {
        return meta.type.accept(new DataType.TypeVisitor<FlinkCol>() {

            @Override
            public FlinkCol intType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.INT());
            }

            @Override
            public FlinkCol smallIntType(DataType dataType) {
                return new FlinkCol(meta.colName, DataTypes.SMALLINT(), new ShortConvert());
            }

            @Override
            public FlinkCol tinyIntType(DataType dataType) {
                return new FlinkCol(meta.colName, DataTypes.TINYINT(), new ShortConvert());
            }

            @Override
            public FlinkCol floatType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.FLOAT());
            }

            @Override
            public FlinkCol timeType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.TIME(3));
            }

            @Override
            public FlinkCol bigInt(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.BIGINT());
            }

            public FlinkCol decimalType(DataType type) {
                try {
                    int precision = type.columnSize;
                    Integer scale = type.getDecimalDigits();
                    if (precision < 1) {
                        precision = 38;
                    }
                    return new FlinkCol(meta.colName, DataTypes.DECIMAL(precision, scale), new DecimalConvert(precision, scale));
                } catch (Exception e) {
                    throw new RuntimeException("colName:" + meta.colName + ",type:" + type.toString(), e);
                }
            }

            @Override
            public FlinkCol doubleType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.DOUBLE());
            }

            @Override
            public FlinkCol dateType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.DATE(), new DateConvert());
            }

            @Override
            public FlinkCol timestampType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.TIMESTAMP(3), new TimestampDataConvert());
            }

            @Override
            public FlinkCol bitType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.BINARY(1), FlinkCol.Byte());
            }

            @Override
            public FlinkCol blobType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.BYTES(), new BinaryRawValueDataConvert());
            }

            @Override
            public FlinkCol varcharType(DataType type) {
                return new FlinkCol(meta.colName, DataTypes.VARCHAR(type.columnSize), new StringConvert());
            }
        });

    }

    @Override
    public RowData map(DTO dto) throws Exception {
        RowData row = createRowData(dto);
        int index = 0;
        Map<String, Object> vals
                = (dto.getEventType() == DTO.EventType.DELETE || dto.getEventType() == DTO.EventType.UPDATE_BEFORE)
                ? dto.getBefore() : dto.getAfter();
        if (vals == null) {
            throw new IllegalStateException("incr data of " + dto.getTableName() + " can not be null");
        }
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

    static class ShortConvert extends FlinkCol.DateProcess {
        @Override
        public Object apply(Object o) {
            Short s = (Short) o;
            return s.intValue();
        }
    }

    static class DateConvert extends FlinkCol.DateProcess {
        @Override
        public Object apply(Object o) {
            LocalDate localDate = (LocalDate) super.apply(o);
            return (int) localDate.toEpochDay();
        }
    }

    private static final ZoneId sysDefaultZone = ZoneId.systemDefault();

    static class TimestampDataConvert extends FlinkCol.DateTimeProcess {
        @Override
        public Object apply(Object o) {
            LocalDateTime v = (LocalDateTime) super.apply(o);
            return TimestampData.fromLocalDateTime(v);
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

}
