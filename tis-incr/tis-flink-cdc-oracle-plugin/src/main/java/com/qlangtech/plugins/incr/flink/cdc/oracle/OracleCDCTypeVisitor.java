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

package com.qlangtech.plugins.incr.flink.cdc.oracle;

import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCDCPipelineEventProcess;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol.LocalDateProcess;
import com.qlangtech.plugins.incr.flink.cdc.RowFieldGetterFactory;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import io.debezium.data.VariableScaleDecimal;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-03 07:24
 **/
public class OracleCDCTypeVisitor extends AbstractRowDataMapper.DefaultTypeVisitor {
    public OracleCDCTypeVisitor(IColMetaGetter meta, int colIndex) {
        super(meta, colIndex);
    }

    @Override
    public FlinkCol timestampType(DataType type) {

        return new FlinkCol(meta, type, new AtomicDataType(new TimestampType(nullable, 3)) //DataTypes.TIMESTAMP(3)
                , new OracleTimestampDataConvert()
                , new OracleDateTimeProcess()
                , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(), new OracleFlinkCDCPipelineTimestampDataConvert())
                , new RowFieldGetterFactory.TimestampGetter(meta.getName(), colIndex));
    }

    public static class OracleDateTimeProcess extends FlinkCol.DateTimeProcess {
        @Override
        public Object apply(Object o) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Long) o) / 1000), ZoneId.systemDefault());
        }
    }

    public static class OracleTimestampDataConvert extends OracleDateTimeProcess {
        @Override
        public Object apply(Object o) {
            LocalDateTime v = (LocalDateTime) super.apply(o);
            return TimestampData.fromLocalDateTime(v);
        }
    }

    public static class OracleFlinkCDCPipelineTimestampDataConvert extends OracleDateTimeProcess {
        @Override
        public Object apply(Object o) {
            LocalDateTime v = (LocalDateTime) super.apply(o);
            return org.apache.flink.cdc.common.data.TimestampData.fromLocalDateTime(v);
        }
    }

    @Override
    public FlinkCol dateType(DataType type) {
        // return super.dateType(type);
        return new FlinkCol(meta, type, new AtomicDataType(new DateType(nullable))
                , new OracleDateConvert()
                , new OracleLocalDateProcess()
                , new FlinkCDCPipelineEventProcess(org.apache.flink.cdc.common.types.DataTypes.DATE(), new OracleDateConvert())
                , new RowFieldGetterFactory.DateGetter(meta.getName(), colIndex));
    }

    public static class OracleLocalDateProcess extends LocalDateProcess {
        @Override
        public Object apply(Object o) {
            return LocalDate.ofInstant(Instant.ofEpochMilli((Long) o), ZoneId.systemDefault());

        }
    }

    public static class OracleDateConvert extends OracleLocalDateProcess {
        @Override
        public Object apply(Object o) {
            LocalDate localDate = (LocalDate) super.apply(o);
            return Integer.valueOf((int) localDate.toEpochDay());
        }
    }

    @Override
    public FlinkCol intType(DataType type) {
        FlinkCol col = super.intType(type);
        return col.setSourceDTOColValProcess(new OracleIntegerValueDTOConvert());
    }

    @Override
    public FlinkCol decimalType(DataType type) {
        FlinkCol col = super.decimalType(type);
        return col;
        //   return col.setSourceDTOColValProcess(new MySQLBinaryRawValueDTOConvert());
    }

    static class OracleIntegerValueDTOConvert extends BiFunction {
        @Override
        public Object apply(Object o) {
            /**
             * 测试中发现full_types表中的部分binlog接收到的值是Struct"Struct{wkb=[B@644ced88}" 需要继续拆包才能在下游中使用
             */
            if (o instanceof Struct) {
                Struct val = (Struct) o;
                BigDecimal wrappedValue = VariableScaleDecimal.toLogical(val).getDecimalValue().orElse(BigDecimal.ZERO);
                return wrappedValue.intValue();
            }
            return o;
        }
    }
}
