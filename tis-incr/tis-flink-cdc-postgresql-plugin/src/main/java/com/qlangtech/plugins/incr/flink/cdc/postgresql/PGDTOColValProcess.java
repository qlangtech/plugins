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

package com.qlangtech.plugins.incr.flink.cdc.postgresql;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.ISourceValConvert;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper.DecimalConvert;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-02-21 20:57
 **/
public class PGDTOColValProcess implements ISourceValConvert, Serializable {
    final Map<String, FlinkColMapper> tabColsMapper;


    public PGDTOColValProcess(List<ISelectedTab> tabs,IFlinkColCreator<FlinkCol> flinkColCreator) {



        Map<String, FlinkColMapper> tabColsMapper = Maps.newHashMap();
        FlinkColMapper colsMapper = null;
        for (ISelectedTab tab : tabs) {
            colsMapper = AbstractRowDataMapper.getAllTabColsMetaMapper(tab.getCols(), flinkColCreator);
            tabColsMapper.put(tab.getName(), colsMapper);
        }

        this.tabColsMapper = tabColsMapper;
    }

    @Override
    public Object convert(DTO dto, Field field, Object val) {
        BiFunction process = tabColsMapper.get(dto.getTableName()).getSourceDTOColValProcess(field.name());
        if (process == null) {
            // 说明用户在选在表的列时候，没有选择该列，所以就不用处理了
            return null;
        }
        return process.apply(val);
    }


    static class PGCDCTypeVisitor extends AbstractRowDataMapper.DefaultTypeVisitor {
        public PGCDCTypeVisitor(IColMetaGetter meta, int colIndex) {
            super(meta, colIndex);
        }

        @Override
        public FlinkCol decimalType(DataType type) {
            FlinkCol flinkCol = super.decimalType(type);

            flinkCol.setSourceDTOColValProcess(new PGDecimalDTOConvert());
            return flinkCol;
        }
    }

    /**
     * for reslve issue: https://github.com/datavane/tis/issues/293
     *
     * @see DecimalConvert
     */
    static class PGDecimalDTOConvert extends BiFunction {


        @Override
        public Object apply(Object o) {
            /**
             * 测试中发现full_types表中的部分binlog接收到的值是Struct"Struct{wkb=[B@644ced88}" 需要继续拆包才能在下游中使用
             */
            if (o instanceof Struct) {
                Struct val = (Struct) o;
                //   Schema schema = val.schema();
                return new BigDecimal(new BigInteger(val.getBytes("value")), val.getInt32("scale"));
                // return vals.toString();
            }
            return o;
        }
    }

}
