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

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 执行TIS中定义的 transformer执行逻辑
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-18 15:29
 **/
public class RowDataTransformerMapper implements MapFunction<RowData, RowData>, Serializable {
    private final List<FlinkCol> cols;
    private final List<UDFDefinition> transformerUDF;
    private final Map<String, Integer> col2IdxMapper;

    public RowDataTransformerMapper(List<FlinkCol> cols, RecordTransformerRules transformerRules, IFlinkColCreator<FlinkCol> flinkColCreator) {
        if (flinkColCreator == null) {
            throw new IllegalArgumentException("param flinkColCreator can not be null");
        }
        this.cols = Lists.newArrayList(Objects.requireNonNull(cols, "cols can not be null"));
        this.transformerUDF
                = Objects.requireNonNull(transformerRules, "param transformerRules can not be null")
                .rules.stream().map((t) -> t.getUdf()).collect(Collectors.toList());

        Map<String, Pair<Integer, FlinkCol>> col2IdxBuilder = Maps.newHashMap();
        int idx = 0;
        for (FlinkCol col : cols) {
            col2IdxBuilder.put(col.name, Pair.of(idx++, col));
        }
        FlinkCol col = null;
        for (TargetColType colType : transformerRules.relevantTypedColKeys()) {
            if (colType.isVirtual()) {
                // 新增虚拟列
                col = flinkColCreator.build(colType, idx);
                col2IdxBuilder.put(colType.getName(), Pair.of(idx++, col));
                this.cols.add(col);
            } else {
                // 替换已有列
                Pair<Integer, FlinkCol> exist = col2IdxBuilder.get(colType.getName());
                if (exist == null) {
                    throw new IllegalStateException("colName:" + colType.getName() + " relevant table col conf can not be null");
                }
                int existIdx = exist.getLeft();
                col = flinkColCreator.build(colType, existIdx);
                col2IdxBuilder.put(colType.getName(), Pair.of(existIdx, col));
                this.cols.set(existIdx, col);
            }
        }

        this.col2IdxMapper = Collections.unmodifiableMap(
                col2IdxBuilder.entrySet().stream().collect(
                        Collectors.toMap((entry) -> entry.getKey(), (entry) -> entry.getValue().getKey())));
    }

    @Override
    public RowData map(RowData row) throws Exception {
        GenericRowData grow = new GenericRowData(row.getRowKind(), this.cols.size());
        TransformerRowData trowData = new TransformerRowData(grow);
        trowData.setCol2Index(this.col2IdxMapper);
        for (UDFDefinition transformer : this.transformerUDF) {
            transformer.evaluate(trowData);
        }

        return trowData;
    }
}
