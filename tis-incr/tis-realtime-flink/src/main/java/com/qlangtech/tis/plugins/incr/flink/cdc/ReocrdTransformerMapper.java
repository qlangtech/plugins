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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.MapFunction;

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
public abstract class ReocrdTransformerMapper<Type> implements MapFunction<Type, Type>, Serializable {
    public final List<FlinkCol> cols;
    private final List<UDFDefinition> transformerUDF;
    private final FlinkCol2Index col2IdxMapper;

    public ReocrdTransformerMapper(List<FlinkCol> cols, RecordTransformerRules transformerRules) {

        this.cols = Collections.unmodifiableList(Objects.requireNonNull(cols, "cols can not be null"));
        this.transformerUDF
                = Objects.requireNonNull(transformerRules, "param transformerRules can not be null")
                .rules.stream().map((t) -> t.getUdf()).collect(Collectors.toList());

        Map<String, Pair<Integer, FlinkCol>> col2IdxBuilder = Maps.newHashMap();
        int idx = 0;
        for (FlinkCol col : cols) {
            col2IdxBuilder.put(col.name, Pair.of(idx++, col));
        }

        this.col2IdxMapper = new FlinkCol2Index(Collections.unmodifiableMap(
                col2IdxBuilder.entrySet().stream().collect(
                        Collectors.toMap((entry) -> entry.getKey(), (entry) -> entry.getValue().getKey()))));
    }

    @Override
    public Type map(Type row) throws Exception {

        AbstractTransformerRecord<Type> trowData = createDelegate(row);
        trowData.setCol2Index(this.col2IdxMapper);
        for (UDFDefinition transformer : this.transformerUDF) {
            transformer.evaluate(trowData);
        }

        return trowData.getDelegate();
    }

    protected abstract AbstractTransformerRecord<Type> createDelegate(Type row);
}
