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
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

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
        this(cols, transformerRules.getTransformerUDFs());
    }

    public ReocrdTransformerMapper(List<FlinkCol> cols, List<UDFDefinition> transformerUDF) {
        this.cols = Lists.newArrayList(Objects.requireNonNull(cols, "cols can not be null"));
        this.transformerUDF = transformerUDF;
        this.col2IdxMapper = FlinkCol2Index.create(cols);
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
