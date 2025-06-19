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

import com.alibaba.datax.common.element.ICol2Index;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-03 13:48
 **/
public class FlinkCol2Index implements ICol2Index, Serializable {
    final Map<String, ICol2Index.Col> col2IndexMapper;

    public static FlinkCol2Index create(List<FlinkCol> cols) {
        if (CollectionUtils.isEmpty(cols)) {
            throw new IllegalArgumentException("param cols can not be empty");
        }
        Map<String, Pair<Integer, FlinkCol>> col2IdxBuilder = com.google.common.collect.Maps.newHashMap();
        int idx = 0;
        for (FlinkCol col : cols) {
            col2IdxBuilder.put(col.name, Pair.of(idx++, col));
        }

        return new FlinkCol2Index(
                col2IdxBuilder.entrySet().stream().collect(
                        Collectors.toUnmodifiableMap((entry) -> entry.getKey() //
                                , (entry) -> new ICol2Index.Col(entry.getValue().getKey(), entry.getValue().getValue().colType))));
    }


    public FlinkCol2Index(Map<String, ICol2Index.Col> col2IndexMapper) {
        this.col2IndexMapper = (Objects.requireNonNull(col2IndexMapper, "param col2IndexMapper can not be null"));
    }

    @Override
    public int contextParamValsCount() {
        throw new UnsupportedOperationException("contextParamValsCount");
    }

    @Override
    public Map<String, ICol2Index.Col> getCol2Index() {
        return this.col2IndexMapper;
    }

    public Integer get(String field) {
        return Objects.requireNonNull(col2IndexMapper.get(field), "field:" + field + " relevant Col can not be null").getIndex();
    }

    public String descKeyVals() {
        return col2IndexMapper.entrySet().stream().map((entry) -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining(","));
    }
}
