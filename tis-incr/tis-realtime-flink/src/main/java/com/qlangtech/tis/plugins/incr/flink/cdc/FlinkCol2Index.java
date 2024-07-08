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
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-03 13:48
 **/
public class FlinkCol2Index implements ICol2Index, Serializable {
    final Map<String, Integer> col2IndexMapper;

    public FlinkCol2Index(Map<String, Integer> col2IndexMapper) {
        this.col2IndexMapper = Maps.newHashMap(Objects.requireNonNull(col2IndexMapper, "param col2IndexMapper can not be null"));
    }

    public Integer get(String field) {
        return col2IndexMapper.get(field);
    }

    public String descKeyVals() {
        return col2IndexMapper.entrySet().stream().map((entry) -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining(","));
    }
}
