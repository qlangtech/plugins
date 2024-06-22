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

import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import org.apache.flink.table.data.StringData;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-18 17:33
 **/
public abstract class AbstractTransformerRecord<Type> implements ColumnAwareRecord<Object> {

    protected static final Object NULL = new Object();

    protected Type row;
    protected Map<String, Integer> col2IndexMapper;



    public AbstractTransformerRecord(Type row) {
        this.row = Objects.requireNonNull(row, "param row can not be null");
    }

    @Override
    public void setCol2Index(Map<String, Integer> mapper) {
        this.col2IndexMapper = mapper;
    }


    protected Integer getPos(String field) {
        Integer pos = col2IndexMapper.get(field);
        if (pos == null) {
            throw new IllegalStateException("field:" + field + " relevant pos can not be null,exist:"
                    + col2IndexMapper.entrySet().stream().map((entry) -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining(",")));
        }
        return pos;
    }




    public abstract Type getDelegate();






}
