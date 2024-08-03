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
import com.alibaba.datax.common.element.ICol2Index;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-18 17:33
 **/
public abstract class AbstractTransformerRecord<Type> implements ColumnAwareRecord<Object> {

    protected static final Object NULL = new Object();

    protected Type row;
    protected FlinkCol2Index col2IndexMapper;

    public AbstractTransformerRecord(Type row) {
        this.row = Objects.requireNonNull(row, "param row can not be null");
    }

    @Override
    public void setCol2Index(ICol2Index mapper) {
        this.col2IndexMapper = (FlinkCol2Index) mapper;
    }

    @Override
    public ICol2Index getCol2Index() {
        return col2IndexMapper;
    }

    protected Integer getPos(String field) {
        Integer pos = col2IndexMapper.get(field);
        if (pos == null) {
            throw new IllegalStateException("field:" + field + " relevant pos can not be null,exist:"
                    + col2IndexMapper.descKeyVals());
        }
        return pos;
    }


    public abstract Type getDelegate();


}
