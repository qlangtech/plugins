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
import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-18 17:33
 **/
public abstract class AbstractTransformerRecord<Type> implements ColumnAwareRecord<Object> {

    protected static final Object NULL = new Object();
    protected final List<FlinkCol> cols;
    protected Type row;
    protected FlinkCol2Index col2IndexMapper;
    protected final DTOConvertTo dtoConvert2Type;

    public AbstractTransformerRecord(DTOConvertTo dtoConvert2Type, Type row, List<FlinkCol> cols) {
        this.row = Objects.requireNonNull(row, "param row can not be null");
        this.cols = Objects.requireNonNull(cols, "cols can not be null");
        this.dtoConvert2Type = Objects.requireNonNull(dtoConvert2Type, "dtoConvert2Type can not be null");
    }

    @Override
    public void setCol2Index(ICol2Index mapper) {
        this.col2IndexMapper = (FlinkCol2Index) mapper;
    }

    protected final FlinkCol getFlinkCol(String field) {
        FlinkCol col = cols.get(getPos(field));
        return col;
    }

    @Override
    public final void setString(String field, final String val) {
        this.setColumn(field, this.dtoConvert2Type.stringValProcessor, val);
    }

    @Override
    public final void setColumn(String field, Object colVal) {
        FlinkCol col = getFlinkCol(field);
        this.setColumn(field, this.dtoConvert2Type.getValGetter(col), colVal);
    }

    protected abstract void setColumn(String field, BiFunction rowProcess, Object colVal);

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

    @Override
    public final String getString(String field) {
        return ColumnAwareRecord.super.getString(field);
    }

    /**
     * @param field
     * @param origin 取被替换前的值
     * @return
     * @see BasicFlinkDataMapper
     */
    @Override
    public final String getString(String field, boolean origin) {
        Object colVal = this.getColumn(field);
        FlinkCol col = getFlinkCol(field);
        return this.dtoConvert2Type.toString(col, colVal);

        //  return colVal != null ? String.valueOf(colVal) : null;
    }

    public abstract Type getDelegate();


}
