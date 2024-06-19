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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-18 17:33
 **/
public class TransformerRowData implements RowData, ColumnAwareRecord<Object> {
    private GenericRowData row;
    private Map<String, Integer> col2IndexMapper;

    public TransformerRowData(GenericRowData row) {
        this.row = Objects.requireNonNull(row, "param row can not be null");
    }

    @Override
    public void setCol2Index(Map<String, Integer> mapper) {
        this.col2IndexMapper = mapper;
    }

    @Override
    public String getString(String field) {
        StringData val = this.getString(getPos(field));
        if (val == null) {
            return null;
        }
        return val.toString();
    }

    private Integer getPos(String field) {
        Integer pos = col2IndexMapper.get(field);
        if (pos == null) {
            throw new IllegalStateException("field:" + field + " relevant pos can not be null,exist:"
                    + col2IndexMapper.entrySet().stream().map((entry) -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining(",")));
        }
        return pos;
    }

    @Override
    public void setColumn(String field, Object val) {
        this.row.setField(getPos(field), val);
    }

    @Override
    public void setString(String field, String val) {
        Integer pos = getPos(field);
        if (val == null) {
            this.row.setField(pos, null);
        } else {
            this.row.setField(pos, StringData.fromString(val));
        }
    }

    @Override
    public Object getColumn(String field) {
        Integer pos = getPos(field);
        return this.row.getField(pos);
    }


    @Override
    public int getArity() {
        return row.getArity();
    }

    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos);
    }

    @Override
    public StringData getString(int pos) {
        return row.getString(pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(pos, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return row.getTimestamp(pos, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return row.getRawValue(pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(pos);
    }

    @Override
    public ArrayData getArray(int pos) {
        return row.getArray(pos);
    }

    @Override
    public MapData getMap(int pos) {
        return row.getMap(pos);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return row.getRow(pos, numFields);
    }


}
