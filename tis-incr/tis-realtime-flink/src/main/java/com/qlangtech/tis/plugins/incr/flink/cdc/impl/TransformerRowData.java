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

package com.qlangtech.tis.plugins.incr.flink.cdc.impl;

import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.cdc.DTOConvertTo;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractTransformerRecord;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-20 16:55
 **/
public class TransformerRowData extends AbstractTransformerRecord<RowData> implements RowData {
    protected Object[] rewriteVals;
    //  protected List<FlinkCol> rewriteCols;
    protected List<FlinkCol> originColsWithContextParamsFlinkCol;
    private final int delegateArity;

    public TransformerRowData(RowData row, List<FlinkCol> rewriteCols, List<FlinkCol> originColsWithContextParamsFlinkCol, int delegateArity) {
        super(DTOConvertTo.RowData, row, rewriteCols);
        if (!(row instanceof GenericRowData)) {
            throw new IllegalArgumentException("param row must be type of " + GenericRowData.class);
        }
        this.delegateArity = delegateArity;
        this.originColsWithContextParamsFlinkCol = originColsWithContextParamsFlinkCol;
        //  this.rewriteCols = rewriteCols;
        int newSize = rewriteCols.size();
        this.rewriteVals = new Object[newSize];
        if (this.delegateArity > newSize) {
            throw new IllegalStateException("delegateArbitary:" + this.delegateArity + " > newSize:" + newSize + " is not allowed");
        }
    }

//    @Override
//    public void setString(String field, String val) {
//        setColumn(field, (val == null) ? null : val);
//    }

//    @Override
//    public void setColumn(String field, Object val) {
//        Integer pos = getPos(field);
//        FlinkCol flinkCol = this.cols.get(pos);
//        rewriteVals[pos] = (val == null ? NULL : flinkCol.rowDataProcess.apply(val));
//    }

    @Override
    protected void setColumn(String field, BiFunction rowProcess, Object val) {
        Integer pos = getPos(field);
       // FlinkCol flinkCol = this.cols.get(pos);
        rewriteVals[pos] = (val == null ? NULL : rowProcess.apply(val));
    }

    @Override
    public Object getColumn(String field) {
        Integer pos = getPos(field);
        FlinkCol flinkCol = this.cols.get(pos);
        if (rewriteVals[pos] != null) {
            Object overWrite = rewriteVals[pos];
            return overWrite == NULL ? null : flinkCol.getRowDataVal(this);
        }

        return getColVal(originColsWithContextParamsFlinkCol.get(pos));
    }

//    @Override
//    public String getString(String field) {
//        StringData val = this.getString(getPos(field));
//        if (val == null) {
//            return null;
//        }
//        return val.toString();
//    }

    @Override
    public RowData getDelegate() {
        // final int rewriteValsLength = this.delegateArbitary;
        GenericRowData old = (GenericRowData) this.row;
        GenericRowData rowData = new GenericRowData(this.row.getRowKind(), this.delegateArity);
        Object rewriteVal = null;
        for (int i = 0; i < this.delegateArity; i++) {
            if ((rewriteVal = rewriteVals[i]) != null && rewriteVal != NULL) {
                rowData.setField(i, rewriteVal);
            } else {
                if (i < old.getArity()) {
                    rowData.setField(i, old.getField(i));
                }
            }
        }
        return rowData;
    }

    private Object getColVal(FlinkCol flinkCol) {
        return Objects.requireNonNull(flinkCol, "param flinkCol can not be null")
                .getRowDataVal(this.row);
    }


    @Override
    public int getArity() {
        return this.rewriteVals.length;
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
        if (rewriteVals[pos] != null) {
            return (rewriteVals[pos] == NULL);
        }

        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (Boolean) val;
        }

        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (Byte) val;
        }
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (Short) val;
        }
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (Integer) val;
        }
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {

        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (Long) val;
        }

        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (Float) val;
        }
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (Double) val;
        }
        return row.getDouble(pos);
    }

    @Override
    public StringData getString(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (StringData) val;
        }
        return row.getString(pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (DecimalData) val;
        }
        return row.getDecimal(pos, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {

        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (TimestampData) val;
        }

        return row.getTimestamp(pos, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (RawValueData<T>) val;
        }
        return row.getRawValue(pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (byte[]) val;
        }
        return row.getBinary(pos);
    }

    @Override
    public ArrayData getArray(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (ArrayData) val;
        }
        return row.getArray(pos);
    }

    @Override
    public MapData getMap(int pos) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (MapData) val;
        }
        return row.getMap(pos);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        Object val = null;
        if ((val = rewriteVals[pos]) != null && val != NULL) {
            return (RowData) val;
        }
        return row.getRow(pos, numFields);
    }
}
