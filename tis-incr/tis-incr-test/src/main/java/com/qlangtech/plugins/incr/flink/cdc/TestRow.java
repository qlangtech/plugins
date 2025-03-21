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

package com.qlangtech.plugins.incr.flink.cdc;

import com.qlangtech.plugins.incr.flink.cdc.RowValsExample.RowVal;
import org.apache.flink.types.RowKind;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-15 15:12
 **/
public class TestRow extends BasicRow {

    public final RowValsExample vals;
    public Object idVal;
    public final RowValsUpdate updateVals = new RowValsUpdate(this);
    private final Map<String, ColMeta> colMetaMapper;

    public ColMeta getColMetaMapper(String colKey) {
        return Objects.requireNonNull(this.colMetaMapper.get(colKey), "colKey:" + colKey + " relevant mapper can not be null");
    }

    public List<Map.Entry<String, RowValsUpdate.UpdatedColVal>> getUpdateValsCols() {
        List<Map.Entry<String, RowValsUpdate.UpdatedColVal>> cols = updateVals.getCols();
        return cols;
    }

    boolean execUpdate() {
        return !updateVals.isEmpty();
    }

    boolean execDelete() {
        return this.willbeDelete;
    }


    public boolean willbeDelete = false;

    public TestRow(RowKind kind, Map<String, ColMeta> colMetaMapper, RowValsExample vals) {
        super(kind);
        this.vals = vals;
        this.colMetaMapper = colMetaMapper;
    }

    public Integer getInt(String key) {
        return vals.getInt(key);
    }

    public String getString(String key) {
        return vals.getString(key);
    }

    public BigDecimal getBigDecimal(String key) {
        return vals.getBigDecimal(key);
    }

    public InputStream getInputStream(String key) {
        return vals.getInputStream(key);
    }


    //@Test
    protected Object getUpdateVal(ColMeta key) {
        Object val = null;
//        RowValsUpdate.UpdatedColVal uptColVal = (RowValsUpdate.UpdatedColVal) updateVals.getObj(key.getName());
//        if (uptColVal != null) {
//            val = uptColVal.updatedVal;
//        }

        RowValsUpdate.UpdatedColVal updateVal = updateVals.getV(key.getName());
        if (updateVal == null) {
            return getSerializeVal(key.getName());
        } else {
            return updateVal.updatedVal.getExpect();
        }

//        val =
//        //val = updateVals.getObj(key.getName());
////        if (val != null) {
////            val = uptColVal.updatedVal;
////        }
//        return val;
    }

    @Override
    public Object getObj(String key) {
        return vals.getObj(key);
    }

    @Override
    public Object getSerializeVal(String key) {
        try {
            RowVal getter = vals.getV(key);
            if (getter == null) {
                return null;
            }
            return getter.getExpect();
        } catch (Exception e) {
            throw new RuntimeException("key:" + key, e);
        }
    }


    @Override
    public String toString() {
        return "TestRow{" +
                "kind=" + kind +
                ", vals=" + vals +
                '}';
    }

    public Object getIdVal() {
        return this.idVal;
    }


    @FunctionalInterface
    public interface ColValSetter {
        /**
         * @param statement
         * @param ovals
         * @return newVal
         * @throws Exception
         */
        public RowValsExample.RowVal setPrepColVal(ColMeta colMeta, IStatementSetter statement, RowValsExample ovals) throws Exception;
    }
}
