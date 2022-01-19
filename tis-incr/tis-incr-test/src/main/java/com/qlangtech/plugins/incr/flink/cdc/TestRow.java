/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.cdc;

import com.google.common.collect.Lists;
import org.apache.flink.types.RowKind;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-15 15:12
 **/
public class TestRow {
    private final RowKind kind;
    public final RowVals<Object> vals;
    public Object idVal;
    public final RowValsUpdate updateVals = new RowValsUpdate();

    boolean execUpdate() {
        return !updateVals.isEmpty();
    }

    boolean execDelete() {
        return this.willbeDelete;
    }


    boolean willbeDelete = false;

    public TestRow(RowKind kind, RowVals vals) {
        this.kind = kind;
        this.vals = vals;
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

    public List<String> getValsList(List<String> keys) throws Exception {
        return getValsList(keys, (rowVals, key, val) -> val);
    }

    public List<String> getValsList(List<String> keys, ValProcessor processor) throws Exception {
        return getValsList(false, keys, processor);
    }

    public List<String> getValsList(boolean updateVal, List<String> keys, ValProcessor processor) throws Exception {
        List<String> valsEnum = Lists.newArrayList(kind.shortString());
        for (String key : keys) {
            Object val = null;
            if (updateVal) {
                RowValsUpdate.UpdatedColVal uptColVal = (RowValsUpdate.UpdatedColVal) updateVals.getObj(key);
                if (uptColVal != null) {
                    val = uptColVal.updatedVal;
                }
            }
            if (val == null) {
                val = vals.getObj(key);
            }
            valsEnum.add(key + ":" + processor.process(vals, key, val));
        }
        return valsEnum;
    }

    interface ValProcessor {
        Object process(RowVals<Object> rowVals, String key, Object val) throws Exception;
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
//        public Integer getInt(String key) {
//            return (Integer) vals.get(key);
//        }

    @FunctionalInterface
   public interface ColValSetter {
        /**
         * @param statement
         * @param parameterIndex
         * @param ovals
         * @return newVal
         * @throws Exception
         */
        public Object setPrepColVal(PreparedStatement statement, int parameterIndex, RowVals<Object> ovals) throws Exception;
    }
}
