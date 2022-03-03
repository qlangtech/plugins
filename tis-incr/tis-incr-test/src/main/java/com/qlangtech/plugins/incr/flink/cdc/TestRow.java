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

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.google.common.collect.Lists;
import org.apache.flink.types.RowKind;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;

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

    public List<String> getValsList(List<HdfsColMeta> keys) throws Exception {
        return getValsList(keys, (rowVals, key, val) -> val);
    }

    public List<String> getValsList(List<HdfsColMeta> keys, ValProcessor processor) throws Exception {
        return getValsList(Optional.empty(), keys, processor);
    }

    public List<String> getValsList(Optional<RowKind> updateVal, List<HdfsColMeta> keys, ValProcessor processor) throws Exception {
        RowKind rowKind = updateVal.isPresent() ? updateVal.get() : this.kind;
        List<String> valsEnum = Lists.newArrayList(rowKind.shortString());
        for (HdfsColMeta key : keys) {
            Object val = null;
            if (rowKind != RowKind.INSERT) {
                RowValsUpdate.UpdatedColVal uptColVal = (RowValsUpdate.UpdatedColVal) updateVals.getObj(key.getName());
                if (uptColVal != null) {
                    val = uptColVal.updatedVal;
                }
            }
            if (val == null) {
                val = vals.getObj(key.getName());
            }
            valsEnum.add(key.getName() + ":" + processor.process(vals, key.getName(), val));
        }
        return valsEnum;
    }

    public Object get(String key) {
        return vals.getObj(key);
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
