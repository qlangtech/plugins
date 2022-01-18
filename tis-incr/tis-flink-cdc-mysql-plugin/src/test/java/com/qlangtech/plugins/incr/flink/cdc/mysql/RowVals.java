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

package com.qlangtech.plugins.incr.flink.cdc.mysql;

import com.google.common.collect.Maps;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-15 15:12
 **/
public class RowVals<T> {
    private final Map<String, T> vals;

    public RowVals() {
        this(Maps.newTreeMap());
    }

    public RowVals(Map<String, T> vals) {
        this.vals = vals;
    }

    public List<Map.Entry<String, T>> getCols() {
        return new ArrayList<>(vals.entrySet());
    }

    public Integer getInt(String key) {
        return (Integer) vals.get(key);
    }

    public String getString(String key) {
        return (String) vals.get(key);
    }

    public BigDecimal getBigDecimal(String key) {
        return (BigDecimal) vals.get(key);
    }

    public InputStream getInputStream(String key) {
        return (InputStream) vals.get(key);
    }

    public Object getObj(String key) {
        return vals.get(key);
    }

    public boolean isEmpty() {
        return vals.isEmpty();
    }

    public void put(String key, T val) {
        this.vals.put(key, val);
    }

    @Override
    public String toString() {
        return "RowVals{" +
                "vals=" + vals +
                '}';
    }
}
