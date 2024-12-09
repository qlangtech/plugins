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

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.ColMeta;
import com.qlangtech.plugins.incr.flink.cdc.IStatementSetter;
import org.bson.Document;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-06 09:24
 **/
public class MongoStatementSetter implements IStatementSetter {

    private final Map<String, Object> vals = Maps.newHashMap();

    public Document createDoc() {
        return new Document(vals);
    }

    @Override
    public void setNull(ColMeta colMeta, int type) throws Exception {
        vals.put(colMeta.getName(), null);
    }

    @Override
    public void setLong(ColMeta colMeta, Long obj) throws Exception {
        vals.put(colMeta.getName(), obj);
    }

    @Override
    public void setDouble(ColMeta colMeta, Double obj) throws Exception {
        vals.put(colMeta.getName(), obj);
    }

    @Override
    public void setDate(ColMeta colMeta, Date obj) throws Exception {
        vals.put(colMeta.getName(), obj);
    }

    @Override
    public void setTimestamp(ColMeta colMeta, Timestamp val) throws Exception {
        vals.put(colMeta.getName(), val);
    }

    @Override
    public void setBoolean(ColMeta colMeta, Boolean obj) throws Exception {
        vals.put(colMeta.getName(), obj);
    }

    @Override
    public void setByte(ColMeta colMeta, byte b) throws Exception {
        vals.put(colMeta.getName(), b);
    }

    @Override
    public void setBytes(ColMeta colMeta, byte[] byteVal) throws Exception {
        vals.put(colMeta.getName(), byteVal);
    }

    @Override
    public void setString(ColMeta colMeta, String val) throws Exception {
        vals.put(colMeta.getName(), val);
    }

    @Override
    public void setInt(ColMeta colMeta, Integer val) throws Exception {
        vals.put(colMeta.getName(), val);
    }

    @Override
    public void setFloat(ColMeta colMeta, Float obj) throws Exception {
        vals.put(colMeta.getName(), obj);
    }

    @Override
    public void setBigDecimal(ColMeta colMeta, BigDecimal val) throws Exception {
        vals.put(colMeta.getName(), val);
    }

    @Override
    public void setTime(ColMeta colMeta, Time obj) throws Exception {
        vals.put(colMeta.getName(), obj);
    }

    @Override
    public void setShort(ColMeta colMeta, Short val) throws Exception {
        vals.put(colMeta.getName(), val);
    }
}
