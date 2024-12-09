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
import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.ISourceValConvert;
import com.qlangtech.plugins.incr.flink.cdc.RowFieldGetterFactory.BasicGetter;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.kafka.connect.data.Field;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-04 16:06
 **/
public class MongoDBSourceDTOColValProcess implements ISourceValConvert, Serializable {
    final Map<String /**tabName*/, FlinkColMapper> tabColsMapper;

    private transient Map<String, List<Field>> tabFields = Maps.newHashMap();

    public MongoDBSourceDTOColValProcess(Map<String, FlinkColMapper> tabColsMapper) {
        this.tabColsMapper = tabColsMapper;
    }

    public List<Field> getFields(String tabName) {
        List<Field> fields = null;

        if (tabFields == null) {
            tabFields = Maps.newHashMap();
        }

        if ((fields = tabFields.get(tabName)) == null) {
            FlinkColMapper colsMapper = Objects.requireNonNull(
                    tabColsMapper.get(tabName), "tabName:" + tabName + " relevant mapper can not be null");
            fields = colsMapper.getColMapper().values().stream()
                    .map((mapper) -> {
                        BasicGetter colValGetter = (BasicGetter) mapper.getRowDataValGetter();
                        return new Field(mapper.name, colValGetter.colIndex, null);
                    }).collect(Collectors.toUnmodifiableList());
            tabFields.put(tabName, fields);
        }

        return fields;

    }

    @Override
    public Object convert(DTO dto, Field field, Object val) {
        FlinkColMapper colMapper = tabColsMapper.get(dto.getTableName());
        if (colMapper == null) {
            throw new IllegalStateException("tableName:" + dto.getTableName()
                    + " relevant colMapper can not be null, exist cols:"
                    + String.join(",", tabColsMapper.keySet()));
        }
        BiFunction process = colMapper.getSourceDTOColValProcess(field.name());
        if (process == null) {
            // 说明用户在选在表的列时候，没有选择该列，所以就不用处理了
            return null;
        }
        return process.apply(val);
    }
}
