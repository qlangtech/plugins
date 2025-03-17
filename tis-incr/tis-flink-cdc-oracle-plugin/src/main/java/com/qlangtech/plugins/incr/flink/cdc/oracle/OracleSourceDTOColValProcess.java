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

package com.qlangtech.plugins.incr.flink.cdc.oracle;

import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.ISourceValConvert;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.connect.data.Field;

import java.io.Serializable;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-02 23:48
 **/
public class OracleSourceDTOColValProcess implements ISourceValConvert, Serializable {
    final Map<String, FlinkColMapper> tabColsMapper;

    /**
     *
     * @param tabColsMapper <key:tableName,val>
     */
    public OracleSourceDTOColValProcess(Map<String, FlinkColMapper> tabColsMapper) {
        if (MapUtils.isEmpty(tabColsMapper)) {
            throw new IllegalArgumentException("tabColsMapper can not be null");
        }
        this.tabColsMapper = tabColsMapper;
    }

    @Override
    public Object convert(DTO dto, Field field, Object val) {
        FlinkColMapper colMapper = tabColsMapper.get(dto.getTableName());
        if (colMapper == null) {
            throw new IllegalStateException("tableName:" + dto.getTableName()
                    + " relevant colMapper can not be null, exist tabs:"
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
