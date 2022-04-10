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

import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-18 12:04
 **/
public final class DTO2RowDataMapper implements MapFunction<DTO, RowData> {
    private final List<FlinkCol> cols;

    public DTO2RowDataMapper(List<FlinkCol> cols) {
        if (CollectionUtils.isEmpty(cols)) {
            throw new IllegalArgumentException("param cols can not be empty");
        }
        this.cols = cols;
    }

    @Override
    public RowData map(DTO dto) throws Exception {
        GenericRowData row = new GenericRowData(DTO2RowMapper.getKind(dto), cols.size());
        int index = 0;
        Map<String, Object> vals
                = (dto.getEventType() == DTO.EventType.DELETE ? dto.getBefore() : dto.getAfter());
        Object val = null;
        for (FlinkCol col : cols) {
            val = vals.get(col.name);
            //col.type
            row.setField(index++, (val == null) ? null : col.processVal(val));
        }
        return row;
    }

//    private RowKind getKind(DTO dto) {
//        switch (dto.getEvent()) {
//            case DELETE:
//                return RowKind.DELETE;
//            case UPDATE:
//                return RowKind.UPDATE_AFTER;
//            case ADD:
//                return RowKind.INSERT;
//            default:
//                throw new IllegalStateException("invalid event type:" + dto.getEvent());
//        }
//
//    }
}
