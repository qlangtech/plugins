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

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-18 12:04
 **/
public abstract class AbstractRowDataMapper extends BasicFlinkDataMapper<GenericRowData, RowData> {

    protected final List<FlinkCol> cols;

    public AbstractRowDataMapper(List<FlinkCol> cols, DTOConvertTo dtoConvert2Type) {
        super(dtoConvert2Type);
        if (CollectionUtils.isEmpty(cols)) {
            throw new IllegalArgumentException("param cols can not be empty");
        }
        this.cols = cols;
    }

    protected abstract void setRowDataVal(int index, GenericRowData row, Object value);

    @Override
    protected void fillRowVals(DTO dto, GenericRowData row) {
        Map<String, Object> vals
                = (dto.getEventType() == DTO.EventType.DELETE || dto.getEventType() == DTO.EventType.UPDATE_BEFORE)
                ? dto.getBefore() : dto.getAfter();
        if (vals == null) {
            throw new IllegalStateException("incr data of " + dto.getTableName() + " can not be null");
        }
        int index = 0;
        Object val = null;
        for (FlinkCol col : cols) {
            try {
                val = vals.get(col.name);
                setRowDataVal(index++, row, (val == null) ? null : dtoConvert2Type.processVal(col, val));
            } catch (Exception e) {
                throw new IllegalStateException("colName:" + col.name + ",index:" + index, e);
            }
        }
    }


}
