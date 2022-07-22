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

package com.qlangtech.tis.plugins.incr.flink.connector.mysql;

import com.dtstack.chunjun.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRowConverter;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-19 14:41
 **/
public class ChunjunRowDataMapper extends AbstractRowDataMapper {
    private final JdbcColumnConverter rowConverter;


    public ChunjunRowDataMapper(List<FlinkCol> cols, JdbcColumnConverter rowConverter) {
        super(cols);
        this.rowConverter = Objects.requireNonNull(rowConverter, "rowConverter can not be null");
        //this.rowConverter = rowConverter;
    }

    @Override
    protected void setRowDataVal(int index, RowData row, Object value) {
//        ColumnRowData rowData = (ColumnRowData) row;
//        AbstractBaseColumn col = null;
//        rowData.setField(index, col);
    }

    @Override
    protected RowData createRowData(DTO dto) {

//        if (rowConverter == null) {
//            this.rowConverter = (JdbcRowConverter) this.outputFormat.getRowConverter();
//        }
        try {
            return rowConverter.toInternal(new ChunjunResultSet(cols, dto));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //  return new ColumnRowData(DTO2RowMapper.getKind(dto), cols.size());
    }
}
