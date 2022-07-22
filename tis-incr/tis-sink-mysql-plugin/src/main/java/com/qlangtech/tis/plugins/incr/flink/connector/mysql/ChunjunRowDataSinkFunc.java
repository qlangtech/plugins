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
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.plugins.incr.flink.cdc.DTO2RowDataMapper;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-19 14:54
 **/
public class ChunjunRowDataSinkFunc extends BasicTISSinkFactory.RowDataSinkFunc {
    final IStreamTableCreator.IStreamTableMeta streamTableMeta;
    private final JdbcColumnConverter rowConverter;

    public ChunjunRowDataSinkFunc(IDataxProcessor.TableAlias tab, JdbcColumnConverter rowConverter
            , IStreamTableCreator.IStreamTableMeta streamTableMeta, SinkFunction<RowData> sinkFunction) {
        super(tab, sinkFunction);
        this.streamTableMeta = streamTableMeta;
        this.rowConverter = rowConverter;
    }

    @Override
    protected DataStream<RowData> streamMap(DataStream<DTO> sourceStream) {
//        return sourceStream.map(new ChunjunRowDataMapper(
//                DTO2RowDataMapper.getAllTabColsMeta(this.streamTableMeta))).name(tab.getFrom() + "_dto2rowdata");
//        return sourceStream.map(new ChunjunRowDataMapper(
//                DTO2RowDataMapper.getAllTabColsMeta(this.streamTableMeta), this.rowConverter)).name(tab.getFrom() + "_dto2rowdata");

        return sourceStream.map(new DTO2RowDataMapper(
                DTO2RowDataMapper.getAllTabColsMeta(this.streamTableMeta))).name(tab.getFrom() + "_dto2rowdata");
    }
}
