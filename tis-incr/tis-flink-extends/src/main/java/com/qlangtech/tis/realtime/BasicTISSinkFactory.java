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

package com.qlangtech.tis.realtime;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.DTO2RowDataMapper;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-05-13 23:01
 **/
public abstract class BasicTISSinkFactory<TRANSFER_OBJ> extends TISSinkFactory {

    @Override
    public abstract Map<IDataxProcessor.TableAlias, TabSinkFunc<TRANSFER_OBJ>> createSinkFunction(IDataxProcessor dataxProcessor);

    /**
     * (RowData,DTO) -> DTO
     */
    public final static class DTOSinkFunc extends TabSinkFunc<DTO> {
        public DTOSinkFunc(IDataxProcessor.TableAlias tab, SinkFunction<DTO> sinkFunction) {
            super(tab, sinkFunction);
        }

        @Override
        protected DataStream<DTO> streamMap(DTOStream sourceStream) {
            if (sourceStream.clazz == DTO.class) {
                return sourceStream.stream;
            } else if (sourceStream.clazz == RowData.class) {
                throw new UnsupportedOperationException("rowData -> DTO is not support");
            }

            throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
        }
    }

    /**
     * (RowData,DTO) -> RowData
     */
    public final static class RowDataSinkFunc extends TabSinkFunc<RowData> {
        final IStreamTableCreator.IStreamTableMeta streamTableMeta;

        public RowDataSinkFunc(IDataxProcessor.TableAlias tab
                , SinkFunction<RowData> sinkFunction, IStreamTableCreator.IStreamTableMeta streamTableMeta) {
            super(tab, sinkFunction);
            this.streamTableMeta = streamTableMeta;
        }

        @Override
        protected DataStream<RowData> streamMap(DTOStream sourceStream) {
            if (sourceStream.clazz == DTO.class) {
                // return sourceStream.stream;
                return sourceStream.stream.map(new DTO2RowDataMapper(
                        DTO2RowDataMapper.getAllTabColsMeta(this.streamTableMeta))).name(tab.getFrom() + "_dto2Rowdata");
            } else if (sourceStream.clazz == RowData.class) {
                return sourceStream.stream;
            }
            throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
        }
    }

//    /**
//     * RowData -> DTO
//     */
//    public final static class RowData2DTOSinkFunc extends TabSinkFunc<RowData, DTO> {
//        public RowData2DTOSinkFunc(IDataxProcessor.TableAlias tab, SinkFunction<DTO> sinkFunction) {
//            super(tab, sinkFunction);
//        }
//
//        @Override
//        protected DataStream<DTO> streamMap(DataStream<RowData> sourceStream) {
//            throw new UnsupportedOperationException();
//        }
//    }
//
//    public static abstract class RowDataSinkFunc extends TabSinkFunc<DTO, RowData> {
//        public RowDataSinkFunc(IDataxProcessor.TableAlias tab, SinkFunction<RowData> sinkFunction) {
//            super(tab, sinkFunction);
//        }
//    }
}
