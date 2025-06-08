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

import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-05-13 23:01
 **/
public abstract class BasicTISSinkFactory<TRANSFER_OBJ> extends TISSinkFactory {
    public static final String KEY_SKIP_UPDATE_BEFORE_EVENT = "skipUpdateBeforeEventOrSpecEvent";

    private static final Logger logger = LoggerFactory.getLogger(BasicTISSinkFactory.class);

    @Override
    public abstract Map<TableAlias, TabSinkFunc<?, ?, TRANSFER_OBJ>> createSinkFunction(IDataxProcessor dataxProcessor, IFlinkColCreator flinkColCreator);


//    /**
//     * (RowData,DTO) -> DTO
//     */
//    public final static class DTOSinkFunc extends TabSinkFunc<SinkFunction<DTO>, DataStreamSink<DTO>, DTO> {
//
//        /**
//         * @param tab
//         * @param sinkFunction
//         * @param supportUpset 是否支持类似MySQL的replace类型的更新操作？
//         */
//        public DTOSinkFunc(TableAlias tab, List<String> primaryKeys, SinkFunction<DTO> sinkFunction
//                , boolean supportUpset, List<EventType> filterRowKinds, List<FlinkCol> colsMeta, int sinkTaskParallelism) {
//            super(tab, primaryKeys, sinkFunction, colsMeta, sinkTaskParallelism);
//            if (supportUpset || CollectionUtils.isNotEmpty(filterRowKinds)) {
//                this.setSourceFilter(KEY_SKIP_UPDATE_BEFORE_EVENT
//                        , new FilterUpdateBeforeEvent.DTOFilter(supportUpset, filterRowKinds));
//            }
//        }
//
//        @Override
//        protected DataStream<DTO> streamMap(DTOStream sourceStream) {
//            if (sourceStream.clazz == DTO.class) {
//                return sourceStream.getStream();
//            } else if (sourceStream.clazz == RowData.class) {
//                throw new UnsupportedOperationException("RowData -> DTO is not support");
//            }
//
//            throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
//        }
//
//        @Override
//        protected DataStreamSink<DTO> addSinkToSource(DataStream<DTO> source) {
//            return source.addSink(sinkFunction).name(tab.getTo()).setParallelism(this.sinkTaskParallelism);
//        }
//    }

}
