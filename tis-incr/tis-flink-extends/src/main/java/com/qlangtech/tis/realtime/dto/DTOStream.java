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

package com.qlangtech.tis.realtime.dto;

import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-27 10:19
 **/
public abstract class DTOStream<T> {

    protected transient DataStream<T> stream;
    public transient final Class<T> clazz;

    private DTOStream(Class<T> clazz) {
        this.clazz = clazz;
    }

    public DataStream<T> getStream() {
        return this.stream;
    }

    public abstract DTOStream<T> addStream(SingleOutputStreamOperator<T> mainStream);

    public static DTOStream<DTO> createDispatched(String table) {
        return createDispatched(table, false);
    }

    public static DTOStream<DTO> createDispatched(String table, boolean startNewChain) {
        return new DispatchedDTOStream(DTO.class, new OutputTag<DTO>(table) {
        }, startNewChain);
    }

    public static DTOStream<RowData> createRowData() {
        return new RowDataDTOStream();
    }

    public static DTOStream<RowData> createRowData(DataStream<RowData> stream) {
        return new RowDataDTOStream(stream);
    }

    /**
     * binlog监听，可将同一个Stream中的不同表重新分区，将每个表成为独立的Stream
     */
    public static class DispatchedDTOStream<RECORD_TYPE> extends DTOStream<RECORD_TYPE> {
        public final OutputTag<RECORD_TYPE> outputTag;
        boolean hasGetStream = false;

        private final boolean startNewChain;

        public DispatchedDTOStream(Class<RECORD_TYPE> clazz, OutputTag<RECORD_TYPE> outputTag, boolean startNewChain) {
            super(clazz);
            this.outputTag = outputTag;
            this.startNewChain = startNewChain;
        }

        @Override
        public DataStream<RECORD_TYPE> getStream() {
            DataStream<RECORD_TYPE> stream = super.getStream();
            if (this.startNewChain && !hasGetStream) {
                stream = stream.map(new NoneMapper()).startNewChain();
                hasGetStream = true;
            }
            return stream;
        }

        public DTOStream<RECORD_TYPE> addStream(SingleOutputStreamOperator<RECORD_TYPE> mainStream) {
            if (stream == null) {
                stream = mainStream.getSideOutput(outputTag);
            } else {
                stream = stream.union(mainStream.getSideOutput(outputTag));
            }
            return this;
        }
    }

    public static class NoneMapper<RECORD_TYPE> implements MapFunction<RECORD_TYPE, RECORD_TYPE> {
        @Override
        public RECORD_TYPE map(RECORD_TYPE o) throws Exception {
            // 啥也不做就行了
            return o;
        }
    }


    /**
     * 利用pull的方式拉取增量数据，每个流本来就是独立的不需要分流
     */
    private static class RowDataDTOStream extends DTOStream<RowData> {
        public RowDataDTOStream() {
            super(RowData.class);
        }

        public RowDataDTOStream(DataStream<RowData> stream) {
            super(RowData.class);
            this.stream = stream;
        }

        @Override
        public DTOStream<RowData> addStream(SingleOutputStreamOperator<RowData> mainStream) {
            if (stream == null) {
                stream = mainStream;
            } else {
                stream = stream.union(mainStream);
            }
            return this;
        }
    }
}
