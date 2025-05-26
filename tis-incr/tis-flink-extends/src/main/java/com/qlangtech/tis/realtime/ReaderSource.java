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

import com.qlangtech.tis.async.message.client.consumer.Tab2OutputTag;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.dto.DTOStream.DispatchedDTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-15 18:03
 **/
public abstract class ReaderSource<T> {
    public final String tokenName;

    private ReaderSource(String tokenName) {
        if (StringUtils.isEmpty(tokenName)) {
            throw new IllegalArgumentException("param tokenName can not be empty");
        }
        this.tokenName = tokenName;
    }

    protected void afterSourceStreamGetter(Tab2OutputTag<DTOStream<T>> tab2OutputStream, SingleOutputStreamOperator<T> operator) {
        //return operator;
    }

    public void getSourceStream(
            StreamExecutionEnvironment env, Tab2OutputTag<DTOStream<T>> tab2OutputStream) {

        SingleOutputStreamOperator<T> operator = addAsSource(env)
                .name(this.tokenName)
                .setParallelism(1);

        afterSourceStreamGetter(tab2OutputStream, operator);
    }

    protected abstract DataStreamSource<T> addAsSource(StreamExecutionEnvironment env);


    public static ReaderSource<DTO> createDTOSource(String tokenName, SourceFunction<DTO> sourceFunc) {
        return new SideOutputReaderSource<DTO>(tokenName) {
            @Override
            protected DataStreamSource<DTO> addAsSource(StreamExecutionEnvironment env) {
                return env.addSource(sourceFunc);
            }

            @Override
            protected SourceProcessFunction<DTO> createStreamTagFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
                return new DTOSourceTagProcessFunction(tab2OutputTag);
            }
        };
    }


    public static ReaderSource<DTO> createDTOSource(String tokenName, boolean flinkCDCPipelineEnable, Source<DTO, ?, ?> sourceFunc) {
        return new SideOutputReaderSource<DTO>(tokenName) {
            @Override
            protected DataStreamSource<DTO> addAsSource(StreamExecutionEnvironment env) {
                return env.fromSource(sourceFunc, WatermarkStrategy.noWatermarks(), tokenName);
            }

            @Override
            protected SourceProcessFunction<DTO> createStreamTagFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
                return flinkCDCPipelineEnable ? DTOSourceTagProcessFunction.createMergeAllTabsInOneBus() : new DTOSourceTagProcessFunction(tab2OutputTag);
            }
        };
    }

    public static ReaderSource<DTO> createDTOSource(String tokenName, final DataStreamSource<DTO> source) {
        return new SideOutputReaderSource<DTO>(tokenName) {
            @Override
            protected DataStreamSource<DTO> addAsSource(StreamExecutionEnvironment env) {
                return source;
            }

            @Override
            protected SourceProcessFunction<DTO> createStreamTagFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
                return new DTOSourceTagProcessFunction(tab2OutputTag);
            }
        };
    }


    public static abstract class SideOutputReaderSource<RECORD_TYPE> extends ReaderSource<RECORD_TYPE> {
        /**
         * DTO.class
         *
         * @param tokenName
         */
        public SideOutputReaderSource(String tokenName) {
            super(tokenName);
        }

        /**
         * 创建为流标记的算子
         *
         * @return
         */
        protected abstract SourceProcessFunction<RECORD_TYPE> createStreamTagFunction(Map<String, OutputTag<RECORD_TYPE>> tab2OutputTag);


        @Override
        protected final void afterSourceStreamGetter(
                Tab2OutputTag<DTOStream<RECORD_TYPE>> tab2OutputStream, SingleOutputStreamOperator<RECORD_TYPE> operator) {

            Map<String, OutputTag<RECORD_TYPE>> tab2OutputTag
                    = tab2OutputStream.createTab2OutputTag((dtoStream) -> ((DispatchedDTOStream) dtoStream).outputTag);

            /**
             * 为主事件流打上分支流标记
             */
            SingleOutputStreamOperator<RECORD_TYPE> mainStream
                    = operator.process(createStreamTagFunction(tab2OutputTag));


            /**
             * 利用标记从主事件流中分叉出子事件流
             */
            for (Map.Entry<TableAlias, DTOStream<RECORD_TYPE>> e : tab2OutputStream.entrySet()) {
                e.getValue().addStream(mainStream);
            }
        }
    }


    public static ReaderSource<RowData> createRowDataSource(String tokenName, ISelectedTab tab, SourceFunction<RowData> sourceFunc) {
        return new RowDataOutputReaderSource(tokenName, tab) {
            @Override
            protected DataStreamSource<RowData> addAsSource(StreamExecutionEnvironment env) {
                return env.addSource(sourceFunc);
            }
        };
    }

    public static ReaderSource<RowData> createRowDataSource(String tokenName, ISelectedTab tab, DataStreamSource<RowData> streamSource) {
        return new RowDataOutputReaderSource(tokenName, tab) {
            @Override
            protected DataStreamSource<RowData> addAsSource(StreamExecutionEnvironment env) {
                return streamSource;
            }
        };
    }

    private static abstract class RowDataOutputReaderSource extends ReaderSource<RowData> {
        private ISelectedTab tab;

        public RowDataOutputReaderSource(String tokenName, ISelectedTab tab) {
            super(tokenName);
            this.tab = tab;
        }

        @Override
        protected final void afterSourceStreamGetter(
                Tab2OutputTag<DTOStream<RowData>> tab2OutputStream, SingleOutputStreamOperator<RowData> operator) {
            DTOStream dtoStream = tab2OutputStream.get(tab);
            dtoStream.addStream(operator);
            // return operator;
        }
    }
}
