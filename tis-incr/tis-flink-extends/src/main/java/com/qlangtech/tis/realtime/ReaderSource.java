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
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.dto.DTOStream.DispatchedDTOStream;
import com.qlangtech.tis.realtime.source.HttpSource;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.yarn.rpc.IncrRateControllerCfgDTO;
import com.qlangtech.tis.realtime.yarn.rpc.IncrRateControllerCfgDTO.RateControllerType;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-15 18:03
 **/
public abstract class ReaderSource<T> {
    public final String tokenName;
    private final DataXName dataXName;
    protected final IncrStreamFactory streamFactory;

    private ReaderSource(IncrStreamFactory streamFactory, DataXName dataXName, String tokenName) {
        if (StringUtils.isEmpty(tokenName)) {
            throw new IllegalArgumentException("param tokenName can not be empty");
        }
        this.tokenName = tokenName;
        this.dataXName = Objects.requireNonNull(dataXName, "dataXName can not be null");
        this.streamFactory = Objects.requireNonNull(streamFactory, "streamFactory can not be null");
    }


//    protected void afterSourceStreamGetter(Tab2OutputTag<DTOStream<T>> tab2OutputStream, SingleOutputStreamOperator<T> operator) {
//        //return operator;
//    }

    protected void afterSourceStreamGetter(Tab2OutputTag<DTOStream<T>> tab2OutputStream
            , BroadcastConnectedStream<T, IncrRateControllerCfgDTO> sourceConnect) {
        //return operator;
    }

    public void getSourceStream(
            StreamExecutionEnvironment env, Tab2OutputTag<DTOStream<T>> tab2OutputStream) {

        SingleOutputStreamOperator<T> operator = addAsSource(env)
                .name(this.tokenName)
                .setParallelism(1);


        boolean supportRateLimiter = this.streamFactory.supportRateLimiter();

        DataStreamSource<IncrRateControllerCfgDTO> rateCfgSource
                = env.fromSource(supportRateLimiter ? new HttpSource(this.dataXName) : new DataGeneratorSource<>(
                new EmptyGeneratorFunction(),
                0,
                TypeInformation.of(IncrRateControllerCfgDTO.class)
        ), WatermarkStrategy.noWatermarks(), "rateCfgSource");
        MapStateDescriptor<String, Double> broadcastStateDesc
                = new MapStateDescriptor<>("broadcastState", String.class, Double.class);
        BroadcastStream<IncrRateControllerCfgDTO> broadcast = rateCfgSource.broadcast(broadcastStateDesc);

        BroadcastConnectedStream<T, IncrRateControllerCfgDTO> sourceConnect = operator.connect(broadcast);

        afterSourceStreamGetter(tab2OutputStream, sourceConnect);
    }

    private static class EmptyGeneratorFunction implements GeneratorFunction<Long, IncrRateControllerCfgDTO> {
        @Override
        public IncrRateControllerCfgDTO map(Long aLong) throws Exception {
            IncrRateControllerCfgDTO mockRateControllerCfg = new IncrRateControllerCfgDTO();
            mockRateControllerCfg.setControllerType(RateControllerType.SkipProcess);
            //mockRateControllerCfg.setPause(false);
            mockRateControllerCfg.setLastModified(System.currentTimeMillis());
            return mockRateControllerCfg;
        }
    }

    protected abstract DataStreamSource<T> addAsSource(StreamExecutionEnvironment env);


//    public static ReaderSource<DTO> createDTOSource(IncrStreamFactory streamFactory, DataXName dataXName, String tokenName, SourceFunction<DTO> sourceFunc) {
//        return new SideOutputReaderSource<DTO>(streamFactory, dataXName, tokenName) {
//            @Override
//            protected DataStreamSource<DTO> addAsSource(StreamExecutionEnvironment env) {
//                return env.addSource(sourceFunc, TypeInformation.of(DTO.class));
//            }
//
//            @Override
//            protected SourceProcessFunction<DTO> createStreamTagFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
//                return new DTOSourceTagProcessFunction(dataXName, tab2OutputTag);
//            }
//        };
//    }


    public static ReaderSource<DTO> createDTOSource(IncrStreamFactory streamFactory, DataXName dataXName, String tokenName
            , boolean flinkCDCPipelineEnable, Source<DTO, ?, ?> sourceFunc) {

        return new SideOutputReaderSource<DTO>(streamFactory, dataXName, tokenName) {
            @Override
            protected DataStreamSource<DTO> addAsSource(StreamExecutionEnvironment env) {
                return env.fromSource(streamFactory.supportRateLimiter()
                                ? new RateLimitedSourceWrapper(dataXName, sourceFunc, streamFactory.getRateLimiterStrategy())
                                : sourceFunc
                        , WatermarkStrategy.noWatermarks(), tokenName, TypeInformation.of(DTO.class));
            }

            @Override
            protected SourceProcessFunction<DTO> createStreamTagFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
                return DTOSourceTagProcessFunction.create(dataXName, flinkCDCPipelineEnable, tab2OutputTag);// : new DTOSourceTagProcessFunction(tab2OutputTag);
            }
        };
    }

    public static ReaderSource<DTO> createDTOSource(IncrStreamFactory streamFactory, DataXName dataXName, String tokenName
            , boolean flinkCDCPipelineEnable, final DataStreamSource<DTO> source) {
        return new SideOutputReaderSource<DTO>(streamFactory, dataXName, tokenName) {
            @Override
            protected DataStreamSource<DTO> addAsSource(StreamExecutionEnvironment env) {
                return source;
            }

            @Override
            protected SourceProcessFunction<DTO> createStreamTagFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
                return DTOSourceTagProcessFunction.create(dataXName, flinkCDCPipelineEnable, tab2OutputTag);// new DTOSourceTagProcessFunction(tab2OutputTag);
            }
        };
    }


    public static abstract class SideOutputReaderSource<RECORD_TYPE> extends ReaderSource<RECORD_TYPE> {
        /**
         * DTO.class
         *
         * @param tokenName
         */
        public SideOutputReaderSource(IncrStreamFactory streamFactory, DataXName dataXName, String tokenName) {
            super(streamFactory, dataXName, tokenName);
        }

        /**
         * 创建为流标记的算子
         *
         * @return
         */
        protected abstract SourceProcessFunction<RECORD_TYPE> createStreamTagFunction(Map<String, OutputTag<RECORD_TYPE>> tab2OutputTag);


        @Override
        protected final void afterSourceStreamGetter(
                Tab2OutputTag<DTOStream<RECORD_TYPE>> tab2OutputStream, BroadcastConnectedStream<RECORD_TYPE, IncrRateControllerCfgDTO> sourceConnect
                //        SingleOutputStreamOperator<RECORD_TYPE> operator
        ) {

            Map<String, OutputTag<RECORD_TYPE>> tab2OutputTag
                    = tab2OutputStream.createTab2OutputTag((dtoStream) -> ((DispatchedDTOStream) dtoStream).outputTag);

            /**
             * 为主事件流打上分支流标记
             */
            SingleOutputStreamOperator<RECORD_TYPE> mainStream
                    = sourceConnect.process(createStreamTagFunction(tab2OutputTag));


            /**
             * 利用标记从主事件流中分叉出子事件流
             */
            for (Map.Entry<TableAlias, DTOStream<RECORD_TYPE>> e : tab2OutputStream.entrySet()) {
                e.getValue().addStream(mainStream);
            }
        }
    }


    public static ReaderSource<RowData> createRowDataSource(
            IncrStreamFactory streamFactory, DataXName dataXName, String tokenName, ISelectedTab tab, SourceFunction<RowData> sourceFunc) {
        return new RowDataOutputReaderSource(streamFactory, dataXName, tokenName, tab) {
            @Override
            protected DataStreamSource<RowData> addAsSource(StreamExecutionEnvironment env) {
                return env.addSource(sourceFunc);
            }
        };
    }

    public static ReaderSource<RowData> createRowDataSource(
            IncrStreamFactory streamFactory, DataXName dataXName, String tokenName, ISelectedTab tab, DataStreamSource<RowData> streamSource) {
        return new RowDataOutputReaderSource(streamFactory, dataXName, tokenName, tab) {
            @Override
            protected DataStreamSource<RowData> addAsSource(StreamExecutionEnvironment env) {
                return streamSource;
            }
        };
    }

    private static abstract class RowDataOutputReaderSource extends ReaderSource<RowData> {
        private ISelectedTab tab;

        public RowDataOutputReaderSource(IncrStreamFactory streamFactory, DataXName dataXName, String tokenName, ISelectedTab tab) {
            super(streamFactory, dataXName, tokenName);
            this.tab = tab;
        }

        @Override
        protected final void afterSourceStreamGetter(
                Tab2OutputTag<DTOStream<RowData>> tab2OutputStream, BroadcastConnectedStream<RowData, IncrRateControllerCfgDTO> sourceConnect
                //        SingleOutputStreamOperator<RowData> operator
        ) {
            throw new UnsupportedOperationException();
//            DTOStream dtoStream = tab2OutputStream.get(tab);
//            dtoStream.addStream(operator);
            // return operator;
        }
    }
}
