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

import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-15 18:03
 **/
public class ReaderSource<T> {
    public final SourceFunction<T> sourceFunc;
    public final String tokenName;

    public final Class<T> rowType;

    private ReaderSource(String tokenName, SourceFunction<T> sourceFunc, Class<T> rowType) {
        if (StringUtils.isEmpty(tokenName)) {
            throw new IllegalArgumentException("param tokenName can not be empty");
        }
        this.sourceFunc = sourceFunc;
        this.tokenName = tokenName;
        this.rowType = rowType;
    }

    protected SingleOutputStreamOperator<T> afterSourceStreamGetter(Map<String, DTOStream> tab2OutputStream, SingleOutputStreamOperator<T> operator) {
        return operator;
    }

    public SingleOutputStreamOperator<T> getSourceStream(
            StreamExecutionEnvironment env, Map<String, DTOStream> tab2OutputStream) {

        SingleOutputStreamOperator<T> operator = addAsSource(env)
                .name(this.tokenName)
                .setParallelism(1);

        return afterSourceStreamGetter(tab2OutputStream, operator);

//        if (rowType == DTO.class) {
//
//            return (SingleOutputStreamOperator<T>) (((SingleOutputStreamOperator<DTO>) operator)
//                    .process(new SourceProcessFunction(tab2OutputStream.entrySet().stream()
//                    .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue().outputTag)))));
//
//        } else if (rowType == RowData.class) {
//            return operator;
//        } else {
//
//        }
//        return env.addSource(this.sourceFunc)
//                .name(this.tokenName)
//                .setParallelism(1)
//                .;
    }

    protected DataStreamSource<T> addAsSource(StreamExecutionEnvironment env) {
        return env.addSource(this.sourceFunc);
    }


    public static ReaderSource<DTO> createDTOSource(String tokenName, SourceFunction<DTO> sourceFunc) {
        return new ReaderSource<DTO>(tokenName, sourceFunc, DTO.class) {
            @Override
            protected SingleOutputStreamOperator<DTO> afterSourceStreamGetter(
                    Map<String, DTOStream> tab2OutputStream, SingleOutputStreamOperator<DTO> operator) {
                SingleOutputStreamOperator<DTO> mainStream
                        = operator.process(new SourceProcessFunction(tab2OutputStream.entrySet().stream()
                        .collect(Collectors.toMap((e) -> e.getKey(), (e) -> ((DTOStream.DispatchedDTOStream) e.getValue()).outputTag))));
                for (Map.Entry<String, DTOStream> e : tab2OutputStream.entrySet()) {
                    e.getValue().addStream(mainStream);
                }
                return mainStream;
            }
        };
    }

    public static ReaderSource<RowData> createRowDataSource(String tokenName, ISelectedTab tab, SourceFunction<RowData> sourceFunc) {
        return new ReaderSource<RowData>(tokenName, sourceFunc, RowData.class) {
            @Override
            protected SingleOutputStreamOperator<RowData> afterSourceStreamGetter(
                    Map<String, DTOStream> tab2OutputStream, SingleOutputStreamOperator<RowData> operator) {
                DTOStream dtoStream = tab2OutputStream.get(tab.getName());
                dtoStream.addStream(operator);
                return operator;
            }
        };
    }
}
