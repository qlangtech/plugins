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
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * <TRANSFER_OBJ/> 可以是用：
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-16 12:32
 * @see com.qlangtech.tis.realtime.transfer.DTO
 **/
public abstract class TabSinkFunc<TRANSFER_OBJ> {
    // private transient final Map<IDataxProcessor.TableAlias, SinkFunction<TRANSFER_OBJ>> sinkFunction;
    // public transient final StreamExecutionEnvironment env;

    private transient final SinkFunction<TRANSFER_OBJ> sinkFunction;
    private transient final IDataxProcessor.TableAlias tab;

    public TabSinkFunc(IDataxProcessor.TableAlias tab, SinkFunction<TRANSFER_OBJ> sinkFunction) {
        this.sinkFunction = sinkFunction;
        this.tab = tab;
        //  this.env = env;
    }

    /**
     * map
     *
     * @param sourceStream
     * @return
     */
    protected abstract DataStream<TRANSFER_OBJ> streamMap(DataStream<DTO> sourceStream);

    public void add2Sink(DataStream<DTO> sourceStream) {

        this.streamMap(sourceStream).addSink(sinkFunction).name(tab.getTo());

//        if (sinkFunction.size() < 2) {
//            for (Map.Entry<IDataxProcessor.TableAlias, SinkFunction<TRANSFER_OBJ>> entry : sinkFunction.entrySet()) {
//                streamMap(sourceStream).addSink(entry.getValue()).name(entry.getKey().getTo());
//            }
//        } else {
//            if (StringUtils.isEmpty(originTableName)) {
//                throw new IllegalArgumentException("param originTableName can not be null");
//            }
//            boolean hasMatch = false;
//            for (Map.Entry<IDataxProcessor.TableAlias, SinkFunction<TRANSFER_OBJ>> entry : sinkFunction.entrySet()) {
//                if (originTableName.equals(entry.getKey().getFrom())) {
//                    streamMap(sourceStream).addSink(entry.getValue()).name(entry.getKey().getTo());
//                    hasMatch = true;
//                    break;
//                }
//            }
//            if (!hasMatch) {
//                throw new IllegalStateException("tabName:" + originTableName + " can not find SINK in :"
//                        + sinkFunction.keySet().stream()
//                        .map((t) -> "(" + t.getFrom() + "," + t.getTo() + ")").collect(Collectors.joining(" ")));
//            }
//        }
    }
}
