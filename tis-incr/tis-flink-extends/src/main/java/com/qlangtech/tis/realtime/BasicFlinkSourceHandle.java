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

import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 14:18
 **/
public abstract class BasicFlinkSourceHandle implements IConsumerHandle<List<ReaderSource>, JobExecutionResult>, Serializable {

    private transient TISSinkFactory sinkFuncFactory;


    @Override
    public final JobExecutionResult consume(TargetResName dataxName, AsyncMsg<List<ReaderSource>> asyncMsg
            , IDataxProcessor dataXProcessor) throws Exception {
        StreamExecutionEnvironment env = getFlinkExecutionEnvironment();

        if (CollectionUtils.isEmpty(asyncMsg.getFocusTabs())) {
            throw new IllegalArgumentException("focusTabs can not be empty");
        }
        //Key
        Map<String, DTOStream> tab2OutputTag = createTab2OutputTag(asyncMsg, env, dataXProcessor);

        Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> sinks
                = this.getSinkFuncFactory().createSinkFunction(dataXProcessor);
        sinks.forEach((tab, func) -> {
            if (StringUtils.isEmpty(tab.getTo()) || StringUtils.isEmpty(tab.getFrom())) {
                throw new IllegalArgumentException("tab of "
                        + this.getSinkFuncFactory().getDescriptor().getDisplayName() + ":" + tab + " is illegal");
            }
        });

        SinkFuncs sinkFunction = new SinkFuncs(env, sinks);

        this.processTableStream(env, tab2OutputTag, sinkFunction);
        return executeFlinkJob(dataxName, env);
    }

    /**
     * 处理各个表对应的数据流
     *
     * @param
     */
    protected abstract void processTableStream(StreamExecutionEnvironment env
            , Map<String, DTOStream> tab2OutputTag, SinkFuncs sinkFunction);


    private Map<String, DTOStream> createTab2OutputTag(
            AsyncMsg<List<ReaderSource>> asyncMsg, StreamExecutionEnvironment env, IDataxProcessor dataXProcessor) throws java.io.IOException {
        Map<String, DTOStream> tab2OutputTag
                = asyncMsg.getFocusTabs().stream().collect(Collectors.toMap((tab) -> tab
                , (tab) -> new DTOStream(new OutputTag<DTO>(tab) {
                }, getTabColMetas(dataXProcessor, tab))));

        List<SingleOutputStreamOperator<DTO>> mainDataStream = Lists.newArrayList();
        for (ReaderSource sourceFunc : asyncMsg.getSource()) {
            mainDataStream.add(getSourceStream(env, tab2OutputTag, sourceFunc));
        }

        for (SingleOutputStreamOperator<DTO> mainStream : mainDataStream) {
            for (Map.Entry<String, DTOStream> e : tab2OutputTag.entrySet()) {
                e.getValue().addStream(mainStream);
            }
        }
        return tab2OutputTag;
    }

    protected List<FlinkCol> getTabColMetas(IDataxProcessor dataXProcessor, String tabName) {
        return Collections.emptyList();
    }

    protected StreamExecutionEnvironment getFlinkExecutionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    protected JobExecutionResult executeFlinkJob(TargetResName dataxName, StreamExecutionEnvironment env) throws Exception {
        return env.execute(dataxName.getName());
    }


    private SingleOutputStreamOperator<DTO> getSourceStream(
            StreamExecutionEnvironment env, Map<String, DTOStream> tab2OutputStream, ReaderSource sourceFunc) {

        return env.addSource(sourceFunc.sourceFunc)
                .name(sourceFunc.tokenName)
                .setParallelism(1)
                .process(new SourceProcessFunction(tab2OutputStream.entrySet().stream()
                        .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue().outputTag))));
    }


    public TISSinkFactory getSinkFuncFactory() {
        return sinkFuncFactory;
    }

    public void setSinkFuncFactory(TISSinkFactory sinkFuncFactory) {
        if (sinkFuncFactory == null) {
            throw new IllegalArgumentException("param sinkFuncFactory can not be null");
        }
        this.sinkFuncFactory = sinkFuncFactory;
    }


}
