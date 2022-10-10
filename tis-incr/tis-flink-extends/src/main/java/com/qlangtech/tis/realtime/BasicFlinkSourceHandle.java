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

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.Tab2OutputTag;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.extension.TISExtensible;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 14:18
 **/
@Public
@TISExtensible
public abstract class BasicFlinkSourceHandle<SINK_TRANSFER_OBJ> implements IConsumerHandle<List<ReaderSource>, JobExecutionResult>, Serializable {

    private transient TISSinkFactory sinkFuncFactory;
    private transient IncrStreamFactory streamFactory;

    protected String getDataXName() {
        String name = this.sinkFuncFactory.dataXName;
        if (StringUtils.isEmpty(name)) {
            throw new IllegalStateException("dataXName can not be empty");
        }
        return name;
    }

    public static IStreamTableCreator.IStreamTableMeta getStreamTableMeta(TargetResName dataxName, String tabName) {
        TISSinkFactory sinKFactory = TISSinkFactory.getIncrSinKFactory(dataxName.getName());

//        if (!(sinKFactory instanceof IStreamTableCreator)) {
//            throw new IllegalStateException("writer:"
//                    + sinKFactory.getClass().getName() + " must be type of " + IStreamTableCreator.class.getSimpleName());
//        }
//        return ((IStreamTableCreator) sinKFactory).getStreamTableMeta(tabName);
        return getStreamTableMeta(sinKFactory, tabName);
    }

    public static IStreamTableCreator.IStreamTableMeta getStreamTableMeta(TISSinkFactory sinKFactory, String tabName) {
//        TISSinkFactory sinKFactory = TISSinkFactory.getIncrSinKFactory(dataxName.getName());

        if (!(sinKFactory instanceof IStreamTableCreator)) {
            throw new IllegalStateException("writer:"
                    + sinKFactory.getClass().getName() + " must be type of " + IStreamTableCreator.class.getSimpleName());
        }
        return ((IStreamTableCreator) sinKFactory).getStreamTableMeta(tabName);
    }


    @Override
    public JobExecutionResult consume(TargetResName dataxName, AsyncMsg<List<ReaderSource>> asyncMsg
            , IDataxProcessor dataXProcessor) throws Exception {
        StreamExecutionEnvironment env = getFlinkExecutionEnvironment();

        if (CollectionUtils.isEmpty(asyncMsg.getFocusTabs())) {
            throw new IllegalArgumentException("focusTabs can not be empty");
        }
        //dataXProcessor.getTabAlias()
        //Key
        Tab2OutputTag<DTOStream> tab2OutputTag = createTab2OutputTag(asyncMsg, env, dataxName);

        Map<TableAlias, TabSinkFunc<SINK_TRANSFER_OBJ>> sinks
                = this.getSinkFuncFactory().createSinkFunction(dataXProcessor);
        sinks.forEach((tab, func) -> {
            if (StringUtils.isEmpty(tab.getTo()) || StringUtils.isEmpty(tab.getFrom())) {
                throw new IllegalArgumentException("tab of "
                        + this.getSinkFuncFactory().getDescriptor().getDisplayName() + ":" + tab + " is illegal");
            }
        });


        this.processTableStream(env, tab2OutputTag, new SinkFuncs(sinks));
        return executeFlinkJob(dataxName, env);
    }

    /**
     * 处理各个表对应的数据流
     *
     * @param
     */
    protected abstract void processTableStream(StreamExecutionEnvironment env
            , Tab2OutputTag<DTOStream> tab2OutputTag, SinkFuncs<SINK_TRANSFER_OBJ> sinkFunction);


    private Tab2OutputTag<DTOStream> createTab2OutputTag(
            AsyncMsg<List<ReaderSource>> asyncMsg, StreamExecutionEnvironment env, TargetResName dataxName) throws java.io.IOException {
        Tab2OutputTag<DTOStream> tab2OutputTag = asyncMsg.getTab2OutputTag();
        for (ReaderSource sourceFunc : asyncMsg.getSource()) {

            sourceFunc.getSourceStream(env, tab2OutputTag);
        }
        return tab2OutputTag;
    }

    protected List<FlinkCol> getTabColMetas(TargetResName dataxName, String tabName) {
        return Collections.emptyList();
    }

    protected StreamExecutionEnvironment getFlinkExecutionEnvironment() {
        return streamFactory.createStreamExecutionEnvironment();
    }

    protected JobExecutionResult executeFlinkJob(TargetResName dataxName, StreamExecutionEnvironment env) throws Exception {
        return env.execute(dataxName.getName());
    }


//    private SingleOutputStreamOperator<DTO> getSourceStream(
//            StreamExecutionEnvironment env, Map<String, DTOStream> tab2OutputStream, ReaderSource sourceFunc) {
//
//
//
//
//        return env.addSource(sourceFunc.sourceFunc)
//                .name(sourceFunc.tokenName)
//                .setParallelism(1)
//                .process(new SourceProcessFunction(tab2OutputStream.entrySet().stream()
//                        .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue().outputTag))));
//    }


    public TISSinkFactory getSinkFuncFactory() {
        return this.sinkFuncFactory;
    }

    public void setSinkFuncFactory(TISSinkFactory sinkFuncFactory) {
        if (sinkFuncFactory == null) {
            throw new IllegalArgumentException("param sinkFuncFactory can not be null");
        }
        this.sinkFuncFactory = sinkFuncFactory;
    }

    public void setStreamFactory(IncrStreamFactory streamFactory) {
        this.streamFactory = Objects.requireNonNull(streamFactory, "streamFactory can not be null");
    }
}
