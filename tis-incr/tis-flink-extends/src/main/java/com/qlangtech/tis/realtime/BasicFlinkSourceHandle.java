/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.realtime;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//import org.apache.flink.types.Row;

//import com.qlangtech.plugins.incr.flink.cdc.DTOTypeInfo;
//import com.qlangtech.tis.realtime.transfer.DTO;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 14:18
 **/
public abstract class BasicFlinkSourceHandle implements IConsumerHandle<List<ReaderSource>>, Serializable {

    private transient TISSinkFactory sinkFuncFactory;


    @Override
    public final void consume(TargetResName dataxName, AsyncMsg<List<ReaderSource>> asyncMsg, IDataxProcessor dataXProcessor) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (CollectionUtils.isEmpty(asyncMsg.getFocusTabs())) {
            throw new IllegalArgumentException("focusTabs can not be empty");
        }
        //Key
        Map<String, DTOStream> tab2OutputTag
                = asyncMsg.getFocusTabs().stream().collect(Collectors.toMap((tab) -> tab
                , (tab) -> new DTOStream(new OutputTag<DTO>(tab) {
                })));

        List<SingleOutputStreamOperator<DTO>> mainDataStream = Lists.newArrayList();
        for (ReaderSource sourceFunc : asyncMsg.getSource()) {
            mainDataStream.add(getSourceStream(env, tab2OutputTag, sourceFunc));
        }

        for (SingleOutputStreamOperator<DTO> mainStream : mainDataStream) {
            for (Map.Entry<String, DTOStream> e : tab2OutputTag.entrySet()) {
                e.getValue().addStream(mainStream);
            }
        }

        SinkFuncs sinkFunction = new SinkFuncs(this.getSinkFuncFactory().createSinkFunction(dataXProcessor));

        Map<String, DataStream<DTO>> streamMap = Maps.newHashMap();
        for (Map.Entry<String, DTOStream> e : tab2OutputTag.entrySet()) {
            streamMap.put(e.getKey(), e.getValue().getStream());
        }

        processTableStream(streamMap, sinkFunction);

        //
        env.execute(dataxName.getName());
    }

    /**
     * 处理各个表对应的数据流
     *
     * @param
     */
    protected abstract void processTableStream(Map<String, DataStream<DTO>> streamMap, SinkFuncs sinkFunction);


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
