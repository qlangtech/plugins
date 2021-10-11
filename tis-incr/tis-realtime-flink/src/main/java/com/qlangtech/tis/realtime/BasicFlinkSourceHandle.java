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
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
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
public abstract class BasicFlinkSourceHandle implements IConsumerHandle<List<SourceFunction<DTO>>>, Serializable {

    private transient TISSinkFactory sinkFuncFactory;


    @Override
    public final void consume(AsyncMsg<List<SourceFunction<DTO>>> asyncMsg, IDataxProcessor dataXProcessor) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (CollectionUtils.isEmpty(asyncMsg.getFocusTabs())) {
            throw new IllegalArgumentException("focusTabs can not be empty");
        }

//        String[] fieldNames = new String[]{DTOTypeInfo.KEY_FIELD_DB_NAME, DTOTypeInfo.KEY_FIELD_TABLE_NAME, "waitingorder_id", "order_id", "entity_id", "is_valid", "last_ver"};
//        TypeInformation<?>[] types = new TypeInformation<?>[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.INT};

        //
        Map<String, DTOStream> tab2OutputTag
                = asyncMsg.getFocusTabs().stream().collect(Collectors.toMap((tab) -> tab
                , (tab) -> new DTOStream(new OutputTag<DTO>(tab) {
                })));

        List<SingleOutputStreamOperator<DTO>> mainDataStream = Lists.newArrayList();
        for (SourceFunction<DTO> sourceFunc : asyncMsg.getSource()) {
            mainDataStream.add(getSourceStream(env, tab2OutputTag, sourceFunc));
        }

        for (SingleOutputStreamOperator<DTO> mainStream : mainDataStream) {
            for (Map.Entry<String, DTOStream> e : tab2OutputTag.entrySet()) {
                e.getValue().addStream(mainStream);
            }
        }

        SinkFunction<DTO> sinkFunction = this.getSinkFuncFactory().createSinkFunction(dataXProcessor);

        Map<String, DataStream<DTO>> streamMap = Maps.newHashMap();
        for (Map.Entry<String, DTOStream> e : tab2OutputTag.entrySet()) {
            streamMap.put(e.getKey(), e.getValue().getStream());
        }

        processTableStream(streamMap, sinkFunction);

        //
        env.execute("rocketmq-flink-example");
    }

    /**
     * 处理各个表对应的数据流
     *
     * @param
     */
    protected abstract void processTableStream(Map<String, DataStream<DTO>> streamMap, SinkFunction<DTO> sinkFunction);

    protected static class DTOStream implements Serializable {
        private final OutputTag<DTO> outputTag;
        private transient DataStream<DTO> stream;

        public DTOStream(OutputTag<DTO> outputTag) {
            this.outputTag = outputTag;
        }

        public DataStream<DTO> getStream() {
            return this.stream;
        }

        private void addStream(SingleOutputStreamOperator<DTO> mainStream) {
            if (stream == null) {
                stream = mainStream.getSideOutput(outputTag);
            } else {
                stream = stream.union(mainStream.getSideOutput(outputTag));
            }
        }
    }

    private SingleOutputStreamOperator<DTO> getSourceStream(
            StreamExecutionEnvironment env, Map<String, DTOStream> tab2OutputTag, SourceFunction<DTO> sourceFunc) {
        return env.addSource(sourceFunc)
                .name("rocketmq-source")
                .setParallelism(1)
                .process(new ProcessFunction<DTO, DTO>() {
                    @Override
                    public void processElement(DTO in, Context ctx, Collector<DTO> out) throws Exception {
                        //side_output: https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/side_output.html
                        final String tabName = in.getTableName();// String.valueOf(in.getAfter().get(DTOTypeInfo.KEY_FIELD_TABLE_NAME));
                        DTOStream outputTag = tab2OutputTag.get(tabName);
                        if (outputTag == null) {
                            throw new IllegalStateException("target table:" + tabName + " can not find relevant in tab2OutputTag");
                        }
                        ctx.output(outputTag.outputTag, in);
                    }
                });
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
