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

import com.qlangtech.plugins.incr.flink.cdc.DTO2RowDataMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.StreamerUtil;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-23 11:18
 **/
public abstract class HoodieFlinkSourceHandle extends BasicFlinkSourceHandle {
    @Override
    protected void processTableStream(StreamExecutionEnvironment env
            , Map<String, DTOStream> tab2OutputTag, SinkFuncs sinkFunction) {
        Map<String, Callable<Object>> tabStreamerCfg = createTabStreamerCfg();
        try {
            for (Map.Entry<String, DTOStream> entry : tab2OutputTag.entrySet()) {
                this.registerTable(env, Objects.requireNonNull(tabStreamerCfg.get(entry.getKey())
                        , "tab:" + entry.getKey() + " relevant instance of 'FlinkStreamerConfig' can not be null")
                        , entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //FlinkStreamerConfig
    protected abstract Map<String, Callable<Object>> createTabStreamerCfg();

    private void registerTable(StreamExecutionEnvironment env, Callable<Object> tabStreamerCfg
            , String tabName, DTOStream dtoDataStream) throws Exception {
        int parallelism = env.getParallelism();

        final org.apache.hudi.streamer.FlinkStreamerConfig cfg = (org.apache.hudi.streamer.FlinkStreamerConfig)tabStreamerCfg.call();

        RowType rowType =
                (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(cfg))
                        .getLogicalType();

        DataStream<RowData> dataStream = dtoDataStream.getStream()
                .map(new DTO2RowDataMapper(dtoDataStream.cols), InternalTypeInfo.of(rowType))
                .name(tabName).uid("uid_" + tabName);


        if (cfg.transformerClassNames != null && !cfg.transformerClassNames.isEmpty()) {
            Option<Transformer> transformer = StreamerUtil.createTransformer(cfg.transformerClassNames);
            if (transformer.isPresent()) {
                dataStream = transformer.get().apply(dataStream);
            }
        }

        Configuration conf = org.apache.hudi.streamer.FlinkStreamerConfig.toFlinkConfig(cfg);
        long ckpTimeout = env.getCheckpointConfig().getCheckpointTimeout();
        conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);


        DataStream<HoodieRecord> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, parallelism, dataStream);
        DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, parallelism, hoodieRecordDataStream);
        if (StreamerUtil.needsAsyncCompaction(conf)) {
            Pipelines.compact(conf, pipeline);
        } else {
            Pipelines.clean(conf, pipeline);
        }

    }


}
