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

package com.qlangtech.tis.plugin.kafka.consumer;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.plugin.datax.kafka.reader.DataXKafkaReader;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.realtime.DTOSourceTagProcessFunction;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.ReaderSource.SideOutputReaderSource;
import com.qlangtech.tis.realtime.SourceProcessFunction;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * reference: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-27
 */
public class FlinkKafkaFunction implements IMQListener<JobExecutionResult> {

    private final KafkaMQListenerFactory sourceFactory;


    public FlinkKafkaFunction(KafkaMQListenerFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }

    @Override
    public IConsumerHandle getConsumerHandle() {
        return this.sourceFactory.getConsumerHander();
    }

    @Override
    public JobExecutionResult start(TargetResName dataxName, IDataxReader dataSource
            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
//        try {
//            Objects.requireNonNull(dataXProcessor, "param dataXProcessor can not be null");
        DataXKafkaReader kafkaReader = (DataXKafkaReader) dataSource;
//            BasicDataSourceFactory dsFactory = (BasicDataSourceFactory) rdbmsReader.getDataSourceFactory();
        //  Map<String, DeserializationSchema<RowData>> tabColsMapper = Maps.newHashMap();
//            TableInDB tablesInDB = dsFactory.getTablesInDB();
        IFlinkColCreator<FlinkCol> flinkColCreator = sourceFactory.createFlinkColCreator(kafkaReader);
//            IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getName());
//        DataType physicalDataType = null;
//        DeserializationSchema<RowData> deserializeSchema = null;
//        List<DataTypes.Field> fields = null;
        // for (ISelectedTab tab : tabs) {
//            FlinkColMapper colsMapper
//                    = AbstractRowDataMapper.getAllTabColsMetaMapper(tab.getCols(), flinkColCreator);
//            fields = colsMapper.getColMapper().entrySet().stream()
//                    .map((entry) -> FIELD(entry.getKey(), entry.getValue().type)).collect(Collectors.toList());
//            // tabColsMapper.put(tab.getName(), colsMapper);
//            physicalDataType = ROW(fields);
//            deserializeSchema
//                    = kafkaReader.format.createDecodingFormat(tab.getName())
//                    .createRuntimeDecoder(null, physicalDataType);
//
//            tabColsMapper.put(tab.getName(), Objects.requireNonNull(deserializeSchema, "deserializeSchema can not be null"));
        //}
        IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getName());
        Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper
                = RecordTransformerRules.contextParamValsGetterMapper(
                         pluginContext.getCollectionName(), pluginContext, kafkaReader, tabs);

        KafkaSourceBuilder<DTO> kafkaSourceBuilder = kafkaReader.createKafkaSourceBuilder(contextParamValsGetterMapper);
//        kafkaSourceBuilder.setValueOnlyDeserializer(
//                new KafkaDTODeserializationSchema(kafkaReader.format, contextParamValsGetterMapper));
//        KafkaSourceBuilder<DTO> kafkaSourceBuilder
//                = KafkaSource.<DTO>builder()
//                .setProperties(kafkaReader.buildKafkaProperties())
//                .setValueOnlyDeserializer(new KafkaDTODeserializationSchema(kafkaReader.format, contextParamValsGetterMapper));

        Objects.requireNonNull(sourceFactory.startOffset, "startOffset can not be null")
                .setOffset(kafkaSourceBuilder);
        kafkaReader.subscription.setSubscription(kafkaSourceBuilder);

        KafkaSource<DTO> source = kafkaSourceBuilder.build();
        try {
            SourceChannel sourceChannel = new SourceChannel(
                    createKafkaSource(kafkaReader.bootstrapServers, source));

            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null)
                    , (tabName) -> createDispatched(tabName, sourceFactory.independentBinLogMonitor));

            return (JobExecutionResult) this.getConsumerHandle().consume(dataxName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DTOStream<DTO> createDispatched(String table, boolean startNewChain) {
        return new KafkaDispatchedDTOStream(table, startNewChain);
    }

    public static ReaderSource<DTO> createKafkaSource(String tokenName, Source<DTO, ?, ?> sourceFunc) {
        return new SideOutputReaderSource<DTO>(tokenName) {
            @Override
            protected DataStreamSource<DTO> addAsSource(StreamExecutionEnvironment env) {
                return env.fromSource(sourceFunc, WatermarkStrategy.noWatermarks(), tokenName);
            }

            @Override
            protected SourceProcessFunction<DTO> createStreamTagFunction(Map<String, OutputTag<DTO>> tab2OutputTag) {
                TreeMap<String, OutputTag<DTO>> caseInsensitiveMapper = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
                caseInsensitiveMapper.putAll(tab2OutputTag);
                return new DTOSourceTagProcessFunction(caseInsensitiveMapper);
            }
        };
    }


}
