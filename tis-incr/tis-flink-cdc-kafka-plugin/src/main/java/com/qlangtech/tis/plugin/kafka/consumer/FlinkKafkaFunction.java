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
import com.qlangtech.tis.plugin.datax.kafka.reader.DataXKafkaReader;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.realtime.KafkaSourceTagProcessFunction;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.ReaderSource.SideOutputReaderSource;
import com.qlangtech.tis.realtime.SourceProcessFunction;
import com.qlangtech.tis.realtime.dto.DTOStream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
        Map<String, DeserializationSchema<RowData>> tabColsMapper = Maps.newHashMap();
//            TableInDB tablesInDB = dsFactory.getTablesInDB();
        IFlinkColCreator<FlinkCol> flinkColCreator = sourceFactory.createFlinkColCreator();
//            IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getName());
        DataType physicalDataType = null;
        DeserializationSchema<RowData> deserializeSchema = null;
        List<DataTypes.Field> fields = null;
        for (ISelectedTab tab : tabs) {
            FlinkColMapper colsMapper
                    = AbstractRowDataMapper.getAllTabColsMetaMapper(tab.getCols(), flinkColCreator);
            fields = colsMapper.getColMapper().entrySet().stream()
                    .map((entry) -> FIELD(entry.getKey(), entry.getValue().type)).collect(Collectors.toList());
            // tabColsMapper.put(tab.getName(), colsMapper);
            physicalDataType = ROW(fields);
            deserializeSchema
                    = kafkaReader.format.createDecodingFormat(tab.getName())
                    .createRuntimeDecoder(null, physicalDataType);

            tabColsMapper.put(tab.getName(), Objects.requireNonNull(deserializeSchema, "deserializeSchema can not be null"));
        }

        KafkaSourceBuilder<Tuple2<String/*tableName*/, byte[]>> kafkaSourceBuilder
                = KafkaSource.<Tuple2<String, byte[]>>builder()
                .setProperties(kafkaReader.buildKafkaProperties())
                .setValueOnlyDeserializer(new KafkaDeserializationSchema(kafkaReader.format));

        Objects.requireNonNull(sourceFactory.startOffset, "startOffset can not be null")
                .setOffset(kafkaSourceBuilder);
        kafkaReader.subscription.setSubscription(kafkaSourceBuilder);

        KafkaSource<Tuple2<String, byte[]>> source = kafkaSourceBuilder.build();
        try {
            SourceChannel sourceChannel = new SourceChannel(
                    createKafkaSource(kafkaReader.bootstrapServers, source));

            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null)
                    , (tabName) -> createDispatched(tabName, tabColsMapper));

            return (JobExecutionResult) this.getConsumerHandle().consume(dataxName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DTOStream<Tuple2<String, byte[]>> createDispatched(String table, Map<String, DeserializationSchema<RowData>> tabColsMapper) {
        return new KafkaDispatchedDTOStream(table, tabColsMapper);
    }

    public static ReaderSource<Tuple2<String, byte[]>> createKafkaSource(String tokenName, Source<Tuple2<String, byte[]>, ?, ?> sourceFunc) {
        return new SideOutputReaderSource<Tuple2<String, byte[]>>(tokenName) {
            @Override
            protected DataStreamSource<Tuple2<String, byte[]>> addAsSource(StreamExecutionEnvironment env) {
                return env.fromSource(sourceFunc, WatermarkStrategy.noWatermarks(), tokenName);
            }

            @Override
            protected SourceProcessFunction<Tuple2<String, byte[]>> createStreamTagFunction(Map<String, OutputTag<Tuple2<String, byte[]>>> tab2OutputTag) {
                return new KafkaSourceTagProcessFunction(tab2OutputTag);
            }
        };
    }


}
