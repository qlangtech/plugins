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

package com.qlangtech.plugins.incr.flink.cdc.mongdb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.DefaultTableNameConvert;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.plugins.incr.flink.cdc.mongdb.impl.MongoDBDeserializationSchema;
import com.qlangtech.tis.async.message.client.consumer.IAsyncMsgDeserialize;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.datax.DataXMongodbReader;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMeta;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DBConfig.HostDBs;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.cdc.connectors.mongodb.MongoDBSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/mongodb-cdc/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-02 11:40
 **/
public class FlinkCDCMongoDBSourceFunction implements IMQListener<JobExecutionResult> {
    private final FlinkCDCMongoDBSourceFactory sourceFactory;

    public FlinkCDCMongoDBSourceFunction(FlinkCDCMongoDBSourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }

    @Override
    public JobExecutionResult start(TargetResName dataxName, IDataxReader dataSource
            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        try {
            DataXMongodbReader mongoReader = (DataXMongodbReader) dataSource;
            MangoDBDataSourceFactory dsFactory = mongoReader.getDataSourceFactory();
            IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getName());
            Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper
                    = RecordTransformerRules.contextParamValsGetterMapper(pluginContext, mongoReader, tabs);
            Map<String, Pair<FlinkColMapper, List<MongoCMeta>>> tabColsMapper = Maps.newHashMap();


            IFlinkColCreator<FlinkCol> flinkColCreator = sourceFactory.createFlinkColCreator();
            for (ISelectedTab tab : tabs) {
                FlinkColMapper colsMapper
                        = AbstractRowDataMapper.getAllTabColsMetaMapper(tab.getCols(), flinkColCreator);
                tabColsMapper.put(tab.getName()
                        , Pair.of(colsMapper, tab.getCols().stream().map((c)-> (MongoCMeta) c).collect(Collectors.toUnmodifiableList())));
            }

            final MongoDBDeserializationSchema deserializationSchema
                    = new MongoDBDeserializationSchema(
                    new MongoDBSourceDTOColValProcess(tabColsMapper,mongoReader.parseZoneId())
                    , new DefaultTableNameConvert()
                    , contextParamValsGetterMapper);

            SourceChannel sourceChannel = new SourceChannel(
                    SourceChannel.getSourceFunction(dsFactory, tabs, (dbHost, dbs, tbs, debeziumProperties) -> {
                        List<ReaderSource> sourceFunctions = createSourceFunctions(dsFactory, tabs, deserializationSchema);
                        return sourceFunctions;
                    }));

            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null), DTOStream::createDispatched);
            // IFlinkColCreator<FlinkCol> flinkColCreator = this.sourceFactory.createFlinkColCreator();
            return (JobExecutionResult) getConsumerHandle().consume(dataxName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }

    private List<ReaderSource> createSourceFunctions(
            MangoDBDataSourceFactory dsFactory, List<ISelectedTab> tabs, TISDeserializationSchema deserializationSchema) {
        List<ReaderSource> sourceFuncs = Lists.newArrayList();


        String[] collectionList = tabs.stream()
                .map((tab) -> dsFactory.getDbName() + "." + tab.getName())
                .toArray(String[]::new);
//        for (ISelectedTab tab : tabs) {
//        }
        MongoDBSource.Builder<DTO> builder = MongoDBSource.<DTO>builder()
                .hosts(dsFactory.address)
                .databaseList(dsFactory.dbName)
                //  .startupOptions(sourceFactory.getStartupOptions())

                .collectionList(collectionList)
                .connectionOptions(sourceFactory.connectionOptions)
                .copyExistingPipeline(sourceFactory.copyExistingPipeline)

                // .fullDocumentBeforeChange(sourceFactory.fullDocumentBeforeChange)
//                .scanFullChangelog(true)
//                .updateLookup(true)
                // .copyExisting(sourceFactory.copyExisting)
                //.errorsTolerance(sourceFactory.errorsTolerance)
                .username(dsFactory.getUserName())
                .password(dsFactory.getPassword())
                .deserializer(deserializationSchema);


        Objects.requireNonNull(sourceFactory.startupOption, "startupOption can not be null").setProperty(builder);
        Objects.requireNonNull(sourceFactory.updateRecordComplete, "updateRecordComplete can not be null").setProperty(builder);

        SourceFunction<DTO> source = builder.build();

        sourceFuncs.add(ReaderSource.createDTOSource(dsFactory.address + "_" + dsFactory.dbName, source));

        return sourceFuncs;
    }


    @Override
    public IConsumerHandle getConsumerHandle() {
        return this.sourceFactory.getConsumerHander();
    }
}
