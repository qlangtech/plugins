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

package com.qlangtech.plugins.incr.flink.cdc.pglike;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.valconvert.DateTimeConverter;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactory.ISchemaSupported;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.plugin.incr.IConsumerRateLimiter;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder.PostgresIncrementalSource;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-19 18:12
 **/
public class FlinkCDCPGLikeSourceFunction implements IMQListener<List<ReaderSource>> {
    protected final FlinkCDCPGLikeSourceFactory sourceFactory;

    //   private IDataxProcessor dataXProcessor;

    public FlinkCDCPGLikeSourceFunction(FlinkCDCPGLikeSourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }


//    @Override
//    public IConsumerHandle getConsumerHandle() {
//        return this.sourceFactory.getConsumerHander();
//    }

    @Override
    public AsyncMsg<List<ReaderSource>> start(IConsumerRateLimiter streamFactory,
                                              boolean flinkCDCPipelineEnable, DataXName dataxName, IDataxReader dataSource
            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        try {
            BasicDataXRdbmsReader rdbmsReader = (BasicDataXRdbmsReader) dataSource;
            final BasicDataSourceFactory dsFactory = (BasicDataSourceFactory) rdbmsReader.getDataSourceFactory();
            ISchemaSupported schemaSupported = (ISchemaSupported) dsFactory;
            if (StringUtils.isEmpty(schemaSupported.getDBSchema())) {
                throw new IllegalStateException("dsFactory:" + dsFactory.dbName + " relevant dbSchema can not be null");
            }

            final IFlinkColCreator<FlinkCol> flinkColCreator = this.sourceFactory.createFlinkColCreator(rdbmsReader);

            final Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper
                    = RecordTransformerRules.contextParamValsGetterMapper(
                    dataXProcessor, IPluginContext.namedContext(dataxName.getPipelineName()), rdbmsReader, tabs);

            List<ReaderSource> readerSources = SourceChannel.getSourceFunction(
                    dsFactory, tabs, (dbHost, dbs, tbs, debeziumProperties) -> {
                        /**
                         * for resolve error:
                         * https://stackoverflow.com/questions/59978213/debezium-could-not-access-file-decoderbufs-using-postgres-11-with-default-plug
                         */
                        debeziumProperties.put("plugin.name", "pgoutput");
                        DateTimeConverter.setDatetimeConverters(
                                PGDateTimeConverter.class.getName() //
                                , debeziumProperties,dsFactory.getTimeZone().map(ZoneId::getId));

                        return dbs.getDbStream().map((dbname) -> {

                            JdbcIncrementalSource<DTO> incrSource =
                                    createIncrementalSource(dataxName, tabs, dbHost, tbs, debeziumProperties
                                            , dbname, dsFactory, schemaSupported, flinkColCreator, contextParamValsGetterMapper);


                            return ReaderSource.createDTOSource(streamFactory, dataxName,
                                    dbHost + ":" + dsFactory.port + "_" + dbname, flinkCDCPipelineEnable
                                    , incrSource);
                        }).collect(Collectors.toList());

                    });


            SourceChannel sourceChannel = new SourceChannel(flinkCDCPipelineEnable, readerSources);

            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null, true), DTOStream::createDispatched);

            return sourceChannel;
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }

    protected PostgresIncrementalSource<DTO> createIncrementalSource(DataXName dataxName, List<ISelectedTab> tabs, String dbHost, Set<String> tbs
            , Properties debeziumProperties, String dbname, BasicDataSourceFactory dsFactory
            , ISchemaSupported schemaSupported, IFlinkColCreator<FlinkCol> flinkColCreator
            , Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper) {

        FlinkCDCPGLikeSourceFactory.debeziumProps //
                .forEach((trip) -> {
            // debeziumProperties.setProperty(trip.getMiddle().name(), String.valueOf(trip.getRight().apply(sourceFactory)));
            trip.getValue().accept(debeziumProperties,sourceFactory);
        });

        return PostgresIncrementalSource.<DTO>builder()
                .hostname(dbHost)
                .port(dsFactory.port)
                .database(dbname) // monitor postgres database
                .schemaList(schemaSupported.getDBSchema())  // monitor inventory schema
                .tableList(tbs.toArray(new String[tbs.size()])) // monitor products table
                // .tableList("tis.base")
                .username(dsFactory.userName)
                .decodingPluginName(sourceFactory.decodingPluginName)
                .password(dsFactory.password)
                .debeziumProperties(debeziumProperties)
                .startupOptions(StartupOptionUtils.getStartupOptions(sourceFactory.startupOptions))
                .deserializer(new PostgreSQLDeserializationSchema(
                        tabs, flinkColCreator, contextParamValsGetterMapper, sourceFactory.getRepIdentity()))
                .slotName(dataxName.getPipelineName())
                .build();
    }

}
