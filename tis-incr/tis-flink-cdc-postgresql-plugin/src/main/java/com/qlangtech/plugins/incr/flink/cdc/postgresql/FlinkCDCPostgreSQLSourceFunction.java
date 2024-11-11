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

package com.qlangtech.plugins.incr.flink.cdc.postgresql;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.postgresql.PGDTOColValProcess.PGCDCTypeVisitor;
import com.qlangtech.plugins.incr.flink.cdc.valconvert.DateTimeConverter;
import com.qlangtech.tis.async.message.client.consumer.IAsyncMsgDeserialize;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder.PostgresIncrementalSource;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * https://ververica.github.io/flink-cdc-connectors/master/content/connectors/postgres-cdc.html
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:17
 **/
public class FlinkCDCPostgreSQLSourceFunction implements IMQListener<JobExecutionResult> {

    private final FlinkCDCPostreSQLSourceFactory sourceFactory;

    //   private IDataxProcessor dataXProcessor;

    public FlinkCDCPostgreSQLSourceFunction(FlinkCDCPostreSQLSourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }


    @Override
    public IConsumerHandle getConsumerHandle() {
        return this.sourceFactory.getConsumerHander();
    }

    @Override
    public JobExecutionResult start(TargetResName dataxName, IDataxReader dataSource
            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        try {
            BasicDataXRdbmsReader rdbmsReader = (BasicDataXRdbmsReader) dataSource;
            final BasicDataSourceFactory dsFactory = (BasicDataSourceFactory) rdbmsReader.getDataSourceFactory();
            BasicDataSourceFactory.ISchemaSupported schemaSupported = (BasicDataSourceFactory.ISchemaSupported) dsFactory;
            if (StringUtils.isEmpty(schemaSupported.getDBSchema())) {
                throw new IllegalStateException("dsFactory:" + dsFactory.dbName + " relevant dbSchema can not be null");
            }

            final IFlinkColCreator<FlinkCol> flinkColCreator = this.sourceFactory.createFlinkColCreator();

            final Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper
                    = RecordTransformerRules.contextParamValsGetterMapper(IPluginContext.namedContext(dataxName.getName()), rdbmsReader, tabs);

            List<ReaderSource> readerSources = SourceChannel.getSourceFunction(
                    dsFactory, tabs, (dbHost, dbs, tbs, debeziumProperties) -> {
                        /**
                         * for resolve error:
                         * https://stackoverflow.com/questions/59978213/debezium-could-not-access-file-decoderbufs-using-postgres-11-with-default-plug
                         */
                        debeziumProperties.put("plugin.name", "pgoutput");
                        DateTimeConverter.setDatetimeConverters(PGDateTimeConverter.class.getName(), debeziumProperties);

                        return dbs.getDbStream().map((dbname) -> {

                            JdbcIncrementalSource<DTO> incrSource =
                                    PostgresIncrementalSource.<DTO>builder()
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
                                            .startupOptions(sourceFactory.getStartupOptions())
                                            .deserializer(new PostgreSQLDeserializationSchema(tabs, flinkColCreator, contextParamValsGetterMapper,sourceFactory.getRepIdentity())) // converts SourceRecord to JSON String
                                            .build();


                            return ReaderSource.createDTOSource(dbHost + ":" + dsFactory.port + "_" + dbname, incrSource);
                        }).collect(Collectors.toList());

                    });


            SourceChannel sourceChannel = new SourceChannel(readerSources);
            // for (ISelectedTab tab : tabs) {
            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null), DTOStream::createDispatched);
            //}
            return (JobExecutionResult) getConsumerHandle().consume(dataxName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }

}
