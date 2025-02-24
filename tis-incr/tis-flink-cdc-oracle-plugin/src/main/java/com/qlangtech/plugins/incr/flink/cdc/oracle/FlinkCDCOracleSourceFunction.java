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

package com.qlangtech.plugins.incr.flink.cdc.oracle;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig.HostDB;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.util.IPluginContext;
import io.debezium.config.Field;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.jdbc.JdbcConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:17
 **/
public class FlinkCDCOracleSourceFunction implements IMQListener<JobExecutionResult> {

    private final FlinkCDCOracleSourceFactory sourceFactory;

    public FlinkCDCOracleSourceFunction(FlinkCDCOracleSourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }

    @Override
    public IConsumerHandle getConsumerHandle() {
        return this.sourceFactory.getConsumerHander();
    }

    @Override
    public JobExecutionResult start(TargetResName channalName, IDataxReader dataSource
            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        try {

            BasicDataXRdbmsReader reader = (BasicDataXRdbmsReader) dataSource;
            BasicDataSourceFactory dsFactory = (BasicDataSourceFactory) reader.getDataSourceFactory();
            Map<String, FlinkColMapper> tabColsMapper = Maps.newHashMap();
            IFlinkColCreator<FlinkCol> flinkColCreator = sourceFactory.createFlinkColCreator();
            for (ISelectedTab tab : tabs) {
                FlinkColMapper colsMapper
                        = AbstractRowDataMapper.getAllTabColsMetaMapper(tab.getCols(), flinkColCreator);
                tabColsMapper.put(tab.getName(), colsMapper);
            }
            TableInDB tablesInDB = dsFactory.getTablesInDB();
            IPluginContext pluginContext = IPluginContext.namedContext(channalName.getName());
            Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper
                    = RecordTransformerRules.contextParamValsGetterMapper(pluginContext, reader, tabs);

            final TISDeserializationSchema deserializationSchema
                    = new TISDeserializationSchema(
                    new OracleSourceDTOColValProcess(tabColsMapper)
                    , tablesInDB.getPhysicsTabName2LogicNameConvertor()
                    , contextParamValsGetterMapper);

            SourceChannel sourceChannel = new SourceChannel(
                    SourceChannel.getSourceFunction(dsFactory, tabs
                            , (dbHost, dbs, tbs, debeziumProperties) -> {
                                return dbs.getDbStream().map((databaseName) -> {
                                    // https://nightlies.apache.org/flink/flink-cdc-docs-release-3.2/docs/connectors/flink-sources/oracle-cdc/
                                    // https://nightlies.apache.org/flink/flink-cdc-docs-master/docs/connectors/flink-sources/oracle-cdc/
                                    debeziumProperties.setProperty(OracleConnectorConfig.LOG_MINING_STRATEGY.name()
                                            , LogMiningStrategy.CATALOG_IN_REDO.getValue());// "online_catalog");
                                    //  debeziumProperties.setProperty(OracleConnectorConfig.CONTINUOUS_MINE.name(), "true");

                                    //  debeziumProperties.setProperty(OracleConnectorConfig.LOB_ENABLED.name(), "false");

                                    for (Triple<String, Field, Function<FlinkCDCOracleSourceFactory, Object>> t
                                            : FlinkCDCOracleSourceFactory.debeziumProps) {
                                        debeziumProperties.setProperty(t.getMiddle().name(), String.valueOf(t.getRight().apply(sourceFactory)));
                                    }

                                    if (CollectionUtils.isEmpty(dbs.dbs)) {
                                        throw new IllegalStateException("dbs.dbs can not be empty");
                                    }
                                    debeziumProperties.setProperty(
                                            DATABASE_CONFIG_PREFIX + JdbcConfiguration.CONNECTION_FACTORY_CLASS.name()
                                            , OracleConnectionFactory.class.getCanonicalName());
                                    for (HostDB hostDB : dbs.dbs) {
                                        debeziumProperties.setProperty(DATABASE_CONFIG_PREFIX + DataxUtils.DATASOURCE_FACTORY_IDENTITY
                                                , Objects.requireNonNull(dsFactory.identityValue(), "datasource identity can not be null"));
                                        debeziumProperties.setProperty(DATABASE_CONFIG_PREFIX + DataxUtils.DATASOURCE_JDBC_URL, hostDB.getJdbcUrl());
                                        break;
                                    }

                                    OracleSourceBuilder<DTO> builder = new OracleSourceBuilder()// OracleSource.<DTO>builder()
                                            .hostname(dbHost)
                                            .debeziumProperties(debeziumProperties)
                                            .port(dsFactory.port)
                                            .serverTimeZone(sourceFactory.timeZone)
                                            .startupOptions(sourceFactory.getStartupOptions())
                                            .databaseList((dsFactory.getDbName())) // monitor XE database
                                            // .schemaList("FLINKUSER") // monitor inventory schema
                                             .tableList(tbs.toArray(new String[tbs.size()])) // monitor products table
                                           // .tableList("FLINKUSER.employees")
                                            .username(dsFactory.getUserName())
                                            .password(dsFactory.getPassword())
                                            .deserializer(deserializationSchema); // converts SourceRecord to JSON String

                                    if (dsFactory instanceof DataSourceFactory.ISchemaSupported) {
                                        builder.schemaList(((DataSourceFactory.ISchemaSupported) dsFactory).getDBSchema());
                                    } else {
                                        throw new IllegalStateException("dsFactory must be " + DataSourceFactory.ISchemaSupported.class.getSimpleName());
                                    }

                                    JdbcIncrementalSource<DTO> sourceFunction = builder.build();
                                    return ReaderSource.createDTOSource(
                                            dbHost + ":" + dsFactory.port + "_" + databaseName, sourceFunction);
                                }).collect(Collectors.toList());

                            }));
            // for (ISelectedTab tab : tabs) {
            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null)
                    , (tabName) -> DTOStream.createDispatched(tabName, sourceFactory.independentBinLogMonitor));
            //}
            return (JobExecutionResult) getConsumerHandle().consume(channalName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }


}
