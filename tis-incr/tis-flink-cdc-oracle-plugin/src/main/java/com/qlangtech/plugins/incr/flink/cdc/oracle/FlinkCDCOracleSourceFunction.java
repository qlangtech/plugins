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

import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.tis.async.message.client.consumer.IAsyncMsgDeserialize;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.cdc.connectors.oracle.OracleSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.stream.Collectors;

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
            BasicDataSourceFactory f = (BasicDataSourceFactory) reader.getDataSourceFactory();
            SourceChannel sourceChannel = new SourceChannel(
                    SourceChannel.getSourceFunction(f, tabs
                            , (dbHost, dbs, tbs, debeziumProperties) -> {
                                return dbs.getDbStream().map((databaseName) -> {
                                    SourceFunction<DTO> sourceFunction = OracleSource.<DTO>builder()
                                            .hostname(dbHost)
                                            .debeziumProperties(debeziumProperties)
                                            .port(f.port)
                                            .startupOptions(sourceFactory.getStartupOptions())
                                            .database(StringUtils.upperCase(f.dbName)) // monitor XE database
                                            // .schemaList("") // monitor inventory schema
                                            .tableList(tbs.toArray(new String[tbs.size()])) // monitor products table
                                            //.tableList( StringUtils.lowerCase("DEBEZIUM.BASE"))
                                            .username(f.getUserName())
                                            .password(f.getPassword())
                                            .deserializer(new TISDeserializationSchema()) // converts SourceRecord to JSON String
                                            .build();
                                    return ReaderSource.createDTOSource(
                                            dbHost + ":" + f.port + "_" + databaseName, sourceFunction);
                                }).collect(Collectors.toList());

                            }));
            // for (ISelectedTab tab : tabs) {
            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null), DTOStream::createDispatched);
            //}
            return (JobExecutionResult) getConsumerHandle().consume(channalName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }


}
