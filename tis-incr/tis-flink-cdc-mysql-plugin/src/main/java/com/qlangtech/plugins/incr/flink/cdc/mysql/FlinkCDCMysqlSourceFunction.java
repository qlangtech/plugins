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

package com.qlangtech.plugins.incr.flink.cdc.mysql;

import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.plugins.incr.flink.cdc.TabColIndexer;
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
import com.qlangtech.tis.realtime.transfer.DTO;
import com.ververica.cdc.connectors.mysql.MySqlSource;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

//import org.apache.flink.types.Row;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:17
 **/
public class FlinkCDCMysqlSourceFunction implements IMQListener {

    private final FlinkCDCMySQLSourceFactory sourceFactory;


    //   private IDataxProcessor dataXProcessor;

    public FlinkCDCMysqlSourceFunction(FlinkCDCMySQLSourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }

    @Override
    public IConsumerHandle getConsumerHandle() {
        return this.sourceFactory.getConsumerHander();
    }


    @Override
    public void start(TargetResName dataxName, IDataxReader dataSource
            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        try {
            //TabColIndexer colIndexer = new TabColIndexer(tabs);

//            TISDeserializationSchema deserializationSchema
//                    = new TISDeserializationSchema(new MySQLSourceValConvert(colIndexer));

            TISDeserializationSchema deserializationSchema = new TISDeserializationSchema();

            BasicDataXRdbmsReader rdbmsReader = (BasicDataXRdbmsReader) dataSource;
            SourceChannel sourceChannel = new SourceChannel(
                    SourceChannel.getSourceFunction(
                            (BasicDataSourceFactory) rdbmsReader.getDataSourceFactory()
                            , tabs
                            , (dsFactory, dbHost, dbs, tbs, debeziumProperties) -> {

                                String[] databases = dbs.toArray(new String[dbs.size()]);

                                return Collections.singletonList(new ReaderSource(
                                        dbHost + ":" + dsFactory.port + ":" + dbs.stream().collect(Collectors.joining("_")),
                                        MySqlSource.<DTO>builder()
                                                .hostname(dbHost)
                                                .port(dsFactory.port)
                                                .databaseList(databases) // monitor all tables under inventory database
                                                .tableList(tbs.toArray(new String[tbs.size()]))
                                                .serverTimeZone(BasicDataSourceFactory.DEFAULT_SERVER_TIME_ZONE.getId())
                                                .username(dsFactory.getUserName())
                                                .password(dsFactory.getPassword())
                                                .startupOptions(sourceFactory.getStartupOptions())
                                                .debeziumProperties(debeziumProperties)
                                                .deserializer(deserializationSchema) // converts SourceRecord to JSON String
                                                .build())
                                );
                            }));
            for (ISelectedTab tab : tabs) {
                sourceChannel.addFocusTab(tab.getName());
            }
            getConsumerHandle().consume(dataxName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }


    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public void setDeserialize(IAsyncMsgDeserialize deserialize) {

    }


}
