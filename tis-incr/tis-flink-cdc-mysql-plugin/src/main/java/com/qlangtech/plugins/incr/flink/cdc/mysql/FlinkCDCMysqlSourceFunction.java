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

package com.qlangtech.plugins.incr.flink.cdc.mysql;

import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.tis.async.message.client.consumer.IAsyncMsgDeserialize;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.ververica.cdc.connectors.mysql.MySqlSource;

import java.util.List;

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
    public void start(IDataxReader dataSource, List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        try {


            BasicDataXRdbmsReader rdbmsReader = (BasicDataXRdbmsReader) dataSource;
            SourceChannel sourceChannel = new SourceChannel(
                    SourceChannel.getSourceFunction((BasicDataSourceFactory) rdbmsReader.getDataSourceFactory(), tabs
                            , (dsFactory, dbHost, dbs, tbs, debeziumProperties) -> {
                                return MySqlSource.<DTO>builder()
                                        .hostname(dbHost)
                                        .port(dsFactory.port)
                                        .databaseList(dbs.toArray(new String[dbs.size()])) // monitor all tables under inventory database
                                        .tableList(tbs.toArray(new String[tbs.size()]))
                                        .username(dsFactory.getUserName())
                                        .password(dsFactory.getPassword())
                                        .startupOptions(sourceFactory.getStartupOptions())
                                        .debeziumProperties(debeziumProperties)
                                        .deserializer(new TISDeserializationSchema()) // converts SourceRecord to JSON String
                                        .build();
                            }));
            for (ISelectedTab tab : tabs) {
                sourceChannel.addFocusTab(tab.getName());
            }
            getConsumerHandle().consume(sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }


    //    private SourceFunction<DTO> createSourceFunction(
//            BasicDataSourceFactory dsFactory, String dbHost, List<String> dbs, Set<String> tbs, Properties debeziumProperties) {
//        return MySqlSource.<DTO>builder()
//                .hostname(dbHost)
//                .port(dsFactory.port)
//                .databaseList(dbs.toArray(new String[dbs.size()])) // monitor all tables under inventory database
//                .tableList(tbs.toArray(new String[tbs.size()]))
//                .username(dsFactory.getUserName())
//                .password(dsFactory.getPassword())
//                .startupOptions(sourceFactory.getStartupOptions())
//                .debeziumProperties(debeziumProperties)
//                .deserializer(new TISDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
//    }


    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public void setDeserialize(IAsyncMsgDeserialize deserialize) {

    }


}
