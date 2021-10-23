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

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.tis.async.message.client.consumer.IAsyncMsgDeserialize;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DBConfigGetter;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
    public void start(DBConfigGetter dataSource, List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        try {
            // dataSource.getDescriptor().getDisplayName();
            SourceChannel sourceChannel = new SourceChannel(getMySqlSourceFunction(dataSource, tabs));
            for (ISelectedTab tab : tabs) {
                sourceChannel.addFocusTab(tab.getName());
            }
            getConsumerHandle().consume(sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }


    //https://ververica.github.io/flink-cdc-connectors/master/
    private List<SourceFunction<DTO>> getMySqlSourceFunction(DBConfigGetter dataSource, List<ISelectedTab> tabs) {

        try {
            BasicDataSourceFactory dsFactory = dataSource.getBasicDataSource();
            List<SourceFunction<DTO>> sourceFuncs = Lists.newArrayList();
            DBConfig dbConfig = dataSource.getDbConfig();
            Map<String, List<String>> ip2dbs = Maps.newHashMap();
            Map<String, List<ISelectedTab>> db2tabs = Maps.newHashMap();
            dbConfig.vistDbName((config, ip, dbName) -> {
                List<String> dbs = ip2dbs.get(ip);
                if (dbs == null) {
                    dbs = Lists.newArrayList();
                    ip2dbs.put(ip, dbs);
                }
                dbs.add(dbName);

                if (db2tabs.get(dbName) == null) {
                    db2tabs.put(dbName, tabs);
                }
                return false;
            });

            for (Map.Entry<String /**ip*/, List<String>/**dbs*/> entry : ip2dbs.entrySet()) {


                Set<String> tbs = entry.getValue().stream().flatMap(
                        (dbName) -> db2tabs.get(dbName).stream().map((tab) -> dbName + "." + tab.getName())).collect(Collectors.toSet());


                Properties debeziumProperties = new Properties();
                debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
                sourceFuncs.add(MySqlSource.<DTO>builder()
                        .hostname(entry.getKey())
                        .port(dsFactory.port)
                        .databaseList(entry.getValue().toArray(new String[entry.getValue().size()])) // monitor all tables under inventory database
                        .tableList(tbs.toArray(new String[tbs.size()]))
                        .username(dsFactory.getUserName())
                        .password(dsFactory.getPassword())
                        .startupOptions(sourceFactory.getStartupOptions())
                        .debeziumProperties(debeziumProperties)
                        //.deserializer(new JsonStringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                        .deserializer(new TISDeserializationSchema()) // converts SourceRecord to JSON String
                        .build());
            }

            return sourceFuncs;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

//    public IDataxProcessor getDataXProcessor() {
//        return dataXProcessor;
//    }
//
//    public void setDataXProcessor(IDataxProcessor dataXProcessor) {
//        this.dataXProcessor = dataXProcessor;
//    }

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public void setDeserialize(IAsyncMsgDeserialize deserialize) {

    }


}