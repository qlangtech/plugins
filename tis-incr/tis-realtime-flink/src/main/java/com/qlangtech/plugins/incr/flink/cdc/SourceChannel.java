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

package com.qlangtech.plugins.incr.flink.cdc;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.realtime.ReaderSource;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:47
 **/
public class SourceChannel implements AsyncMsg<List<ReaderSource>> {

    private final List<ReaderSource> sourceFunction;
    private final Set<String> focusTabs = Sets.newHashSet();

    public SourceChannel(List<ReaderSource> sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    //https://ververica.github.io/flink-cdc-connectors/master/
    public static List<ReaderSource> getSourceFunction(
            BasicDataSourceFactory dsFactory, List<ISelectedTab> tabs, ReaderSourceCreator sourceFunctionCreator) {

        try {
            DBConfig dbConfig = dsFactory.getDbConfig();
            List<ReaderSource> sourceFuncs = Lists.newArrayList();
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
                debeziumProperties.put("converters", "datetime");
                debeziumProperties.put("datetime.type", "com.qlangtech.plugins.incr.flink.cdc.mysql.MySqlDateTimeConverter");
                debeziumProperties.put("datetime.format.date", "yyyy-MM-dd");
                debeziumProperties.put("datetime.format.time", "HH:mm:ss");
                debeziumProperties.put("datetime.format.datetime", "yyyy-MM-dd HH:mm:ss");
                debeziumProperties.put("datetime.format.timestamp", "yyyy-MM-dd HH:mm:ss");
                debeziumProperties.put("datetime.format.timestamp.zone", "UTC+8");
                String dbHost = entry.getKey();
                List<String> dbs = entry.getValue();
                sourceFuncs.addAll(sourceFunctionCreator.create(dsFactory, dbHost, dbs, tbs, debeziumProperties));
            }

            return sourceFuncs;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public interface ReaderSourceCreator {
        List<ReaderSource> create(BasicDataSourceFactory dsFactory, String dbHost, List<String> dbs, Set<String> tbs, Properties debeziumProperties);
    }

    @Override
    public List<ReaderSource> getSource() throws IOException {
        return this.sourceFunction;
    }


    @Override
    public String getMsgID() {
        return null;
    }

    @Override
    public Set<String> getFocusTabs() {
        return this.focusTabs;
    }

    public void addFocusTab(String tab) {
        if (StringUtils.isEmpty(tab)) {
            throw new IllegalArgumentException("param tab can not be null");
        }
        this.focusTabs.add(tab);
    }


    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public String getTag() {
        return null;
    }


}
