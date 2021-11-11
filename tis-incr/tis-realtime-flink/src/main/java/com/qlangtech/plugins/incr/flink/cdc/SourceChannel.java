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

package com.qlangtech.plugins.incr.flink.cdc;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

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
public class SourceChannel implements AsyncMsg<List<SourceFunction<DTO>>> {

    private final List<SourceFunction<DTO>> sourceFunction;
    private final Set<String> focusTabs = Sets.newHashSet();

    public SourceChannel(List<SourceFunction<DTO>> sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    //https://ververica.github.io/flink-cdc-connectors/master/
    public static List<SourceFunction<DTO>> getSourceFunction(
            BasicDataSourceFactory dsFactory, List<ISelectedTab> tabs, SourceFunctionCreator sourceFunctionCreator) {

        try {
            DBConfig dbConfig = dsFactory.getDbConfig();
            // BasicDataSourceFactory dsFactory = dataSource.getBasicDataSource();
            List<SourceFunction<DTO>> sourceFuncs = Lists.newArrayList();
            // DBConfig dbConfig = dataSource.getDbConfig();
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
                String dbHost = entry.getKey();
                List<String> dbs = entry.getValue();
                sourceFuncs.add(sourceFunctionCreator.create(dsFactory, dbHost, dbs, tbs, debeziumProperties));
            }

            return sourceFuncs;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public interface SourceFunctionCreator {
        SourceFunction<DTO> create(BasicDataSourceFactory dsFactory, String dbHost, List<String> dbs, Set<String> tbs, Properties debeziumProperties);
    }

    @Override
    public List<SourceFunction<DTO>> getSource() throws IOException {
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
