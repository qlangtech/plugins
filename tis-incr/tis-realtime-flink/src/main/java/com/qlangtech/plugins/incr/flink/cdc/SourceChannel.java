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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.Tab2OutputTag;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DBConfig.DBTable;
import com.qlangtech.tis.plugin.ds.DBConfig.HostDBs;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.realtime.DTOSourceTagProcessFunction;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:47
 **/
public class SourceChannel implements AsyncMsg<List<ReaderSource>> {

    private final List<ReaderSource> sourceFunction;
    private Set<String> focusTabs = null;// = Sets.newHashSet();
    private Tab2OutputTag<DTOStream> tab2OutputTag = null;
    private final boolean flinkCDCPipelineEnable;

    @Override
    public Tab2OutputTag<DTOStream> getTab2OutputTag() {
        return Objects.requireNonNull(tab2OutputTag);
    }

    public SourceChannel(boolean flinkCDCPipelineEnable, List<ReaderSource> sourceFunction) {
        if (CollectionUtils.isEmpty(sourceFunction)) {
            throw new IllegalArgumentException("param sourceFunction can not be empty");
        }
        this.sourceFunction = sourceFunction;
        this.flinkCDCPipelineEnable = flinkCDCPipelineEnable;
    }

    public SourceChannel(boolean flinkCDCPipelineEnable, ReaderSource sourceFunction) {
        this(flinkCDCPipelineEnable, Collections.singletonList(sourceFunction));
    }

    private static Optional<DataSourceFactory.ISchemaSupported> schemaSupported(DataSourceFactory dsFactory) {
        if (dsFactory instanceof DataSourceFactory.ISchemaSupported) {
            return Optional.of((DataSourceFactory.ISchemaSupported) dsFactory);
        }
        return Optional.empty();
    }


    public static List<ReaderSource> getSourceFunction(DataSourceFactory dsFactory
            , List<ISelectedTab> tabs, ReaderSourceCreator sourceFunctionCreator) {

        final Optional<DataSourceFactory.ISchemaSupported> schemaSupport = schemaSupported(dsFactory);
        TableInDB tabsInDB = dsFactory.getTablesInDB();
        return getSourceFunction(dsFactory, (tab) -> {
            DataXJobInfo jobInfo = tabsInDB.createDataXJobInfo(DataXJobSubmit.TableDataXEntity.createTableEntity(null, tab.jdbcUrl, tab.getTabName()), true);
            Optional<String[]> targetTableNames = jobInfo.getTargetTableNames();

            List<String> physicsTabNames = targetTableNames.map(Lists::newArrayList).orElseGet(() -> Lists.newArrayList(tab.getTabName()));

            return physicsTabNames.stream().map((t) -> {
                EntityName entity = EntityName.parse(t, true);
                if (!entity.useDftDbName()) {
                    return entity.getFullName();
                }
                return DataSourceFactory.ISchemaSupported.getCDCTableTokens(schemaSupport, tab.dbNanme, t);
//                return schemaSupport
//                        .filter((schema) -> !schema.isUseDBNameAsSchemaName())
//                        .map(DataSourceFactory.ISchemaSupported::getDBSchema).orElse(tab.dbNanme) + "." + t;
            });
        }, tabs, sourceFunctionCreator);
    }


    //https://ververica.github.io/flink-cdc-connectors/master/
    private static List<ReaderSource> getSourceFunction(DataSourceFactory dsFactory
            , Function<DBTable, Stream<String>> tabnameCreator, List<ISelectedTab> tabs, ReaderSourceCreator sourceFunctionCreator) {

        try {
            DBConfig dbConfig = dsFactory.getDbConfig();
            List<ReaderSource> sourceFuncs = Lists.newArrayList();
            Map<String, HostDBs> ip2dbs = dbConfig.getHostDBsMapper();
            Map<String, List<ISelectedTab>> db2tabs = Maps.newHashMap();
            dbConfig.vistDbName((config, jdbcUrl, ip, dbName) -> {
                if (db2tabs.get(dbName) == null) {
                    db2tabs.put(dbName, tabs);
                }
                return false;
            });

            List<ReaderSource> oneHostSources = null;
            for (Map.Entry<String /**ip*/, HostDBs /**dbs*/> entry : ip2dbs.entrySet()) {


                Set<String> tbs = entry.getValue().mapPhysicsTabs(db2tabs, tabnameCreator);

                Properties debeziumProperties = new Properties();
                debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock

                String dbHost = entry.getKey();
                HostDBs dbs = entry.getValue();
                oneHostSources = sourceFunctionCreator.create(dbHost, dbs, tbs, debeziumProperties);
                if (oneHostSources != null) {
                    sourceFuncs.addAll(oneHostSources);
                }
            }
            return sourceFuncs;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public interface ReaderSourceCreator {
        List<ReaderSource> create(String dbHost, HostDBs dbs, Set<String> tbs, Properties debeziumProperties);
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

    public void setFocusTabs(List<ISelectedTab> tabs, TableAliasMapper tabAliasMapper, Function<String, DTOStream> dtoStreamCreator) {
        if (CollectionUtils.isEmpty(tabs)) {
            throw new IllegalArgumentException("param tabs can not be null");
        }
        if (tabAliasMapper.isNull()) {
            throw new IllegalArgumentException("param tabAliasMapper can not be null");
        }
        this.focusTabs = DTOSourceTagProcessFunction.createFocusTabs(this.flinkCDCPipelineEnable, tabs);
//     (this.flinkCDCPipelineEnable
//                ? Stream.of(DTOSourceTagProcessFunction.KEY_MERGE_ALL_TABS_IN_ONE_BUS)
//                : tabs.stream().map((t) -> t.getName())).collect(Collectors.toSet());

        Map<TableAlias, DTOStream> tab2StreamMapper
                = this.focusTabs.stream().collect(Collectors.toMap(
                (name) -> (flinkCDCPipelineEnable ? TableAlias.create(name, name) : tabAliasMapper.getWithCheckNotNull(name))
                , (name) -> dtoStreamCreator.apply(name)));
        this.tab2OutputTag = new Tab2OutputTag<>(tab2StreamMapper);
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
