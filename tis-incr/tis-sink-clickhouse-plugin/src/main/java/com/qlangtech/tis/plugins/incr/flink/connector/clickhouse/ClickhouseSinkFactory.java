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

package com.qlangtech.tis.plugins.incr.flink.connector.clickhouse;

import com.google.common.collect.Maps;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.DataXClickhouseWriter;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.clickhouse.ClickHouseDataSourceFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-18 12:05
 **/
public class ClickhouseSinkFactory extends TISSinkFactory {
    public static final String DISPLAY_NAME_FLINK_CDC_SINK = "Flink-CDC-ClickHouse-Sink";

    @Override
    public Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {
        Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> sinkFuncs = Maps.newHashMap();
        IDataxProcessor.TableAlias tableName = null;
        // Map<String, IDataxProcessor.TableAlias> tabAlias = dataxProcessor.getTabAlias();
        DataXClickhouseWriter dataXWriter = (DataXClickhouseWriter) dataxProcessor.getWriter(null);
        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null");
        IDataxReader reader = dataxProcessor.getReader(null);
        List<ISelectedTab> tabs = reader.getSelectedTabs();

        ClickHouseDataSourceFactory dsFactory = dataXWriter.getDataSourceFactory();
        DBConfig dbConfig = dsFactory.getDbConfig();

        for (Map.Entry<String, IDataxProcessor.TableAlias> tabAliasEntry : dataxProcessor.getTabAlias().entrySet()) {
            tableName = tabAliasEntry.getValue();

            Objects.requireNonNull(tableName, "tableName can not be null");
            if (StringUtils.isEmpty(tableName.getFrom())) {
                throw new IllegalStateException("tableName.getFrom() can not be empty");
            }


            AtomicReference<SinkFunction<DTO>> sinkFuncRef = new AtomicReference<>();
            final IDataxProcessor.TableAlias tabName = tableName;
            AtomicReference<Object[]> exceptionLoader = new AtomicReference<>();
            final String targetTabName = tableName.getTo();
            dbConfig.vistDbURL(false, (dbName, jdbcUrl) -> {
                try {
                    Optional<ISelectedTab> selectedTab = tabs.stream()
                            .filter((tab) -> StringUtils.equals(tabName.getFrom(), tab.getName())).findFirst();
                    if (!selectedTab.isPresent()) {
                        throw new IllegalStateException("target table:" + tabName.getFrom()
                                + " can not find matched table in:["
                                + tabs.stream().map((t) -> t.getName()).collect(Collectors.joining(",")) + "]");
                    }
                    /**
                     * 需要先初始化表starrocks目标库中的表
                     */
                    //  dataXWriter.initWriterTable(targetTabName, Collections.singletonList(jdbcUrl));

                    sinkFuncRef.set(createSinkFunction(dbName, targetTabName, selectedTab.get(), jdbcUrl, dsFactory));

                } catch (Throwable e) {
                    exceptionLoader.set(new Object[]{jdbcUrl, e});
                }
            });
            if (exceptionLoader.get() != null) {
                Object[] error = exceptionLoader.get();
                throw new RuntimeException((String) error[0], (Throwable) error[1]);
            }
            Objects.requireNonNull(sinkFuncRef.get(), "sinkFunc can not be null");
            sinkFuncs.put(tableName, sinkFuncRef.get());
        }
//        if (tabAlias == null || tabAlias.isEmpty()) {
//            throw new IllegalStateException("has not set tables");
//        }
//        IDataxProcessor.TableAlias tableName = null;
//        for (Map.Entry<String, IDataxProcessor.TableAlias> entry : tabAlias.entrySet()) {
//            tableName = entry.getValue();
//            break;
//        }


        if (sinkFuncs.size() < 1) {
            throw new IllegalStateException("size of sinkFuncs can not be small than 1");
        }
        return sinkFuncs;
    }

    private SinkFunction<DTO> createSinkFunction(
            String dbName, final String targetTabName, ISelectedTab tab, String jdbcUrl, ClickHouseDataSourceFactory dsFactory) {
//import org.apache.flink.table.types.DataType;
//        TableSchema.Builder schemaBuilder = TableSchema.builder();
//        String[] fieldKeys = new String[tab.getCols().size()];
//        if (fieldKeys.length < 1) {
//            throw new IllegalArgumentException("fieldKeys.length can not small than 1");
//        }
//        int index = 0;
//        for (ISelectedTab.ColMeta cm : tab.getCols()) {
//            schemaBuilder.field(cm.getName(), mapFlinkColType(cm.getType()));
//            fieldKeys[index++] = cm.getName();
//        }
//
//        return StarRocksSink.sink(
//                // the table structure
//                schemaBuilder.build(),
//                // the sink options
//                createRocksSinkOptions(dbName, targetTabName, jdbcUrl, dsFactory)
//                // set the slots with streamRowData
//                , (slots, streamRowData) -> {
//                    if (streamRowData.getEventType() == DTO.EventType.DELETE) {
//                        return;
//                    }
//                    for (int i = 0; i < fieldKeys.length; i++) {
//                        slots[i] = streamRowData.getAfter().get(fieldKeys[i]);
//                    }
//                }
//        );
        return null;
    }


    @TISExtension
    public static class DefaultSinkFunctionDescriptor extends BaseSinkFunctionDescriptor {
        @Override
        public String getDisplayName() {
            return DISPLAY_NAME_FLINK_CDC_SINK;
        }

        @Override
        protected IDataXPluginMeta.EndType getTargetType() {
            return IDataXPluginMeta.EndType.Clickhouse;
        }
    }
}
