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
package com.qlangtech.tis.plugin.ds.tidb;

import com.alibaba.citrus.turbine.Context;
import com.pingcap.com.google.common.collect.Lists;
import com.pingcap.com.google.common.collect.Maps;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.util.RangeSplitter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * 针对PingCap TiKV作为数据源实现
 *
 * @author: baisui 百岁
 * @create: 2020-11-24 10:55
 **/
public class TiKVDataSourceFactory extends DataSourceFactory {

    private transient static final Logger logger = LoggerFactory.getLogger(TiKVDataSourceFactory.class);

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.host})
    public String pdAddrs;

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String dbName;

    @Override
    public DataDumpers getDataDumpers(TISTable table) {

        // target cols
        final List<ColumnMetaData> reflectCols = table.getReflectCols();
        if (CollectionUtils.isEmpty(reflectCols)) {
            throw new IllegalStateException("param reflectCols can not be null");
        }

        final AtomicReference<TiTableInfoWrapper> tabRef = new AtomicReference<>();

        final List<TiPartition> parts = this.openTiDB((session, c, db) -> {

            TiTableInfo tiTable = c.getTable(db, table.getTableName());
            tabRef.set(new TiTableInfoWrapper(tiTable));
            TiDAGRequest dagRequest = getTiDAGRequest(reflectCols, session, tiTable);

            // Snapshot snapshot = session.createSnapshot(dagRequest.getStartTs());
            List<Long> prunedPhysicalIds = dagRequest.getPrunedPhysicalIds();
            List<TiPartition> partitions = null;
            // Iterator<TiChunk> iterator = null;

            return prunedPhysicalIds.stream().flatMap((prunedPhysicalId)
                    -> createPartitions(prunedPhysicalId, session, dagRequest.copyReqWithPhysicalId(prunedPhysicalId)).stream())
                    .collect(Collectors.toList());

            // int rowCount = 0;
//            for (Long prunedPhysicalId : prunedPhysicalIds) {
//                partitions = createPartitions(prunedPhysicalId, session, dagRequest.copyReqWithPhysicalId(prunedPhysicalId));
//                for (TiPartition p : partitions) {
//                    parts.add(p);
//                }
//            }
        });

        int[] index = new int[1];
        final int splitCount = parts.size();
        Objects.requireNonNull(tabRef.get(), "instacne of TiTableInfo can not be null");
        Iterator<IDataSourceDumper> dumpers = new Iterator<IDataSourceDumper>() {
            @Override
            public boolean hasNext() {
                return index[0] < splitCount;
            }

            @Override
            public IDataSourceDumper next() {
                return new TiKVDataSourceDumper(TiKVDataSourceFactory.this, parts.get(index[0]++), tabRef.get(), reflectCols);
            }
        };
        return new DataDumpers(splitCount, dumpers);
    }


    public TiDAGRequest getTiDAGRequest(List<ColumnMetaData> reflectCols, TiSession session, TiTableInfo tiTable) {
        return TiDAGRequest.Builder
                .newBuilder()
                .setFullTableScan(tiTable)
                //                .addFilter(
                //                        ComparisonBinaryExpression
                //                                .equal(
                //                                        ColumnRef.create("table_id", IntegerType.BIGINT),
                //                                        Constant.create(targetTblId, IntegerType.BIGINT)))
                .addRequiredCols(reflectCols.stream().map((col) -> col.getKey()).collect(Collectors.toList()))
                .setStartTs(session.getTimestamp())
                .build(TiDAGRequest.PushDownType.NORMAL);
    }

    public List<TiPartition> createPartitions(Long physicalId, TiSession session, TiDAGRequest dagRequest) {

        final List<TiPartition> partitions = Lists.newArrayList();

        List<RangeSplitter.RegionTask> keyWithRegionTasks = RangeSplitter
                .newSplitter(session.getRegionManager())
                .splitRangeByRegion(dagRequest.getRangesByPhysicalId(physicalId), dagRequest.getStoreType());

        Map<String, List<RangeSplitter.RegionTask>> hostTasksMap = Maps.newHashMap();
        List<RangeSplitter.RegionTask> tasks = null;
        for (RangeSplitter.RegionTask task : keyWithRegionTasks) {
            tasks = hostTasksMap.get(task.getHost());
            if (tasks == null) {
                tasks = Lists.newArrayList();
                hostTasksMap.put(task.getHost(), tasks);
            }
            tasks.add(task);
        }
        int index = 0;
        for (List<RangeSplitter.RegionTask> tks : hostTasksMap.values()) {
            partitions.add(new TiPartition(index++, tks));
        }
        return partitions;
    }

    @Override
    public List<String> getTablesInDB() {
        return this.openTiDB((s, c, d) -> {
            List<TiTableInfo> tabs = c.listTables(d);
            return tabs.stream().map((tt) -> tt.getName()).collect(Collectors.toList());
        });
    }

    <T> T openTiDB(IVistTiDB<T> vistTiDB) {
        TiSession session = null;
        try {
            session = getTiSession();
            try (Catalog cat = session.getCatalog()) {
                TiDBInfo db = cat.getDatabase(dbName);
                return vistTiDB.visit(session, cat, db);
            }
        } finally {
            try {
                session.close();
            } catch (Throwable e) {}
        }
    }

    public TiSession getTiSession() {
        TiConfiguration conf = TiConfiguration.createDefault(this.pdAddrs);
        return TiSession.getInstance(conf);
    }


    @Override
    public List<ColumnMetaData> getTableMetadata(String table) {
        return this.openTiDB((session, c, db) -> {
            TiTableInfo table1 = c.getTable(db, table);
            int[] index = new int[1];

            return table1.getColumns().stream().map((col) -> {
                return new ColumnMetaData(index[0]++, col.getName(), col.getType().getTypeCode(), col.isPrimaryKey());
            }).collect(Collectors.toList());
        });
    }

    @Override
    public String getName() {
        return this.dbName;
    }


    @Override
    public DataSource createFacadeDataSource() {
        return null;
    }


    @TISExtension
    public static class DefaultDescriptor extends DataSourceFactory.BaseDataSourceFactoryDescriptor {

        @Override
        protected String getDataSourceName() {
            return "TiKV";
        }

        @Override
        protected boolean supportFacade() {
            return true;
        }

        @Override
        protected List<String> facadeSourceTypes() {
            return Collections.singletonList(DS_TYPE_MYSQL);
        }

        @Override
        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            try {
                ParseDescribable<DataSourceFactory> tikv = this.newInstance(postFormVals.rawFormData);
                DataSourceFactory sourceFactory = tikv.instance;
                List<String> tables = sourceFactory.getTablesInDB();
                if (tables.size() < 1) {
                    msgHandler.addErrorMessage(context, "TiKV库" + sourceFactory.getName() + "中的没有数据表");
                    return false;
                }
            } catch (Exception e) {
                msgHandler.addErrorMessage(context, e.getMessage());
                logger.warn(e.getMessage(), e);
                // throw new RuntimeException(e);
                return false;
            }
            return true;
        }
    }
}
