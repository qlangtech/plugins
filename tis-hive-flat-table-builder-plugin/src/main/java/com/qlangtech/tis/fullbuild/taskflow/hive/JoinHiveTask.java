/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.fullbuild.taskflow.hive;

import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.dump.hive.HiveRemoveHistoryDataTask;
import com.qlangtech.tis.dump.hive.HiveTableBuilder;
import com.qlangtech.tis.fs.IFs2Table;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.hive.HiveColumn;
import com.qlangtech.tis.hive.HiveInsertFromSelectParser;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.er.ERRules;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年12月22日 下午7:14:24
 */
public class JoinHiveTask extends HiveTask {

    private static final MessageFormat SQL_INSERT_TABLE = new MessageFormat("INSERT OVERWRITE TABLE {0} PARTITION (pt,pmod) \n {1}");

    private static final Logger log = LoggerFactory.getLogger(JoinHiveTask.class);

    private final ITISFileSystemFactory fileSystem;

    private final IFs2Table fs2Table;

    public JoinHiveTask(ISqlTask nodeMeta, boolean isFinalNode, IPrimaryTabFinder erRules, IJoinTaskStatus joinTaskStatus
            , ITISFileSystemFactory fileSystem, IFs2Table fs2Table) {
        super(nodeMeta, isFinalNode, erRules, joinTaskStatus);
        this.fileSystem = fileSystem;
        Objects.nonNull(fs2Table);
        this.fs2Table = fs2Table;
    }


    @Override
    protected void executeSql(String taskName, String rewritedSql) {
        // 处理历史表，多余的partition要删除，表不同了需要删除重建
        processJoinTask(rewritedSql);
        final EntityName newCreateTab = EntityName.parse(this.nodeMeta.getExportName());
        final String insertSql = SQL_INSERT_TABLE.format(new Object[]{newCreateTab.getFullName(), rewritedSql});
        super.executeSql(taskName, insertSql);
    }

    /**
     * 处理join表，是否需要自动创建表或者删除重新创建表
     *
     * @param sql
     */
    private void processJoinTask(String sql) {
        try {
            final HiveInsertFromSelectParser insertParser = getSQLParserResult(sql);
            final Connection conn = this.getTaskContext().getObj();
            // final DumpTable dumpTable =
            // DumpTable.createTable(insertParser.getTargetTableName());
            final EntityName dumpTable = EntityName.parse(this.getName());
            if (HiveTableBuilder.isTableExists(conn, dumpTable)) {
                if (HiveTableBuilder.isTableSame(conn, insertParser.getCols(), dumpTable)) {
                    log.info("Start clean up history file '{}'", dumpTable);
                    IJoinTaskContext param = this.getContext().getExecContext();
                    //  EntityName dumpTable, IJoinTaskContext chainContext, ITISFileSystemFactory fileSys;
                    RemoveJoinHistoryDataTask.deleteHistoryJoinTable(dumpTable, param, this.fileSystem);
                    // 表结构没有变化，需要清理表中的历史数据 清理历史hdfs数据
                    //this.fs2Table.deleteHistoryFile(dumpTable, this.getTaskContext());
                    // 清理hive数据

                    fs2Table.dropHistoryTable(dumpTable, this.getTaskContext());
                } else {
                    HiveDBUtils.execute(conn, "drop table " + dumpTable);
                    createHiveTable(dumpTable, insertParser.getColsExcludePartitionCols(), conn);
                }
            } else {
                // 说明原表并不存在 直接创建
                log.info("table " + dumpTable + " doesn't exist");
                log.info("create table " + dumpTable);
                createHiveTable(dumpTable, insertParser.getColsExcludePartitionCols(), conn);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HiveInsertFromSelectParser getSQLParserResult() throws Exception {
        return this.getSQLParserResult(mergeVelocityTemplate(Collections.emptyMap()));
    }

    private HiveInsertFromSelectParser getSQLParserResult(String sql) throws ParseException {
        final HiveInsertFromSelectParser insertParser = new HiveInsertFromSelectParser();
        insertParser.start(sql);
        return insertParser;
    }

    /**
     * 创建hive表
     */
    private void createHiveTable(EntityName dumpTable, List<HiveColumn> cols, Connection conn) throws Exception {
        // final String user = this.getContext().joinTaskContext().getContextUserName();
        HiveTableBuilder tableBuilder = new HiveTableBuilder("0");
        tableBuilder.createHiveTableAndBindPartition(conn, dumpTable, cols, (hiveSQl) -> {
            hiveSQl.append("\n LOCATION '").append(
                    HiveRemoveHistoryDataTask.getJoinTableStorePath(fileSystem.getRootDir(), dumpTable)
                            .replaceAll("\\.", "/")).append("'");
        });
    }
    // 索引数据: /user/admin/search4totalpay/all/0/output/20160104003306
    // dump数据: /user/admin/search4totalpay/all/0/20160105003307
}
