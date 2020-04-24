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

import com.qlangtech.tis.dump.DumpTable;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.dump.hive.HiveRemoveHistoryDataTask;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.fullbuild.indexbuild.ITabPartition;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.taskflow.AdapterTask;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlRewriter;
import com.qlangtech.tis.sql.parser.SqlRewriter.AliasTable;
import com.qlangtech.tis.sql.parser.SqlStringBuilder;
import com.qlangtech.tis.sql.parser.er.ERRules;
import com.qlangtech.tis.sql.parser.meta.DependencyNode;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/* *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年11月1日 下午10:58:29
 */
public abstract class HiveTask extends AdapterTask {

    // private HiveDBUtils hiveDBHelper;
    private static final Logger logger = LoggerFactory.getLogger(HiveTask.class);

    private final IJoinTaskStatus joinTaskStatus;

    protected final ISqlTask nodeMeta;

    private static final SqlParser sqlParser = new com.facebook.presto.sql.parser.SqlParser();

    private final Map<EntityName, ERRules.TabFieldProcessor> dumpNodeExtraMetaMap;

    /**
     * @param joinTaskStatus
     */
    protected HiveTask(ISqlTask nodeMeta, Map<EntityName, ERRules.TabFieldProcessor> dumpNodeExtraMetaMap, IJoinTaskStatus joinTaskStatus) {
        super(nodeMeta.getId());
        if (joinTaskStatus == null) {
            throw new IllegalStateException("param joinTaskStatus can not be null");
        }
        this.dumpNodeExtraMetaMap = dumpNodeExtraMetaMap;
        this.joinTaskStatus = joinTaskStatus;
        this.nodeMeta = nodeMeta;
    }

    @Override
    public final String getName() {
        return this.nodeMeta.getExportName();
    }

    private String sqlContent;

    private AliasTable primaryTable;

    @Override
    public String getContent() {
        if (this.sqlContent == null) {
            // 执行sql rewrite
            if (getDumpPartition().size() < 1) {
                throw new IllegalStateException("dumpPartition set size can not small than 1");
            }
            // SQL rewrite
            final String sqlContent = nodeMeta.getSql();
            // nodeMeta.getDependencies();
            // logger.info("raw sql:\n" + sqlContent);
            Optional<List<Expression>> parameters = Optional.empty();
            SqlStringBuilder builder = new SqlStringBuilder();
            SqlRewriter rewriter = new SqlRewriter(builder, getDumpPartition(), this.dumpNodeExtraMetaMap, parameters);
            // 执行rewrite
            rewriter.process(sqlParser.createStatement(sqlContent, new ParsingOptions()), 0);
            this.primaryTable = rewriter.getPrimayTable();
            if (this.primaryTable == null) {
                throw new IllegalStateException("task:" + this.getName() + " has not find primary table");
            }
            this.sqlContent = builder.toString();
        }
        return this.sqlContent;
    }

    private static class DependencyNodeStatus {

        final DependencyNode taskNode;

        final Boolean dependencyWorkStatus;

        public DependencyNodeStatus(DependencyNode taskNode, Boolean dependencyWorkStatus) {
            super();
            this.taskNode = taskNode;
            this.dependencyWorkStatus = dependencyWorkStatus;
        }
    }

    @Override
    protected void executeSql(String taskname, String sql) {
        this.validateDependenciesNode(taskname);
        final Connection conn = this.getTaskContext().getObj();
        final DumpTable newCreateTab = DumpTable.createTable(this.nodeMeta.getExportName());
        final String newCreatePt = primaryTable.getTabPartition();
        List<String> allpts = null;
        try {
            logger.info("\n execute hive task:{} \n{}", taskname, sql);
            HiveDBUtils.execute(conn, sql, joinTaskStatus);
            // 将当前的join task的partition设置到当前上下文中
            Map<IDumpTable, ITabPartition> dumpPartition = this.getDumpPartition();
            dumpPartition.put(newCreateTab, () -> newCreatePt);
            allpts = HiveRemoveHistoryDataTask.getHistoryPts(conn, newCreateTab);
        } catch (Exception e) {
            // TODO 一旦有异常要将整个链路执行都停下来
            throw new RuntimeException("taskname:" + taskname, e);
        }
        AliasTable child = null;
        // 校验最新的Partition 是否已经生成
        if (!allpts.contains(newCreatePt)) {
            StringBuffer errInfo = new StringBuffer();
            errInfo.append("\ntable:" + newCreateTab + "," + IDumpTable.PARTITION_PT + ":" + newCreatePt + " is not exist in exist partition set [" + Joiner.on(",").join(allpts) + "]");
            child = primaryTable.getChild();
            if (child != null && !child.isSubQueryTable()) {
                try {
                    allpts = HiveRemoveHistoryDataTask.getHistoryPts(conn, child.getTable());
                } catch (Exception e) {
                    throw new RuntimeException(child.getTable().getFullName(), e);
                }
                errInfo.append("\n\t child table:").append(child.getTable()).append(",").append(IDumpTable.PARTITION_PT).append(":").append(newCreatePt).append(" is not exist in exist partition set [").append(Joiner.on(",").join(allpts)).append("]");
            }
            throw new IllegalStateException(errInfo.toString());
        }
    }

    protected void validateDependenciesNode(String taskname) {
        Boolean dependencyWorkStatus = null;
        final List<DependencyNodeStatus> lackDependencies = Lists.newArrayList();
        for (DependencyNode depenency : this.nodeMeta.getDependencies()) {
            dependencyWorkStatus = this.getTaskWorkStatus().get(depenency.getId());
            if (dependencyWorkStatus == null || !dependencyWorkStatus) {
                lackDependencies.add(new DependencyNodeStatus(depenency, dependencyWorkStatus));
            }
        }
        if (!lackDependencies.isEmpty()) {
            // 说明有依赖到的node没有被执行
            throw new IllegalStateException("taskname:" + taskname + " lackDependencies:" + lackDependencies.stream().map((r) -> "(" + r.taskNode.getId() + "," + r.taskNode.getDbName() + "." + r.taskNode.getName() + ",status:" + (r.dependencyWorkStatus == null ? "notExecute" : r.dependencyWorkStatus) + ")").collect(Collectors.joining()));
        }
    }
}
