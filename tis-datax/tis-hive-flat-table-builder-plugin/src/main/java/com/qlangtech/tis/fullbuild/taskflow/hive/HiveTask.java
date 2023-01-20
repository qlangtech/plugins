/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.qlangtech.tis.fullbuild.taskflow.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.dump.hive.HiveRemoveHistoryDataTask;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.fullbuild.indexbuild.ITabPartition;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.taskflow.AdapterTask;
import com.qlangtech.tis.sql.parser.IAliasTable;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.TabPartitions;
import com.qlangtech.tis.sql.parser.er.ERRules;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import com.qlangtech.tis.sql.parser.meta.DependencyNode;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年11月1日 下午10:58:29
 */
public abstract class HiveTask extends AdapterTask {

    private static final Logger logger = LoggerFactory.getLogger(HiveTask.class);

    private final IJoinTaskStatus joinTaskStatus;

    protected final ISqlTask nodeMeta;
    protected final boolean isFinalNode;

    //private static final SqlParser sqlParser = new com.facebook.presto.sql.parser.SqlParser();

    private final IPrimaryTabFinder erRules;

    /**
     * @param joinTaskStatus
     */
    protected HiveTask(ISqlTask nodeMeta, boolean isFinalNode, IPrimaryTabFinder erRules, IJoinTaskStatus joinTaskStatus) {
        super(nodeMeta.getId());
        if (joinTaskStatus == null) {
            throw new IllegalStateException("param joinTaskStatus can not be null");
        }
        Objects.requireNonNull(erRules, "param erRule can not be null");
        this.erRules = erRules;
        this.joinTaskStatus = joinTaskStatus;
        this.nodeMeta = nodeMeta;
        this.isFinalNode = isFinalNode;
    }

    @Override
    public final String getName() {
        return this.nodeMeta.getExportName();
    }

    @Override
    public FullbuildPhase phase() {
        return FullbuildPhase.JOIN;
    }

    @Override
    public String getIdentityName() {
        return nodeMeta.getExportName();
    }

    private ISqlTask.RewriteSql rewriteSql;

    // private AliasTable primaryTable;

    @Override
    public String getContent() {
        if (this.rewriteSql == null) {
            // 执行sql rewrite
            // SQL rewrite
            //final String sqlContent = nodeMeta.getSql();
            // nodeMeta.getDependencies();
            // logger.info("raw sql:\n" + sqlContent);
            //SqlStringBuilder builder = ;
            this.rewriteSql = nodeMeta.getRewriteSql(this.getName(), this.getDumpPartition(), this.erRules, this.getContext(), this.isFinalNode);
        }
        return this.rewriteSql.sqlContent;
    }

//    private static String getRewriteSql(String taskName,String sqlContent, Map<IDumpTable, ITabPartition> dumpPartition, ERRules erRules
//            , ITemplateContext templateContext,boolean isFinalNode) {
//        Optional<List<Expression>> parameters = Optional.empty();
//        IJoinTaskContext joinContext = templateContext.joinTaskContext();
//        SqlStringBuilder builder = new SqlStringBuilder();
//        SqlRewriter rewriter = new SqlRewriter(builder, dumpPartition, erRules, parameters, isFinalNode, joinContext);
//        // 执行rewrite
//        rewriter.process(sqlParser.createStatement(sqlContent, new ParsingOptions()), 0);
//        AliasTable primaryTable = rewriter.getPrimayTable();
//        if (primaryTable == null) {
//            throw new IllegalStateException("task:" + taskName + " has not find primary table");
//        }
//        return builder.toString();
//    }

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
        final EntityName newCreateTab = EntityName.parse(this.nodeMeta.getExportName());
        //final String newCreatePt = primaryTable.getTabPartition();
        this.getContent();
        List<String> allpts = null;
        try {
            logger.info("\n execute hive task:{} \n{}", taskname, sql);
            HiveDBUtils.execute(conn, sql, joinTaskStatus);
            // 将当前的join task的partition设置到当前上下文中
            TabPartitions dumpPartition = this.getDumpPartition();
            dumpPartition.putPt(newCreateTab, this.rewriteSql.primaryTable);
            allpts = HiveRemoveHistoryDataTask.getHistoryPts(conn, newCreateTab);
        } catch (Exception e) {
            // TODO 一旦有异常要将整个链路执行都停下来
            throw new RuntimeException("taskname:" + taskname, e);
        }
        IAliasTable child = null;
        // 校验最新的Partition 是否已经生成
        if (!allpts.contains(this.rewriteSql.primaryTable.getPt())) {
            StringBuffer errInfo = new StringBuffer();
            errInfo.append("\ntable:" + newCreateTab + "," + IDumpTable.PARTITION_PT + ":" + this.rewriteSql.primaryTable
                    + " is not exist in exist partition set [" + Joiner.on(",").join(allpts) + "]");
            child = this.rewriteSql.primaryTable.getChild();
            if (child != null && !child.isSubQueryTable()) {
                try {
                    allpts = HiveRemoveHistoryDataTask.getHistoryPts(conn, child.getTable());
                } catch (Exception e) {
                    throw new RuntimeException(child.getTable().getFullName(), e);
                }
                errInfo.append("\n\t child table:").append(child.getTable()).append(",").append(IDumpTable.PARTITION_PT)
                        .append(":").append(this.rewriteSql.primaryTable)
                        .append(" is not exist in exist partition set [").append(Joiner.on(",").join(allpts)).append("]");
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
            throw new IllegalStateException("taskname:" + taskname + " lackDependencies:"
                    + lackDependencies.stream().map((r) -> "(" + r.taskNode.getId() + "," + r.taskNode.parseEntityName()
                    + ",status:" + (r.dependencyWorkStatus == null ? "notExecute" : r.dependencyWorkStatus) + ")")
                    .collect(Collectors.joining())
                    + "/n TaskWorkStatus:"
                    + this.getTaskWorkStatus().entrySet().stream()
                    .map((e) -> "[" + e.getKey() + "->" + e.getValue() + "]")
                    .collect(Collectors.joining(",")));
        }
    }
}
