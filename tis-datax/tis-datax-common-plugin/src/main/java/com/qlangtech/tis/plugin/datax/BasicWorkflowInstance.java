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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.plugin.datax.BasicDistributedSPIDataXJobSubmit.WorkflowNodeVisit;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.meta.NodeType.NodeTypeParseException;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 10:50
 **/
public abstract class BasicWorkflowInstance {

    private static final Logger logger = LoggerFactory.getLogger(BasicWorkflowInstance.class);

    protected final ExecutePhaseRange executePhaseRange;
    protected final String appName;
    protected final Long spiWorkflowId;

    public BasicWorkflowInstance(ExecutePhaseRange executePhaseRange, String appName, Long spiWorkflowId) {
        this.executePhaseRange = executePhaseRange;
        this.appName = appName;
        this.spiWorkflowId = spiWorkflowId;
    }

    public static void vistWorkflowNodes(String appName, List<IWorkflowNode> nodes, WorkflowNodeVisit visitor) {

        for (IWorkflowNode node : nodes) {
            // if ((appName + BasicDistributedSPIDataXJobSubmit.KEY_START_INITIALIZE_SUFFIX).equals(node.getNodeName())) {
            switch (node.getNodeType()) {
                case DUMP: {
                    visitor.vistDumpWorkerNode(node);
                    break;
                }
                case JOINER_SQL:
                    try {
                        visitor.vistJoinWorkerNode(node.parseSqlTaskCfg().get(), node);
                        break;
                    } catch (NodeTypeParseException e) {
                        throw new RuntimeException(e);
                    }
                case START: {
                    // 说明是 初始节点跳过
                    visitor.vistStartInitNode(node);
                    break;
                }
                default:
                    throw new IllegalStateException("illegal node type:" + node.getNodeType());
            }

//            if (node.get) {
//
//
//            } else {
//                try {
//                    Optional<SqlTaskCfg> sqlTaskCfg = node.parseSqlTaskCfg();
//                    if (sqlTaskCfg.isPresent()) {
//                        visitor.vistJoinWorkerNode(sqlTaskCfg.get(), node);
//                    } else {
//                        visitor.vistDumpWorkerNode(node);
//                    }
//                } catch (NodeType.NodeTypeParseException e) {
//                    throw new RuntimeException(e);
//                }
//            }
        }
    }

    public Long getSPIWorkflowId() {
        return this.spiWorkflowId;
    }

    public abstract boolean isDisabled();


    /**
     * 是否已经失效
     *
     * @param topologNodes
     * @return
     */
    public WorkflowUnEffectiveJudge isUnEffective(Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> topologNodes) {
        Map<ISelectedTab, SelectedTabTriggers> selectedTabTriggers = topologNodes.getKey();
        WorkflowUnEffectiveJudge unEffectiveJudge = new WorkflowUnEffectiveJudge();
        try {
            //
            if (this.isDisabled()) {
                return new WorkflowUnEffectiveJudge(true);
            }

            Map<String /**tableName*/, SelectedTabTriggers> tabTriggers
                    = selectedTabTriggers.entrySet().stream().collect(Collectors.toMap((e) -> e.getKey().getName(), (e) -> e.getValue()));

            Map<String, ISqlTask> joinNodes = topologNodes.getRight();


            vistWorkflowNodes(this.appName, this.getWorkflowNodes(), new WorkflowNodeVisit() {
                @Override
                public void vistStartInitNode(IWorkflowNode node) {
                    unEffectiveJudge.setStatInitNode(node);
                }

                @Override
                public void vistJoinWorkerNode(ISqlTask.SqlTaskCfg cfg, IWorkflowNode node) {
                    ISqlTask sqlTask = joinNodes.get(cfg.getExportName());
                    if (sqlTask == null) {
                        unEffectiveJudge.addDeletedWfNode(node);
                    } else {
                        unEffectiveJudge.addExistWfNode(cfg, node);
                        if (!node.getEnable() || !StringUtils.equals(cfg.getSqlScript(), sqlTask.getSql())) {
                            // 触发条件更改了
                            unEffectiveJudge.setUnEffective();
                        }
                    }
                }

                @Override
                public void vistDumpWorkerNode(IWorkflowNode wfNode) {
                    SelectedTabTriggers tabTrigger = tabTriggers.get(wfNode.getNodeName());
                    if (tabTrigger == null) {
                        // 该表同步已经被删除
                        unEffectiveJudge.addDeletedWfNode(wfNode);
                    } else {
                        unEffectiveJudge.addExistWfNode(tabTrigger, wfNode);

                        if (!wfNode.getEnable() || !JsonUtil.objEquals(JSONObject.parseObject(wfNode.getNodeParams())
                                , tabTrigger.createMRParams()
                                , Sets.newHashSet("/exec/taskSerializeNum", "/exec/jobInfo[]/taskSerializeNum"))) {
                            // 触发条件更改了
                            unEffectiveJudge.setUnEffective();
                        }
                    }
                }
            });

        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return unEffectiveJudge.setUnEffective();
        }

        return unEffectiveJudge;
    }

    public abstract List<IWorkflowNode> getWorkflowNodes();

    public ExecutePhaseRange getExecutePhaseRange() {
        return executePhaseRange;
    }
}
