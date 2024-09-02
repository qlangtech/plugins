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

package com.qlangtech.tis.plugin.datax.powerjob;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.plugin.datax.IWorkflowNode;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.sql.parser.ISqlTask;


import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/23
 */
public class WorkflowUnEffectiveJudge {
    private boolean unEffective;

    private final List<IWorkflowNode> deletedWfNodes = Lists.newArrayList();
    private final Map<String /**tableName*/, IWorkflowNode> existWfNodes = Maps.newHashMap();
    private IWorkflowNode startInitNode;

    public WorkflowUnEffectiveJudge() {
        this(false);
    }
    public WorkflowUnEffectiveJudge(boolean unEffective) {
        this.unEffective = unEffective;
    }
    public void addDeletedWfNode(IWorkflowNode node) {
        deletedWfNodes.add(node);
        this.setUnEffective();
    }

    public List<IWorkflowNode> getDeletedWfNodes() {
        return deletedWfNodes;
    }

    /**
     * 添加已经存在的workflow节点
     *
     * @param tabTriggers
     * @param node
     */
    public void addExistWfNode(SelectedTabTriggers tabTriggers, IWorkflowNode node) {
        String tableName = tabTriggers.getTabName();
        existWfNodes.put(tableName, node);
        //   this.setUnEffective();
    }

    public void addExistWfNode(ISqlTask.SqlTaskCfg sqlTsk, IWorkflowNode node) {
        existWfNodes.put(sqlTsk.getExportName(), node);
    }

    public Optional<IWorkflowNode> getExistWfNode(String tableName) {
        return Optional.ofNullable(existWfNodes.get(tableName));
        //return existWfNodes.entrySet();
    }

    public WorkflowUnEffectiveJudge setUnEffective() {
        this.unEffective = true;
        return this;
    }



    public boolean isUnEffective() {
        return this.unEffective;
    }

    public void setStatInitNode(//PEWorkflowDAG.Node
                                        IWorkflowNode node ) {
        this.startInitNode = node;
    }

    public Optional<IWorkflowNode> getStartInitNode() {
        return Optional.ofNullable(startInitNode);
    }
}
