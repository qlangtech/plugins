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

import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.sql.parser.ISqlTask;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Optional;

/**
 * 与TIS整合的外部workflow系统，workflow实例需要初始化
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 13:58
 **/
public class WorkflowSPIInitializer<WF_INSTANCE extends BasicWorkflowInstance> {

    private final BasicWorkflowPayload<WF_INSTANCE> workflowSPIContext;

    /**
     * @param workflowSPIContext 外部系统执行工作流，支持 dolphinScheduler，powerjob
     */
    public WorkflowSPIInitializer(BasicWorkflowPayload<WF_INSTANCE> workflowSPIContext) {
        this.workflowSPIContext = workflowSPIContext;
    }

    public WF_INSTANCE initialize() {
        return this.initialize(false);
    }

    /**
     * @param forceInitialize
     * @return
     */
    public WF_INSTANCE initialize(boolean forceInitialize) {
        WF_INSTANCE powerJobWorkflowId = workflowSPIContext.loadWorkflowSPI();// this.loadWorkflowSPI(false);

        Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> selectedTabTriggers = null;
        WorkflowUnEffectiveJudge unEffectiveJudge = null;
        if (forceInitialize || powerJobWorkflowId == null || powerJobWorkflowId.isDisabled()
//                || /**是否已经失效*/(unEffectiveJudge = powerJobWorkflowId.isUnEffective(
//                getTISPowerJob(), selectedTabTriggers = createWfNodes())).isUnEffective()
        ) {
//            if () {
            if (powerJobWorkflowId != null) {
                /**是否已经失效*/
                (unEffectiveJudge = powerJobWorkflowId.isUnEffective(selectedTabTriggers = workflowSPIContext.createWfNodes())).isUnEffective();
            }
            // 如果之前还没有打开分布式调度，现在打开了，powerjob workflow还没有创建，现在创建
            workflowSPIContext.innerCreatePowerjobWorkflow(Optional.ofNullable(selectedTabTriggers), Optional.ofNullable(unEffectiveJudge));
            powerJobWorkflowId = workflowSPIContext.loadWorkflowSPI();
            //}

        }
        return powerJobWorkflowId;
    }
}
