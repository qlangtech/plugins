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

package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.plugin.datax.WorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.powerjob.impl.PowerjobWorkFlowBuildHistoryPayloadFactory;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.response.WorkflowInstanceInfoDTO;

import static com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient.result;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-21 11:13
 **/
public class PowerjobWorkFlowBuildHistoryPayload extends WorkFlowBuildHistoryPayload {
    private final TISPowerJobClient powerJobClient;

    public PowerjobWorkFlowBuildHistoryPayload(Integer tisTaskId, ICommonDAOContext daoContext, TISPowerJobClient powerJobClient) {
        super(tisTaskId, daoContext);
        this.powerJobClient = powerJobClient;
    }

    @Override
    public Class<PowerjobWorkFlowBuildHistoryPayloadFactory> getFactory() {
        return PowerjobWorkFlowBuildHistoryPayloadFactory.class;
    }

    @Override
    public ExecResult processExecHistoryRecord() {

        Long powerJobWorkflowInstanceId = this.getSPIWorkflowInstanceId();
        WorkflowInstanceInfoDTO workflowInstanceInfo
                = result(powerJobClient.fetchWorkflowInstanceInfo(powerJobWorkflowInstanceId));

        WorkflowInstanceStatus wfStatus = WorkflowInstanceStatus.of(workflowInstanceInfo.getStatus());
        if (WorkflowInstanceStatus.FINISHED_STATUS.contains(wfStatus.getV())) {
            ExecResult execResult = null;
            switch (wfStatus) {
                case SUCCEED:
                    execResult = (ExecResult.SUCCESS);
                    break;
                case FAILED: {
                    execResult = (ExecResult.FAILD);
                    break;
                }
                case STOPPED:
                    execResult = (ExecResult.CANCEL);
                    break;
                default:
                    throw new IllegalStateException("illegal status :" + wfStatus);
            }
            this.updateFinalStatus(execResult);
            return execResult;
        }

        return null;
    }
}
