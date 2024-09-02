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

package com.qlangtech.tis.plugin.datax.doplinscheduler.history;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.datax.WorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DolphinSchedulerURLBuilder.DolphinSchedulerResponse;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.ExportTISPipelineToDolphinscheduler;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;

import java.util.List;

/**
 * DS 触发的任务执行状态追踪
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-21 15:05
 **/
public class DSWorkFlowBuildHistoryPayload extends WorkFlowBuildHistoryPayload {

    private final ExportTISPipelineToDolphinscheduler exportDSCfg;

    public DSWorkFlowBuildHistoryPayload(IDataxProcessor dataxProcessor, Integer tisTaskId, ICommonDAOContext daoContext) {
        super(dataxProcessor, tisTaskId, daoContext);
        Pair<List<ExportTISPipelineToDolphinscheduler>, IPluginStore<DefaultDataXProcessorManipulate>> pluginStorePair
                = DefaultDataXProcessorManipulate.loadPlugins(null, ExportTISPipelineToDolphinscheduler.class, this.dataxProcessor.identityValue());
        for (ExportTISPipelineToDolphinscheduler exportDSCfg : pluginStorePair.getLeft()) {
            this.exportDSCfg = exportDSCfg;
            return;
        }
        throw new IllegalStateException("this.dataxProcessor.identityValue():"
                + this.dataxProcessor.identityValue() + " relevant exportDSCfg instance can not be null");
    }

    @Override
    public ExecResult processExecHistoryRecord() {
        Long spiWorkflowInstanceId = this.getSPIWorkflowInstanceId();
        //http://192.168.28.201:12345/dolphinscheduler/swagger-ui/index.html?language=zh_CN&lang=cn#/process%20instance%20related%20operation/queryProcessInstanceById

        final String process = "process-instances";
        DolphinSchedulerResponse response = this.exportDSCfg.getDSEndpoint().createSchedulerURLBuilder()
                .appendSubPath("projects", this.exportDSCfg.projectCode, process, spiWorkflowInstanceId).applyGet();

        if (!response.isSuccess()) {
            throw new IllegalStateException(process + " is faild," + response.errorDescribe());
        }
        ExecResult execResult = null;
        JSONObject data = response.getData();
        WorkflowExecutionStatus state
                = WorkflowExecutionStatus.valueOf(data.getString("state"));
        if (state.isFinished()) {
            switch (state) {
                case SUCCESS: {
                    execResult = (ExecResult.SUCCESS);
                    break;
                }
                case FAILURE: {
                    execResult = (ExecResult.FAILD);
                    break;
                }
                default: {
                    execResult = (ExecResult.CANCEL);
                    break;
                }
            }
            this.updateFinalStatus(execResult);
            return execResult;
        }

        return null;
    }

    @Override
    public Class<DSWorkFlowBuildHistoryPayloadFactory> getFactory() {
        return DSWorkFlowBuildHistoryPayloadFactory.class;
    }
}
