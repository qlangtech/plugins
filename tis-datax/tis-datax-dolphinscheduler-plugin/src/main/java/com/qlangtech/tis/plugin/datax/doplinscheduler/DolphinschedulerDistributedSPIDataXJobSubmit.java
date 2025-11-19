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

package com.qlangtech.tis.plugin.datax.doplinscheduler;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.IDescribableManipulate;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.datax.BasicDistributedSPIDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.BasicWorkflowPayload;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.ExportTISPipelineToDolphinscheduler;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta.SqlDataFlowTopology;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-20 18:51
 **/
@Public
@TISExtension()
public class DolphinschedulerDistributedSPIDataXJobSubmit extends BasicDistributedSPIDataXJobSubmit<DSWorkflowInstance> {
    @Override
    public InstanceType getType() {
        return InstanceType.DS;
    }

    @Override
    public boolean cancelTask(IControlMsgHandler module, Context context, IBuildHistory buildHistory) {
        throw TisException.create("Dolphinscheduler DAG job cancel is not supported");
    }

    @Override
    public TriggerBuildResult triggerJob(IControlMsgHandler module, Context context
            , DataXName appName, Optional<Long> workflowInstanceIdOpt, Optional<WorkFlowBuildHistory> latestSuccessWorkflowHistory) {
        return super.triggerJob(module, context, appName, workflowInstanceIdOpt, latestSuccessWorkflowHistory);
    }

    @Override
    protected BasicWorkflowPayload createWorkflowPayload(
            IControlMsgHandler module, Optional<WorkFlowBuildHistory> latestSuccessWorkflowHistory, SqlDataFlowTopology topology) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BasicWorkflowPayload<DSWorkflowInstance> createApplicationPayload(
            IControlMsgHandler module, DataXName appName, Optional<WorkFlowBuildHistory> latestSuccessWorkflowHistory) {
        return createPayload(this, module, appName);
    }

    private static BasicWorkflowPayload<DSWorkflowInstance> createPayload(BasicDistributedSPIDataXJobSubmit submit, IControlMsgHandler module, DataXName appName) {
        ICommonDAOContext commonDAOContext = getCommonDAOContext(module);
//        if (!(module instanceof IPluginContext)) {
//            throw new IllegalStateException("type of module:" + module.getClass() + " must be type of " + IPluginContext.class);
//        }


        DefaultDataXProcessorManipulate.ProcessorManipulateManager<ExportTISPipelineToDolphinscheduler> pair
                = DefaultDataXProcessorManipulate.loadPlugins(null, ExportTISPipelineToDolphinscheduler.class, appName, new IDescribableManipulate.IManipulateStorable() {
            @Override
            public boolean isManipulateStorable() {
                return true;
            }
        });
        ExportTISPipelineToDolphinscheduler exportCfg = null;
        List<ExportTISPipelineToDolphinscheduler> existPlugins = pair.getTargetInstancePlugin();
        for (ExportTISPipelineToDolphinscheduler cfg : existPlugins) {
            exportCfg = cfg;
        }

        DataxProcessor dataxProcessor = (DataxProcessor) DataxProcessor.load(null, appName);
        return new DSWorkflowPayload(Objects.requireNonNull(exportCfg
                , "load export ds scheduler instance size:" + existPlugins.size() + ",appName:" + appName)
                , dataxProcessor, commonDAOContext, submit) {
            @Override
            public void innerCreatePowerjobWorkflow(boolean updateProcess, Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>
                    , Map<String, ISqlTask>>> selectedTabTriggers, Optional<WorkflowUnEffectiveJudge> unEffectiveOpt) {
                /**
                 * 由DS端发起的触发流程不应该包括创建DAG workflow配置的流程，应该实现由TIS端同步配置插件创建
                 */
                throw new UnsupportedOperationException("trigger from dolphinscheduler side shall not contain create DS workflow process");
                //  super.innerCreatePowerjobWorkflow(selectedTabTriggers, unEffectiveOpt);
            }
        };
    }
}
