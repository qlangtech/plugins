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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IMessageHandler;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta.SqlDataFlowTopology;
import com.qlangtech.tis.workflow.pojo.IWorkflow;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

/**
 * 基础第三方 DataX任务触发
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-20 18:49
 * // @see DistributedPowerJobDataXJobSubmit
 * // @see com.qlangtech.tis.plugin.datax.doplinscheduler.DolphinschedulerDistributedSPIDataXJobSubmit
 **/
public abstract class BasicDistributedSPIDataXJobSubmit<WF_INSTANCE extends BasicWorkflowInstance> extends DataXJobSubmit {
    public final static String KEY_START_INITIALIZE_SUFFIX = "_start_initialize";
    transient RpcServiceReference statusRpc;

    @Override
    public IRemoteTaskTrigger createDataXJob(IDataXJobContext dataXJobContext
            , RpcServiceReference statusRpc, DataXJobInfo jobName, IDataxProcessor processor, CuratorDataXTaskMessage msg) {
        SelectedTabTriggers.PowerJobRemoteTaskTrigger tskTrigger
                = new SelectedTabTriggers.PowerJobRemoteTaskTrigger(jobName, msg);
        return tskTrigger;
    }

    /**
     * 触发ETL数据分析实例
     *
     * @param module
     * @param context
     * @param workflow
     * @param dryRun
     * @param powerJobWorkflowInstanceIdOpt powerJobWorkflowInstanceIdOpt 如果是手动触发则为空,如果是定时触发的，例如在powerjob系统中已经生成了powerjob 的workflowInstanceId
     * @return
     */
    @Override
    public TriggerBuildResult triggerWorkflowJob(
            IControlMsgHandler module, Context context,
            IWorkflow workflow, Boolean dryRun
            , Optional<Long> powerJobWorkflowInstanceIdOpt, Optional<WorkFlowBuildHistory> latestSuccessWorkflowHistory) {

        try {
            //   ICommonDAOContext daoContext = getCommonDAOContext(module);
            SqlTaskNodeMeta.SqlDataFlowTopology topology = SqlTaskNodeMeta.getSqlDataFlowTopology(workflow.getName());

            BasicWorkflowPayload<WF_INSTANCE> payload = createWorkflowPayload(module, latestSuccessWorkflowHistory, topology);
            //   PowerJobClient powerJobClient = getTISPowerJob();
            //  RpcServiceReference statusRpc = getStatusRpc();
            //  StatusRpcClientFactory.AssembleSvcCompsite feedback = statusRpc.get();
            return payload.triggerWorkflow(powerJobWorkflowInstanceIdOpt, getStatusRpc());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 触发数据同步Pipeline执行
     *
     * @param module
     * @param context
     * @param appName
     * @param workflowInstanceIdOpt 如果是手动触发则为空
     * @return
     */
    @Override
    public TriggerBuildResult triggerJob(IControlMsgHandler module, Context context
            , String appName, Optional<Long> workflowInstanceIdOpt, Optional<WorkFlowBuildHistory> latestWorkflowHistory) {
        if (StringUtils.isEmpty(appName)) {
            throw new IllegalArgumentException("appName " + appName + " can not be empty");
        }

        BasicWorkflowPayload<WF_INSTANCE> appPayload = createApplicationPayload(module, appName, latestWorkflowHistory);
        return appPayload.triggerWorkflow(workflowInstanceIdOpt, getStatusRpc());
    }

    protected abstract BasicWorkflowPayload<WF_INSTANCE> createWorkflowPayload(
            IControlMsgHandler module, Optional<WorkFlowBuildHistory> latestSuccessWorkflowHistory, SqlDataFlowTopology topology);

    protected abstract BasicWorkflowPayload<WF_INSTANCE> createApplicationPayload(
            IControlMsgHandler module, String appName, Optional<WorkFlowBuildHistory> latestWorkflowHistory);

    @Override
    public IDataXJobContext createJobContext(IExecChainContext parentContext) {
        return DataXJobSubmit.IDataXJobContext.create(parentContext);
    }

    public static ICommonDAOContext getCommonDAOContext(IMessageHandler module) {
        if (!(module instanceof ICommonDAOContext)) {
            throw new IllegalStateException("module must be type of " + ICommonDAOContext.class.getName());
        }
        ICommonDAOContext daoContext = (ICommonDAOContext) module;
        return daoContext;
    }


    protected RpcServiceReference getStatusRpc() {
        if (this.statusRpc != null) {
            return this.statusRpc;
        }
        try {
            this.statusRpc = StatusRpcClientFactory.getService(ITISCoordinator.create());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this.statusRpc;
    }

    public interface WorkflowNodeVisit {
        /**
         * 访问初始任务节点
         *
         * @param node
         */
        void vistStartInitNode(IWorkflowNode node);

        /**
         * 访问 任务执行节点
         *
         * @param node
         */
        void vistDumpWorkerNode(IWorkflowNode node);

        void vistJoinWorkerNode(ISqlTask.SqlTaskCfg cfg, IWorkflowNode node);

    }
}
