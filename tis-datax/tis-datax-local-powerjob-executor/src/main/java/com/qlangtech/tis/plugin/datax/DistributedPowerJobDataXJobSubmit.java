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
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataXPowerJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.ITISPowerJob;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobJobTemplate;
import com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient;
import com.qlangtech.tis.plugin.datax.powerjob.impl.TISWorkflowInfoDTO;
import com.qlangtech.tis.plugin.datax.powerjob.impl.WorkflowListResult;
import com.qlangtech.tis.powerjob.IDataFlowTopology;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.sql.parser.meta.NodeType;
import com.qlangtech.tis.workflow.pojo.IWorkflow;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.request.http.SaveJobInfoRequest;
import tech.powerjob.common.request.http.SaveWorkflowNodeRequest;
import tech.powerjob.common.response.WorkflowInfoDTO;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient.result;

/**
 * 利用PowerJob触发任务
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 21:41
 **/
@TISExtension()
@Public
public class DistributedPowerJobDataXJobSubmit extends DataXJobSubmit implements IDataXPowerJobSubmit {


    public final static String KEY_START_INITIALIZE_SUFFIX = "_start_initialize";

    private static final Logger logger = LoggerFactory.getLogger(DistributedPowerJobDataXJobSubmit.class);

    public DistributedPowerJobDataXJobSubmit() {
    }


    @Override
    public InstanceType getType() {
        return InstanceType.DISTRIBUTE;
    }

    @Override
    public boolean cancelTask(IControlMsgHandler module, Context context, IBuildHistory buildHistory) {

        WorkFlowBuildHistoryPayload buildHistoryPayload
                = new WorkFlowBuildHistoryPayload(buildHistory.getTaskId(), getCommonDAOContext(module));

        TISPowerJobClient tisPowerJob = getTISPowerJob();
        result(tisPowerJob.stopWorkflowInstance(buildHistoryPayload.getPowerJobWorkflowInstanceId()));
        buildHistoryPayload.updateFinalStatus(ExecResult.CANCEL);
        return true;
    }

    @Override
    public void deleteWorkflow(IControlMsgHandler module, Context context, Long wfId) {
        Objects.requireNonNull(wfId, "wfId can not be null");
        TISPowerJobClient tisPowerJob = getTISPowerJob();

        result(tisPowerJob.deleteWorkflow(wfId));
    }

    /**
     * 取得所有当前管理的实例
     * pager.getCurPage(), pager.getRowsPerPage()
     *
     * @param <T>
     * @return
     */
    @Override
    public <T> Pair<Integer, List<T>> fetchAllInstance(Map<String, Object> criteria, int page, int pageSize) {
        TISPowerJobClient tisPowerJob = getTISPowerJob();

        WorkflowListResult workflowResult = tisPowerJob.fetchAllWorkflow(page - 1, pageSize);

        return Pair.of(workflowResult.getTotalItems()
                , workflowResult.getData().stream().map((r) -> (T) r).collect(Collectors.toList()));
    }

    @Override
    public IDataXJobContext createJobContext(IJoinTaskContext parentContext) {
        return DataXJobSubmit.IDataXJobContext.create(parentContext);
    }

    transient RpcServiceReference statusRpc;

    private RpcServiceReference getStatusRpc() {
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


    interface WorkflowVisit {
        /**
         * 访问初始任务节点
         *
         * @param node
         */
        void vistStartInitNode(PEWorkflowDAG.Node node);

        /**
         * 访问 任务执行节点
         *
         * @param node
         */
        void vistDumpWorkerNode(PEWorkflowDAG.Node node);

        void vistJoinWorkerNode(ISqlTask.SqlTaskCfg cfg, PEWorkflowDAG.Node node);

    }

    public static void vistWorkflowNodes(String appName, WorkflowInfoDTO wfInfo, WorkflowVisit visitor) {


        PEWorkflowDAG wfDAG = wfInfo.getPEWorkflowDAG();
        for (PEWorkflowDAG.Node node : wfDAG.getNodes()) {
            if ((appName + KEY_START_INITIALIZE_SUFFIX).equals(node.getNodeName())) {
                // 说明是 初始节点跳过
                visitor.vistStartInitNode(node);
            } else {

                JSONObject nodeCfg = JSON.parseObject(node.getNodeParams());

                if (nodeCfg.containsKey(ISqlTask.KEY_EXECUTE_TYPE)) {
                    try {
                        visitor.vistJoinWorkerNode(ISqlTask.toCfg(nodeCfg), node);
                    } catch (NodeType.NodeTypeParseException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    visitor.vistDumpWorkerNode(node);
                }
            }
        }
    }

    /**
     * https://github.com/datavane/tis/issues/157
     *
     * @param module
     * @param context
     * @param dataxProcessor
     */
    @Override
    public void createJob(IControlMsgHandler module, Context context, DataxProcessor dataxProcessor) {

        PowerWorkflowPayload appPayload = PowerWorkflowPayload.createApplicationPayload(
                this, module, dataxProcessor.identityValue());

        appPayload.innerCreatePowerjobWorkflow(Optional.empty(), Optional.empty());
    }

    /**
     * @param module
     * @param context
     * @param topology
     */
    @Override
    public void createWorkflowJob(IControlMsgHandler module, Context context, IDataFlowTopology topology) {

        PowerWorkflowPayload payload = PowerWorkflowPayload.createTISWorkflowPayload(this, module, topology);

        payload.innerCreatePowerjobWorkflow(
                Optional.empty() //
                , Optional.empty() //
        );
    }


    @Override
    public TriggerBuildResult triggerWorkflowJob(
            IControlMsgHandler module, Context context,
            IWorkflow workflow, Boolean dryRun, Optional<Long> powerJobWorkflowInstanceIdOpt) {

        try {
            ICommonDAOContext daoContext = getCommonDAOContext(module);
            SqlTaskNodeMeta.SqlDataFlowTopology topology = SqlTaskNodeMeta.getSqlDataFlowTopology(workflow.getName());

            PowerWorkflowPayload payload = PowerWorkflowPayload.createTISWorkflowPayload(this, module, topology);
            //   PowerJobClient powerJobClient = getTISPowerJob();
            RpcServiceReference statusRpc = getStatusRpc();
            StatusRpcClientFactory.AssembleSvcCompsite feedback = statusRpc.get();
            return payload.triggerPowerjobWorkflow(daoContext, powerJobWorkflowInstanceIdOpt, statusRpc, feedback);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param module
     * @param context
     * @param appName
     * @param workflowInstanceIdOpt 如果是手动触发则为空
     * @return
     */
    @Override
    public TriggerBuildResult triggerJob(IControlMsgHandler module, Context context
            , String appName, Optional<Long> workflowInstanceIdOpt) {
        if (StringUtils.isEmpty(appName)) {
            throw new IllegalArgumentException("appName " + appName + " can not be empty");
        }

        RpcServiceReference statusRpc = getStatusRpc();
        StatusRpcClientFactory.AssembleSvcCompsite feedback = statusRpc.get();

        PowerWorkflowPayload appPayload = PowerWorkflowPayload.createApplicationPayload(this, module, appName);
        return appPayload.triggerPowerjobWorkflow(getCommonDAOContext(module), workflowInstanceIdOpt, statusRpc, feedback);
    }


    public static ICommonDAOContext getCommonDAOContext(IControlMsgHandler module) {
        if (!(module instanceof ICommonDAOContext)) {
            throw new IllegalStateException("module must be type of " + ICommonDAOContext.class.getName());
        }
        ICommonDAOContext daoContext = (ICommonDAOContext) module;
        return daoContext;
    }

    public TISPowerJobClient getTISPowerJob() {

        DataXJobWorker jobWorker = DataXJobWorker.getJobWorker(TargetResName.K8S_DATAX_INSTANCE_NAME);
        if (!(jobWorker instanceof ITISPowerJob)) {
            throw new IllegalStateException("jobWorker:"
                    + jobWorker.getClass().getName() + " must be type of :" + ITISPowerJob.class);
        }

        ITISPowerJob powerJob = (ITISPowerJob) jobWorker;
        TISPowerJobClient powerJobClient = powerJob.getPowerJobClient();
        return powerJobClient;
    }


    @Override
    public TISWorkflowInfoDTO saveJob(IControlMsgHandler module, Context context, DataxProcessor dataxProcessor) {

        PowerWorkflowPayload appPayload = PowerWorkflowPayload.createApplicationPayload(
                this, module, dataxProcessor.identityValue());


        PowerWorkflowPayload.PowerJobWorkflow powerJobWorkflowId = appPayload.saveJob();

        return (TISWorkflowInfoDTO) result(getTISPowerJob().fetchWorkflow(powerJobWorkflowId.getPowerjobWorkflowId()));
    }


    public static SaveWorkflowNodeRequest createWorkflowNode(PowerJobClient powerJobClient, String name, String jobParams
            , Optional<PEWorkflowDAG.Node> existWfNode, SaveJobInfoRequest jobRequest, K8SDataXPowerJobJobTemplate jobTpl) {
        return createWorkflowNode(powerJobClient, name, jobParams, Optional.of(jobParams), existWfNode, jobRequest, jobTpl);
    }

    public static SaveWorkflowNodeRequest createWorkflowNode(
            PowerJobClient powerJobClient, String name, String jobParams, Optional<String> nodeParams
            , Optional<PEWorkflowDAG.Node> existWfNode, SaveJobInfoRequest jobRequest, K8SDataXPowerJobJobTemplate jobTpl) {

        existWfNode.ifPresent((node) -> {
            jobRequest.setId(node.getJobId());
        });

        jobRequest.setJobName(name);
        jobRequest.setJobParams(jobParams);

        Long jobId = result(powerJobClient.saveJob(jobRequest));

        SaveWorkflowNodeRequest wfNode = jobTpl.createWorkflowNode();
        existWfNode.ifPresent((node) -> {
            wfNode.setId(node.getNodeId());
        });
        wfNode.setJobId(jobId);
        wfNode.setNodeName(name);
        wfNode.setNodeParams(nodeParams.get());
        return wfNode;
    }

    @Override
    public IRemoteTaskTrigger createDataXJob(IDataXJobContext dataXJobContext
            , RpcServiceReference statusRpc, DataXJobInfo jobName, IDataxProcessor processor, CuratorDataXTaskMessage msg) {
        SelectedTabTriggers.PowerJobRemoteTaskTrigger tskTrigger
                = new SelectedTabTriggers.PowerJobRemoteTaskTrigger(jobName, msg);
        return tskTrigger;
    }
}
