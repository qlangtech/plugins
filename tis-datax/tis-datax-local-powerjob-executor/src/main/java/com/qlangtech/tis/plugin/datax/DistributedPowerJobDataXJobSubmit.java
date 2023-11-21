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
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.ITISPowerJob;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.fullbuild.phasestatus.PhaseStatusCollection;
import com.qlangtech.tis.fullbuild.phasestatus.impl.AbstractChildProcessStatus;
import com.qlangtech.tis.fullbuild.phasestatus.impl.DumpPhaseStatus;
import com.qlangtech.tis.fullbuild.phasestatus.impl.JoinPhaseStatus;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobJobTemplate;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.DAGSessionSpec;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.enums.WorkflowNodeType;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.request.http.SaveJobInfoRequest;
import tech.powerjob.common.request.http.SaveWorkflowNodeRequest;
import tech.powerjob.common.request.http.SaveWorkflowRequest;
import tech.powerjob.common.response.ResultDTO;
import tech.powerjob.common.response.WorkflowInfoDTO;
import tech.powerjob.common.response.WorkflowInstanceInfoDTO;
import tech.powerjob.common.response.WorkflowNodeInfoDTO;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.qlangtech.tis.datax.job.DataXJobWorker.K8S_DATAX_INSTANCE_NAME;

/**
 * 利用PowerJob触发任务
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 21:41
 **/
@TISExtension()
@Public
public class DistributedPowerJobDataXJobSubmit extends DataXJobSubmit {

    //    private CuratorFramework curatorClient = null;
//    private DistributedQueue<CuratorDataXTaskMessage> curatorDistributedQueue = null;
    //  PowerJobClient powerJobClient;

    private static final Logger logger = LoggerFactory.getLogger(DistributedPowerJobDataXJobSubmit.class);

    public DistributedPowerJobDataXJobSubmit() {
//        PowerJobClient powerJobClient = new PowerJobClient("127.0.0.1:7700", "powerjob-worker-samples", "powerjob123");
//        powerJobClient.
    }

//    @Override
//    public IRemoteTaskTrigger createDataXJob(IDataXJobContext taskContext, RpcServiceReference statusRpc
//            , IDataxProcessor processor, Pair<String, IDataXBatchPost.LifeCycleHook> lifeCycleHookInfo, String tableName) {
//        if (StringUtils.isEmpty(tableName)) {
//            throw new IllegalArgumentException("param tableName can not be null");
//        }
//        DataxPrePostConsumer prePostConsumer = new DataxPrePostConsumer();
//        DataXLifecycleHookMsg lifecycleHookMsg = new DataXLifecycleHookMsg();
//        lifecycleHookMsg.setLifeCycleHook(Objects.requireNonNull(lifeCycleHookInfo.getValue(), "lifecycle hook type can not be null"));
//        lifecycleHookMsg.setResType(processor.getResType());
//        lifecycleHookMsg.setTableName(tableName);
//        return new IRemoteTaskTrigger() {
//            @Override
//            public String getTaskName() {
//                return Objects.requireNonNull(lifeCycleHookInfo.getKey(), "task name can not be null");
//            }
//
//            @Override
//            public void run() {
//                try {
//                    prePostConsumer.consumeMessage(lifecycleHookMsg);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        };
//    }

    @Override
    public ExecResult processExecHistoryRecord(ICommonDAOContext commonDAO, WorkFlowBuildHistory buildHistory) {

        WorkFlowBuildHistoryPayload buildHistoryPayload = new WorkFlowBuildHistoryPayload(buildHistory.getId(), commonDAO);
        Long powerJobWorkflowInstanceId = buildHistoryPayload.getPowerJobWorkflowInstanceId();

        PowerJobClient powerJobClient = getTISPowerJob();
        WorkflowInstanceInfoDTO workflowInstanceInfo = result(powerJobClient.fetchWorkflowInstanceInfo(powerJobWorkflowInstanceId));

        WorkflowInstanceStatus wfStatus = WorkflowInstanceStatus.of(workflowInstanceInfo.getStatus());
        if (WorkflowInstanceStatus.FINISHED_STATUS.contains(wfStatus.getV())) {
            ExecResult execResult = null;
            switch (wfStatus) {
                case SUCCEED:
                    execResult = (ExecResult.SUCCESS);
                    break;
                case FAILED:
                    execResult = (ExecResult.FAILD);
                    break;
                case STOPPED:
                    execResult = (ExecResult.CANCEL);
                    break;
                default:
                    throw new IllegalStateException("illegal status :" + wfStatus);
            }
            buildHistoryPayload.updateFinalStatus(execResult);
            return execResult;
        }

        return null;
    }

    @Override
    public InstanceType getType() {
        return InstanceType.DISTRIBUTE;
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

    @Override
    public TriggerBuildResult triggerJob(IControlMsgHandler module, Context context, String appName) {
        if (StringUtils.isEmpty(appName)) {
            throw new IllegalArgumentException("appName " + appName + " can not be empty");
        }
        PowerJobClient powerJobClient = getTISPowerJob();
        RpcServiceReference statusRpc = getStatusRpc();
        StatusRpcClientFactory.AssembleSvcCompsite feedback = statusRpc.get();
//        PowerJobClient powerJobClient = powerJob.getPowerJobClient();

        //  JobInfoQuery jobQuery = new JobInfoQuery();
        // jobQuery.setJobNameEq(appName);

        //  Long workflowId = 0l;

        ICommonDAOContext daoContext = getCommonDAOContext(module);

        ApplicationPayload appPayload = new ApplicationPayload(appName, daoContext.getApplicationDAO());
        ApplicationPayload.PowerJobWorkflow powerJobWorkflowId = appPayload.getPowerJobWorkflowId(false);
        if (powerJobWorkflowId == null || powerJobWorkflowId.isDisbaled(getTISPowerJob())) {
            // 如果之前还没有打开分布式调度，现在打开了，powerjob workflow还没有创建，现在创建
            appPayload = this.innerCreateJob(module, (DataxProcessor) DataxProcessor.load(null, appName));
            powerJobWorkflowId = appPayload.getPowerJobWorkflowId(true);
        }

        WorkflowInfoDTO wfInfo = result(powerJobClient.fetchWorkflow(powerJobWorkflowId.getWorkflowId()));

        PEWorkflowDAG wfDAG = wfInfo.getPEWorkflowDAG();

        List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs = Lists.newArrayList();
        for (PEWorkflowDAG.Node node : wfDAG.getNodes()) {
            triggerCfgs.add(SelectedTabTriggers.deserialize(JSONObject.parseObject(node.getNodeParams())));
        }


        PowerJobExecContext chainContext = new PowerJobExecContext();
        chainContext.setAppname(appName);
        chainContext.setWorkflowId(powerJobWorkflowId.getWorkflowId().intValue());
        chainContext.setExecutePhaseRange(powerJobWorkflowId.getExecutePhaseRange());
        // 创建 TIS的taskId
        Integer tisTaskId = IExecChainContext.createNewTask(chainContext);

        if (CollectionUtils.isEmpty(triggerCfgs)) {
            throw new IllegalStateException("powerjob workflowId:" + powerJobWorkflowId.getWorkflowId()
                    + " relevant nodes triggerCfgs can not be null empty");
        }

        PhaseStatusCollection statusCollection = createPhaseStatus(powerJobWorkflowId, triggerCfgs, tisTaskId);
        feedback.initSynJob(statusCollection);

        JSONObject instanceParams = new JSONObject();
        instanceParams.put(JobParams.KEY_TASK_ID, tisTaskId);
        instanceParams.put(JobParams.KEY_COLLECTION, appName);
        instanceParams.put(DataxUtils.EXEC_TIMESTAMP, TimeFormat.getCurrentTimeStamp());
        instanceParams.put(IFullBuildContext.DRY_RUN, false);
        // 取得powerjob instanceId
        Long workflowInstanceId = result(powerJobClient.runWorkflow(wfInfo.getId(), JsonUtil.toString(instanceParams), 0));
        logger.info("create workflow instanceId:{}", workflowInstanceId);

        WorkFlowBuildHistoryPayload buildHistoryPayload = new WorkFlowBuildHistoryPayload(tisTaskId, daoContext);

        buildHistoryPayload.setPowerJobWorkflowInstanceId(workflowInstanceId);

        TriggerBuildResult buildResult = new TriggerBuildResult(true);
        buildResult.taskid = tisTaskId;
        return buildResult;
    }

    private PhaseStatusCollection createPhaseStatus(ApplicationPayload.PowerJobWorkflow powerJobWorkflowId
            , List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs, Integer tisTaskId) {
        PhaseStatusCollection statusCollection = new PhaseStatusCollection(tisTaskId, powerJobWorkflowId.getExecutePhaseRange());
        DumpPhaseStatus dumpPhase = new DumpPhaseStatus(tisTaskId);
        JoinPhaseStatus joinPhase = new JoinPhaseStatus(tisTaskId);
        statusCollection.setDumpPhase(dumpPhase);
        statusCollection.setJoinPhase(joinPhase);

        for (SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg : triggerCfgs) {

            if (StringUtils.isNotEmpty(triggerCfg.getPreTrigger())) {
                setInitStatus(dumpPhase.getTable(triggerCfg.getPreTrigger()));
            }

            if (StringUtils.isNotEmpty(triggerCfg.getPostTrigger())) {
                setInitStatus(joinPhase.getTaskStatus(triggerCfg.getPostTrigger()));
            }

            for (CuratorDataXTaskMessage taskMsg : triggerCfg.getSplitTabsCfg()) {
                setInitStatus(dumpPhase.getTable(DataXJobInfo.parse(taskMsg.getJobName()).jobFileName));
            }

        }
        return statusCollection;
    }

    private void setInitStatus(AbstractChildProcessStatus status) {
        status.setFaild(false);
        status.setWaiting(true);
        status.setComplete(false);
    }

    private static ICommonDAOContext getCommonDAOContext(IControlMsgHandler module) {
        if (!(module instanceof ICommonDAOContext)) {
            throw new IllegalStateException("module must be type of " + ICommonDAOContext.class.getName());
        }
        ICommonDAOContext daoContext = (ICommonDAOContext) module;
        return daoContext;
    }

    private static PowerJobClient getTISPowerJob() {
        DataXJobWorker jobWorker = DataXJobWorker.getJobWorker(K8S_DATAX_INSTANCE_NAME);
        if (!(jobWorker instanceof ITISPowerJob)) {
            throw new IllegalStateException("jobWorker must be type of :" + ITISPowerJob.class);
        }

        ITISPowerJob powerJob = (ITISPowerJob) jobWorker;
        PowerJobClient powerJobClient = powerJob.getPowerJobClient();
        return powerJobClient;
    }

    public  static <T> T result(ResultDTO<T> result) {
        if (!result.isSuccess()) {
            throw new IllegalStateException("execute falid:" + result.getMessage());
        }
        return result.getData();
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
        this.innerCreateJob(module, dataxProcessor);
    }

    private ApplicationPayload innerCreateJob(IControlMsgHandler module, DataxProcessor dataxProcessor) {
        PowerJobClient powerJobClient = getTISPowerJob();


        K8SDataXPowerJobJobTemplate jobTpl
                = (K8SDataXPowerJobJobTemplate) DataXJobWorker.getJobWorker(K8S_DATAX_INSTANCE_NAME, Optional.of(DataXJobWorker.K8SWorkerCptType.JobTpl));


        PowerJobExecContext execChainContext = new PowerJobExecContext();
        PowerJobTskTriggers tskTriggers = new PowerJobTskTriggers();
        execChainContext.setAppname(dataxProcessor.identityValue());
        execChainContext.setTskTriggers(tskTriggers);
        execChainContext.setAttribute(JobCommon.KEY_TASK_ID, -1);

        RpcServiceReference statusRpc = new RpcServiceReference(new AtomicReference(), () -> {
        });

//        Pair<DAGSessionSpec, List<ISelectedTab>> dagSessionSpecs
//                = DAGSessionSpec.createDAGSessionSpec(execChainContext, statusRpc, dataxProcessor, this);
//        DAGSessionSpec dagSession = dagSessionSpec.getLeft();
//        List<ISelectedTab> selectedTabs = dagSessionSpec.getRight();
        // Map<String, IRemoteTaskTrigger> triggerDict = tskTriggers.getTriggerDict();
//        dagSession.buildSpec((dpt) -> {
//            System.out.println(dpt.getLeft() + "->" + dpt.getRight());
//        });

        List<SaveWorkflowNodeRequest> wfNodes = Lists.newArrayList();
        SaveWorkflowNodeRequest wfNode = null;
        Long jobId = null;
        SaveJobInfoRequest jobRequest = null;
        JobMap2WorkflowMaintainer jobIdMaintainer = new JobMap2WorkflowMaintainer();

        JSONObject mrParams = null;
//        JSONObject execCfg = null;
//        JSONArray dataxJobInfo = null;
//        DAGSessionSpec dumpSpec = null;
//        SelectedTabTriggers.PowerJobRemoteTaskTrigger splitTabTrigger = null;


        SelectedTabTriggers tabTriggers = null;
        //  Map<String, String> jobInfo = null;
        boolean containPostTrigger = false;
        for (ISelectedTab selectedTab : dataxProcessor.getReader(null).getSelectedTabs()) {
            tskTriggers = new PowerJobTskTriggers();
            execChainContext.setTskTriggers(tskTriggers);
            jobRequest = jobTpl.createDefaultJobInfoRequest(ExecuteType.MAP_REDUCE);
            jobRequest.setJobName(dataxProcessor.identityValue() + "_" + selectedTab.getName());

            DAGSessionSpec sessionSpec = new DAGSessionSpec();
            tabTriggers = DAGSessionSpec.buildTaskTriggers(execChainContext, dataxProcessor, this, statusRpc, selectedTab, selectedTab.getName(),
                    sessionSpec);
            mrParams = tabTriggers.createMRParams();

            if (tabTriggers.getPostTrigger() != null) {
                containPostTrigger = true;
            }

            jobRequest.setJobParams(JsonUtil.toString(mrParams));
            jobId = result(powerJobClient.saveJob(jobRequest));
            jobIdMaintainer.addJob(selectedTab, jobId);

            wfNode = new SaveWorkflowNodeRequest();
            wfNode.setJobId(jobId);
            wfNode.setNodeName(selectedTab.getName());
            wfNode.setEnable(true);
            wfNode.setNodeParams(jobRequest.getJobParams());
            // wfNode.setNodeParams(powerjobTrigger.getTskMsgSerialize());
            wfNode.setType(WorkflowNodeType.JOB.getCode());

            wfNodes.add(wfNode);
        }


        List<WorkflowNodeInfoDTO> savedWfNodes
                = result(powerJobClient.saveWorkflowNode(wfNodes));

        jobIdMaintainer.addWorkflow(savedWfNodes);
        ICommonDAOContext daoContext = getCommonDAOContext(module);
        ApplicationPayload appPayload = new ApplicationPayload(dataxProcessor.identityValue(), daoContext.getApplicationDAO());
        ApplicationPayload.PowerJobWorkflow powerWf = appPayload.getPowerJobWorkflowId(false);
        SaveWorkflowRequest req = new SaveWorkflowRequest();
        req.setId(powerWf != null ? powerWf.getWorkflowId() : null);
        req.setWfName(dataxProcessor.identityValue());
        req.setWfDescription(dataxProcessor.identityValue());
        req.setEnable(true);
        req.setTimeExpressionType(TimeExpressionType.API);


        PEWorkflowDAG peWorkflowDAG = jobIdMaintainer.createWorkflowDAG();
        req.setDag(peWorkflowDAG);
        Long saveWorkflowId = result(powerJobClient.saveWorkflow(req));

        if(powerWf == null){
            appPayload.setPowerJobWorkflowId(saveWorkflowId
                    , new ExecutePhaseRange(FullbuildPhase.FullDump, containPostTrigger ? FullbuildPhase.JOIN : FullbuildPhase.FullDump));
        }

        return appPayload;
    }

    @Override
    public IRemoteTaskTrigger createDataXJob(IDataXJobContext dataXJobContext
            , RpcServiceReference statusRpc, DataXJobInfo jobName, IDataxProcessor processor, CuratorDataXTaskMessage msg) {
        // AssembleSvcCompsite feedback =  statusRpc.get();
        SelectedTabTriggers.PowerJobRemoteTaskTrigger tskTrigger = new SelectedTabTriggers.PowerJobRemoteTaskTrigger(jobName, msg);

        return tskTrigger;
//        IJoinTaskContext taskContext = dataXJobContext.getTaskContext();
//        IAppSourcePipelineController pipelineController = taskContext.getPipelineController();
//      //  DistributedQueue<CuratorDataXTaskMessage> distributedQueue = getCuratorDistributedQueue();
//        // File jobPath = new File(dataxProcessor.getDataxCfgDir(null), dataXfileName);
//        return new AsynRemoteJobTrigger(jobName.jobFileName) {
//            @Override
//            public void run() {
//                try {
//                    //  IDataxReader reader = dataxProcessor.getReader(null);
//                  //  CuratorDataXTaskMessage msg = getDataXJobDTO(taskContext, jobName);
//                    distributedQueue.put(msg);
//                    pipelineController.registerAppSubExecNodeMetrixStatus(
//                            IAppSourcePipelineController.DATAX_FULL_PIPELINE + taskContext.getIndexName(), jobName.jobFileName);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//
//            @Override
//            public void cancel() {
//                pipelineController.stop(
//                        IAppSourcePipelineController.DATAX_FULL_PIPELINE + taskContext.getIndexName());
//            }
//        };
    }

//
//    private DistributedQueue<CuratorDataXTaskMessage> getCuratorDistributedQueue() {
//        synchronized (this) {
//            if (curatorClient != null && !curatorClient.getZookeeperClient().isConnected()) {
//                curatorClient.close();
//                curatorClient = null;
//                curatorDistributedQueue = null;
//            }
//            if (curatorDistributedQueue == null) {
//                DataXJobWorker dataxJobWorker = DataXJobWorker.getJobWorker(DataXJobWorker.K8S_DATAX_INSTANCE_NAME);
//                if (curatorClient == null) {
//                    this.curatorClient = DataXJobConsumer.getCuratorFramework(dataxJobWorker.getZookeeperAddress());
//                }
//                this.curatorDistributedQueue = DataXJobConsumer.createQueue(curatorClient, dataxJobWorker.getZkQueuePath(), null);
//            }
//            return this.curatorDistributedQueue;
//        }
//    }
}
