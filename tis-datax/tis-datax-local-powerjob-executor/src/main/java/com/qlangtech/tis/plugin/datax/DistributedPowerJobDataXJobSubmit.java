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
import com.google.common.collect.Maps;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataXPowerJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.ITISPowerJob;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobJobTemplate;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobOverwriteTemplate;
import com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.datax.powerjob.impl.TISWorkflowInfoDTO;
import com.qlangtech.tis.plugin.datax.powerjob.impl.WorkflowListResult;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.IDataFlowTopology;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.DAGSessionSpec;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
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

import static com.qlangtech.tis.datax.job.DataXJobWorker.K8S_DATAX_INSTANCE_NAME;
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

    //    private CuratorFramework curatorClient = null;
//    private DistributedQueue<CuratorDataXTaskMessage> curatorDistributedQueue = null;
    //  PowerJobClient powerJobClient;

    public final static String KEY_START_INITIALIZE_SUFFIX = "_start_initialize";

    private static final Logger logger = LoggerFactory.getLogger(DistributedPowerJobDataXJobSubmit.class);

    public DistributedPowerJobDataXJobSubmit() {
//        PowerJobClient powerJobClient = new PowerJobClient("127.0.0.1:7700", "powerjob-worker-samples", "powerjob123");
//        powerJobClient.
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
        void vistWorkerNode(PEWorkflowDAG.Node node);
    }

    public static void vistWorkflowNodes(String appName, WorkflowInfoDTO wfInfo, WorkflowVisit visitor) {


        PEWorkflowDAG wfDAG = wfInfo.getPEWorkflowDAG();

        // List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs = Lists.newArrayList();
        for (PEWorkflowDAG.Node node : wfDAG.getNodes()) {
            if ((appName + KEY_START_INITIALIZE_SUFFIX).equals(node.getNodeName())) {
                // 说明是 初始节点跳过
                visitor.vistStartInitNode(node);
            } else {
                visitor.vistWorkerNode(node);
            }

            // triggerCfgs.add(SelectedTabTriggers.deserialize(JSONObject.parseObject(node.getNodeParams())));
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
        // SqlTaskNodeMeta.SqlDataFlowTopology

        PowerWorkflowPayload appPayload = PowerWorkflowPayload.createApplicationPayload(this, module, dataxProcessor.identityValue());

        // appPayload.innerCreatePowerjobWorkflow();

        appPayload.innerCreatePowerjobWorkflow(
                Optional.empty(), Optional.empty(), this);
    }

    /**
     * @param module
     * @param context
     * @param topology
     */
    @Override
    public void createWorkflowJob(IControlMsgHandler module, Context context, IDataFlowTopology topology) {
        // List<DependencyNode> dumpNodes = topology.getDumpNodes();
        // List<ISqlTask> parseNodes = topology.getParseNodes();


//        DataFlowDataXProcessor dataxProcessor
//                = (DataFlowDataXProcessor) DataxProcessor.load(null, StoreResourceType.DataFlow, topology.getName());
//
//        JobMap2WorkflowMaintainer jobIdMaintainer = new JobMap2WorkflowMaintainer() {
//
//            public void addJob(ISelectedTab selectedTab, Long jobId) {
//                Objects.requireNonNull(jobId, "jobId can not be null");
//                if (!(selectedTab instanceof DataFlowDataXProcessor.TopologySelectedTab)) {
//                    throw new IllegalStateException("selectedTab:" + selectedTab.getClass().getName() + " must be type :"
//                            + DataFlowDataXProcessor.TopologySelectedTab.class.getName());
//                }
//                this.addDumpNode2JobIdMap(((DataFlowDataXProcessor.TopologySelectedTab) selectedTab).getTopologyId(), jobId);
//                //  this.jobName2JobId.put(, jobId);
//            }
//
//            @Override
//            protected PEWorkflowDAG createWorkflowDAG(List<PEWorkflowDAG.Node> nodes, List<PEWorkflowDAG.Edge> edges) {
//
//                IDAGSessionSpec dagSpec = topology.getDAGSessionSpec();
//
//                dagSpec.buildSpec((dpt) -> {
//                    edges.add(new PEWorkflowDAG.Edge(getWfIdByJobName(dpt.getLeft())
//                            , getWfIdByJobName(dpt.getRight())));
////                    System.out.println(dpt.getLeft() + "->" + dpt.getRight());
//                });
//
//                return super.createWorkflowDAG(nodes, edges);
//            }
//
//            @Override
//            public List<SaveWorkflowNodeRequest> beforeCreateWorkflowDAG(K8SDataXPowerJobJobTemplate jobTpl) {
//                // TODO: 更新时需要找到之前的node
//                List<SaveWorkflowNodeRequest> joinNodeReqs = Lists.newArrayList();
//                Optional<PEWorkflowDAG.Node> existWfNode = Optional.empty();
//                for (ISqlTask sqlTask : parseNodes) {
//
//                    try (StringWriter writer = new StringWriter()) {
//                        SqlTaskNodeMeta.persistSqlTask(writer, sqlTask);
//                        SaveJobInfoRequest sqlJoinRequest = jobTpl.createSqlProcessJobRequest();
//
//                        SaveWorkflowNodeRequest wfNodeReq = createWorkflowNode(dataxProcessor.identityValue() + "_" + sqlTask.getExportName()
//                                , String.valueOf(writer.getBuffer()), existWfNode, sqlJoinRequest, jobTpl);
//                        this.addJob(sqlTask, wfNodeReq.getJobId());
//                        joinNodeReqs.add(wfNodeReq);
//
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                return joinNodeReqs;
//            }
//        };

        PowerWorkflowPayload payload = PowerWorkflowPayload.createTISWorkflowPayload(this, module, topology);

        payload.innerCreatePowerjobWorkflow(
                Optional.empty() //
                , Optional.empty() //
                , this);


//        this.innerCreateJob(PowerWorkflowPayload.createTISWorkflowPayload(topology //
//                        , getCommonDAOContext(module)) //
//                , module, dataxProcessor, jobIdMaintainer //
//                , Optional.empty() //
//                , Optional.empty() //
//                , this, StatusRpcClientFactory.getMockStub());
    }


    @Override
    public TriggerBuildResult triggerWorkflowJob(
            IControlMsgHandler module, Context context,
            IWorkflow workflow, Boolean dryRun, Optional<Long> powerJobWorkflowInstanceIdOpt) {

        try {
            SqlTaskNodeMeta.SqlDataFlowTopology topology = SqlTaskNodeMeta.getSqlDataFlowTopology(workflow.getName());

            PowerWorkflowPayload payload = PowerWorkflowPayload.createTISWorkflowPayload(this, module, topology);
            PowerJobClient powerJobClient = getTISPowerJob();
            RpcServiceReference statusRpc = getStatusRpc();
            StatusRpcClientFactory.AssembleSvcCompsite feedback = statusRpc.get();
            return payload.triggerPowerjobWorkflow(powerJobWorkflowInstanceIdOpt, powerJobClient, statusRpc, feedback);
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
    public TriggerBuildResult triggerJob(IControlMsgHandler module, Context context, String appName, Optional<Long> workflowInstanceIdOpt) {
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


        PowerWorkflowPayload appPayload = PowerWorkflowPayload.createApplicationPayload(this, module, appName);
        return appPayload.triggerPowerjobWorkflow(workflowInstanceIdOpt, powerJobClient, statusRpc, feedback);
    }

//    public TriggerBuildResult triggerPowerjobWorkflow(IControlMsgHandler module, String appName, Optional<Long> workflowInstanceIdOpt, PowerJobClient powerJobClient, RpcServiceReference statusRpc, StatusRpcClientFactory.AssembleSvcCompsite feedback, ICommonDAOContext daoContext, PowerWorkflowPayload appPayload) {
//        PowerWorkflowPayload.PowerJobWorkflow powerJobWorkflowId = appPayload.getPowerJobWorkflowId(false);
//        DataxProcessor dataxProcessor = (DataxProcessor) DataxProcessor.load(null, appName);
//
//
//        Map<ISelectedTab, SelectedTabTriggers> selectedTabTriggers = null;
//        WorkflowUnEffectiveJudge unEffectiveJudge = null;
//        if (powerJobWorkflowId == null
////                || /**是否已经失效*/(unEffectiveJudge = powerJobWorkflowId.isUnEffective(
////                getTISPowerJob(), selectedTabTriggers = createWfNodes(dataxProcessor, this, statusRpc))).isUnEffective()
//        ) {
//            // 如果之前还没有打开分布式调度，现在打开了，powerjob workflow还没有创建，现在创建
//            appPayload = this.innerCreateJob(module, dataxProcessor, Optional.ofNullable(selectedTabTriggers), Optional.ofNullable(unEffectiveJudge), this, statusRpc);
//            powerJobWorkflowId = appPayload.getPowerJobWorkflowId(true);
//        }
//
//        WorkflowInfoDTO wfInfo = result(powerJobClient.fetchWorkflow(powerJobWorkflowId.getPowerjobWorkflowId()));
//
//        List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs = Lists.newArrayList();
//        vistWorkflowNodes(appName, wfInfo, new WorkflowVisit() {
//            @Override
//            public void vistStartInitNode(PEWorkflowDAG.Node node) {
//                return;
//            }
//
//            @Override
//            public void vistWorkerNode(PEWorkflowDAG.Node node) {
//                triggerCfgs.add(SelectedTabTriggers.deserialize(JSONObject.parseObject(node.getNodeParams())));
//            }
//        });
//
////        PEWorkflowDAG wfDAG = wfInfo.getPEWorkflowDAG();
////
////
////        for (PEWorkflowDAG.Node node : wfDAG.getNodes()) {
////            if ((appName + KEY_START_INITIALIZE_SUFFIX).equals(node.getNodeName())) {
////                // 说明是 初始节点跳过
////                continue;
////            }
////
////            triggerCfgs.add(SelectedTabTriggers.deserialize(JSONObject.parseObject(node.getNodeParams())));
////        }
//
//
//        PowerJobExecContext chainContext = new PowerJobExecContext();
//        chainContext.setAppname(appName);
//        chainContext.setWorkflowId(powerJobWorkflowId.getPowerjobWorkflowId().intValue());
//        chainContext.setExecutePhaseRange(powerJobWorkflowId.getExecutePhaseRange());
//        //
//        /**===================================================================
//         * 创建 TIS的taskId
//         ===================================================================*/
//        final Integer tisTaskId = IExecChainContext.createNewTask(
//                chainContext, workflowInstanceIdOpt.isPresent() ? TriggerType.CRONTAB : TriggerType.MANUAL);
//
//        if (CollectionUtils.isEmpty(triggerCfgs)) {
//            throw new IllegalStateException("powerjob workflowId:" + powerJobWorkflowId.getPowerjobWorkflowId()
//                    + " relevant nodes triggerCfgs can not be null empty");
//        }
//
//        PhaseStatusCollection statusCollection = createPhaseStatus(powerJobWorkflowId, triggerCfgs, tisTaskId);
//        feedback.initSynJob(statusCollection);
//
//        // 取得powerjob instanceId
//        Long workflowInstanceId = workflowInstanceIdOpt.orElseGet(() -> {
//            // 手动触发的情况
//            JSONObject instanceParams = IExecChainContext.createInstanceParams(tisTaskId, appName, false);// new JSONObject();
//            Long createWorkflowInstanceId = result(powerJobClient.runWorkflow(wfInfo.getId(), JsonUtil.toString(instanceParams), 0));
//            logger.info("create workflow instanceId:{}", createWorkflowInstanceId);
//            return createWorkflowInstanceId;
//        });
//
//
//        WorkFlowBuildHistoryPayload buildHistoryPayload = new WorkFlowBuildHistoryPayload(tisTaskId, daoContext);
//
//        buildHistoryPayload.setPowerJobWorkflowInstanceId(workflowInstanceId);
//
//        TriggerBuildResult buildResult = new TriggerBuildResult(true);
//        buildResult.taskid = tisTaskId;
//
//        initializeService(daoContext);
//        triggrWorkflowJobs.offer(buildHistoryPayload);
//        if (checkWorkflowJobsLock.tryLock()) {
//
//            scheduledExecutorService.schedule(() -> {
//                checkWorkflowJobsLock.lock();
//                try {
//                    int count = 0;
//                    WorkFlowBuildHistoryPayload pl = null;
//                    List<WorkFlowBuildHistoryPayload> checkWf = Lists.newArrayList();
//                    ExecResult execResult = null;
//                    while (true) {
//                        while ((pl = triggrWorkflowJobs.poll()) != null) {
//                            checkWf.add(pl);
//                            count++;
//                        }
//
//                        if (CollectionUtils.isEmpty(checkWf)) {
//                            logger.info("the turn all of the powerjob workflow job has been terminated,jobs count:{}", count);
//                            return;
//                        }
//
//                        // WorkflowInstanceInfoDTO wfStatus = null;
//                        Iterator<WorkFlowBuildHistoryPayload> it = checkWf.iterator();
//                        WorkFlowBuildHistoryPayload p;
//                        int allWfJobsCount = checkWf.size();
//                        int removed = 0;
//                        while (it.hasNext()) {
//                            p = it.next();
//                            if ((execResult = p.processExecHistoryRecord(powerJobClient)) != null) {
//                                // 说明结束了
//                                it.remove();
//                                removed++;
//                                // 正常结束？ 还是失败导致？
//                                if (execResult != ExecResult.SUCCESS) {
//
//                                }
//                                triggrWorkflowJobs.taskFinal(p, execResult);
//                            }
//                        }
//                        logger.info("start to wait next time to check job status,to terminate status count:{},allWfJobsCount:{}", removed, allWfJobsCount);
//                        try {
//                            Thread.sleep(4000);
//                        } catch (InterruptedException e) {
//                            throw new RuntimeException(e);
//                        }
//                    }
//                } finally {
//                    checkWorkflowJobsLock.unlock();
//                }
//            }, 5, TimeUnit.SECONDS);
//            checkWorkflowJobsLock.unlock();
//        }
//
//
//        return buildResult;
//    }


//    public  ExecResult processExecHistoryRecord(PowerJobClient powerJobClient, WorkFlowBuildHistoryPayload buildHistoryPayload) {
//
//        Long powerJobWorkflowInstanceId = buildHistoryPayload.getPowerJobWorkflowInstanceId();
//
//        WorkflowInstanceInfoDTO workflowInstanceInfo = result(powerJobClient.fetchWorkflowInstanceInfo(powerJobWorkflowInstanceId));
//
//        WorkflowInstanceStatus wfStatus = WorkflowInstanceStatus.of(workflowInstanceInfo.getStatus());
//        if (WorkflowInstanceStatus.FINISHED_STATUS.contains(wfStatus.getV())) {
//            ExecResult execResult = null;
//            switch (wfStatus) {
//                case SUCCEED:
//                    execResult = (ExecResult.SUCCESS);
//                    break;
//                case FAILED:
//                    execResult = (ExecResult.FAILD);
//                    break;
//                case STOPPED:
//                    execResult = (ExecResult.CANCEL);
//                    break;
//                default:
//                    throw new IllegalStateException("illegal status :" + wfStatus);
//            }
//            buildHistoryPayload.updateFinalStatus(execResult);
//            return execResult;
//        }
//
//        return null;
//    }


    public static ICommonDAOContext getCommonDAOContext(IControlMsgHandler module) {
        if (!(module instanceof ICommonDAOContext)) {
            throw new IllegalStateException("module must be type of " + ICommonDAOContext.class.getName());
        }
        ICommonDAOContext daoContext = (ICommonDAOContext) module;
        return daoContext;
    }

    public static TISPowerJobClient getTISPowerJob() {
        DataXJobWorker jobWorker = DataXJobWorker.getJobWorker(K8S_DATAX_INSTANCE_NAME);
        if (!(jobWorker instanceof ITISPowerJob)) {
            throw new IllegalStateException("jobWorker must be type of :" + ITISPowerJob.class);
        }

        ITISPowerJob powerJob = (ITISPowerJob) jobWorker;
        TISPowerJobClient powerJobClient = powerJob.getPowerJobClient();
        return powerJobClient;
    }


    @Override
    public TISWorkflowInfoDTO saveJob(IControlMsgHandler module, Context context, DataxProcessor dataxProcessor) {

        PowerWorkflowPayload appPayload = PowerWorkflowPayload.createApplicationPayload(this, module, dataxProcessor.identityValue());

//        appPayload.innerSaveJob();

        PowerWorkflowPayload.PowerJobWorkflow powerJobWorkflowId
                = Objects.requireNonNull(appPayload.getPowerJobWorkflowId(false), "powerJobWorkflowId can not be null");

        Map<ISelectedTab, SelectedTabTriggers> selectedTabTriggers = null;
        WorkflowUnEffectiveJudge unEffectiveJudge = null;
        RpcServiceReference rpcStub = StatusRpcClientFactory.getMockStub();
        DataXJobWorker worker = Objects.requireNonNull(getAppRelevantDataXJobWorkerTemplate(dataxProcessor), "worker can not be empty");
        selectedTabTriggers = createWfNodes(dataxProcessor, this, rpcStub);
        unEffectiveJudge = powerJobWorkflowId.isUnEffective(getTISPowerJob(), selectedTabTriggers);

//        this.innerSaveJob(appPayload
//                , module, dataxProcessor, Optional.of(selectedTabTriggers)
//                , Optional.of(unEffectiveJudge), (K8SDataXPowerJobOverwriteTemplate) worker, this, rpcStub);

        appPayload.innerSaveJob(Optional.of(selectedTabTriggers)
                , Optional.of(unEffectiveJudge), (K8SDataXPowerJobOverwriteTemplate) worker, this, rpcStub);

        return (TISWorkflowInfoDTO) result(getTISPowerJob().fetchWorkflow(powerJobWorkflowId.getPowerjobWorkflowId()));
    }

    private static K8SDataXPowerJobJobTemplate getAppRelevantDataXJobWorkerTemplate(IDataxProcessor dataxProcessor) {
        for (DataXJobWorker worker : HeteroEnum.appJobWorkerTplReWriter.getPlugins(IPluginContext.namedContext(dataxProcessor.identityValue()), null)) {
            return (K8SDataXPowerJobJobTemplate) worker;
        }
        return null;
    }


//    private PowerWorkflowPayload innerCreateJob(IControlMsgHandler module
//            , DataxProcessor dataxProcessor
//            , Optional<Map<ISelectedTab, SelectedTabTriggers>> selectedTabTriggers //
//            , Optional<WorkflowUnEffectiveJudge> unEffectiveOpt, DataXJobSubmit submit, RpcServiceReference statusRpc) {
//
//
//        return this.innerCreateJob(PowerWorkflowPayload.createApplicationPayload(module, dataxProcessor.identityValue(), getCommonDAOContext(module))
//                , module, dataxProcessor, new JobMap2WorkflowMaintainer(), selectedTabTriggers, unEffectiveOpt, submit, statusRpc);
//    }

//    private PowerWorkflowPayload innerCreateJob(PowerWorkflowPayload appPayload, IControlMsgHandler module
//            , IDataxProcessor dataxProcessor
//            , JobMap2WorkflowMaintainer jobIdMaintainer
//            , Optional<Map<ISelectedTab, SelectedTabTriggers>> selectedTabTriggers //
//            , Optional<WorkflowUnEffectiveJudge> unEffectiveOpt, DataXJobSubmit submit, RpcServiceReference statusRpc) {
////        K8SDataXPowerJobJobTemplate jobTpl
////                = (K8SDataXPowerJobJobTemplate) DataXJobWorker.getJobWorker(K8S_DATAX_INSTANCE_NAME, Optional.of(DataXJobWorker.K8SWorkerCptType.JobTpl));
//        // dataxApp 相关的模版
//        Optional<K8SDataXPowerJobJobTemplate> jobTpl = Optional.ofNullable(getAppRelevantDataXJobWorkerTemplate(dataxProcessor));
//
//        return innerSaveJob(appPayload, module, dataxProcessor, jobIdMaintainer, selectedTabTriggers, unEffectiveOpt, jobTpl.orElseGet(() -> {
//            // 为空调用全局模版
//            return (K8SDataXPowerJobJobTemplate) DataXJobWorker.getJobWorker(K8S_DATAX_INSTANCE_NAME, Optional.of(DataXJobWorker.K8SWorkerCptType.JobTpl));
//        }), submit, statusRpc);
//    }

//    private static PowerWorkflowPayload innerSaveJob(PowerWorkflowPayload appPayload, IControlMsgHandler module
//            , DataxProcessor dataxProcessor
//            , Optional<Map<ISelectedTab, SelectedTabTriggers>> selectedTabTriggers, Optional<WorkflowUnEffectiveJudge> unEffectiveOpt
//            , K8SDataXPowerJobJobTemplate jobTpl, DataXJobSubmit submit, RpcServiceReference statusRpc) {
//        return innerSaveJob(appPayload, module, dataxProcessor, new JobMap2WorkflowMaintainer(), selectedTabTriggers, unEffectiveOpt, jobTpl, submit, statusRpc);
//    }

//    /**
//     * 执行Powerjob工作流保存流程
//     *
//     * @param module
//     * @param dataxProcessor
//     * @param selectedTabTriggers
//     * @param unEffectiveOpt
//     * @param jobTpl
//     * @param submit
//     * @param statusRpc
//     * @return
//     */
//    private static PowerWorkflowPayload innerSaveJob(PowerWorkflowPayload appPayload, IControlMsgHandler module
//            , IDataxProcessor dataxProcessor
//            , JobMap2WorkflowMaintainer jobIdMaintainer
//            , Optional<Map<ISelectedTab, SelectedTabTriggers>> selectedTabTriggers, Optional<WorkflowUnEffectiveJudge> unEffectiveOpt
//            , K8SDataXPowerJobJobTemplate jobTpl, DataXJobSubmit submit, RpcServiceReference statusRpc) {
//        PowerJobClient powerJobClient = getTISPowerJob();
//
//
//        Map<ISelectedTab, SelectedTabTriggers> createWfNodesResult = selectedTabTriggers.orElseGet(() -> createWfNodes(dataxProcessor, submit, statusRpc));
//
//        WorkflowUnEffectiveJudge unEffectiveJudge = unEffectiveOpt.orElseGet(() -> new WorkflowUnEffectiveJudge());
//
//        JSONObject mrParams = null;
//
//        SelectedTabTriggers tabTriggers = null;
//        ISelectedTab selectedTab = null;
//
//        boolean containPostTrigger = false;
//
//        // Long jobId = null;
//        List<SaveWorkflowNodeRequest> wfNodes = Lists.newArrayList();
//        Optional<PEWorkflowDAG.Node> changedWfNode = null;
//        for (Map.Entry<ISelectedTab, SelectedTabTriggers> entry : createWfNodesResult.entrySet()) {
//            tabTriggers = entry.getValue();
//            selectedTab = entry.getKey();
//            mrParams = tabTriggers.createMRParams();
//
//            if (tabTriggers.getPostTrigger() != null) {
//                containPostTrigger = true;
//            }
//
//            SaveJobInfoRequest jobRequest = jobTpl.createSynJobRequest();
//            changedWfNode = unEffectiveJudge.getExistWfNode(selectedTab.getName());
//
//            SaveWorkflowNodeRequest wfNode = createWorkflowNode(dataxProcessor.identityValue() + "_" + selectedTab.getName()
//                    , JsonUtil.toString(mrParams), changedWfNode, jobRequest, jobTpl);
//            wfNodes.add(wfNode);
//            jobIdMaintainer.addJob(selectedTab, wfNode.getJobId());
//        }
//
//
//        //===============================================================
//        // process startNode
//        final String startNodeName = dataxProcessor.identityValue() + KEY_START_INITIALIZE_SUFFIX;
//
//        final SaveJobInfoRequest initJobRequest = jobTpl.createInitializeJobRequest();
//        unEffectiveJudge.getStartInitNode().ifPresent((existStarNode) -> {
//            initJobRequest.setId(existStarNode.getJobId());
//        });
//        initJobRequest.setJobName(startNodeName);
//        JSONObject initNode = new JSONObject();
//        initNode.put(DataxUtils.DATAX_NAME, dataxProcessor.identityValue());
//        // 是否是dataflow的处理类型
//        initNode.put(DataxUtils.TIS_WORK_FLOW_CHANNEL, dataxProcessor.getResType() == StoreResourceType.DataFlow);
//        initJobRequest.setJobParams(JsonUtil.toString(initNode));
//
//        SaveWorkflowNodeRequest startWfNode = jobTpl.createWorkflowNode();
//        startWfNode.setJobId(result(powerJobClient.saveJob(initJobRequest)));
//        startWfNode.setNodeName(startNodeName);
//        startWfNode.setNodeParams(initJobRequest.getJobParams());
//
//        wfNodes.add(startWfNode);
//        //===============================================================
//        wfNodes.addAll(jobIdMaintainer.beforeCreateWorkflowDAG(jobTpl));
//        List<WorkflowNodeInfoDTO> savedWfNodes
//                = result(powerJobClient.saveWorkflowNode(wfNodes));
//
//
//        for (PEWorkflowDAG.Node deleteNode : unEffectiveJudge.getDeletedWfNodes()) {
//            result(powerJobClient.deleteJob(deleteNode.getJobId()));
//        }
//
//        jobIdMaintainer.setStartInitJob(savedWfNodes.stream().filter((n) -> startWfNode.getJobId() == (long) n.getJobId()).findFirst());
//        jobIdMaintainer.addWorkflow(savedWfNodes);
//
//        PowerWorkflowPayload.PowerJobWorkflow powerWf = appPayload.getPowerJobWorkflowId(false);
//
//        SaveWorkflowRequest req = jobTpl.createWorkflowRequest(dataxProcessor);
//
//        req.setId(powerWf != null ? powerWf.getPowerjobWorkflowId() : null);
//
//        PEWorkflowDAG peWorkflowDAG = jobIdMaintainer.createWorkflowDAG();
//        req.setDag(peWorkflowDAG);
//        Long saveWorkflowId = result(powerJobClient.saveWorkflow(req));
//
//        if (powerWf == null) {
//            appPayload.setPowerJobWorkflowId(saveWorkflowId
//                    , new ExecutePhaseRange(FullbuildPhase.FullDump, containPostTrigger ? FullbuildPhase.JOIN : FullbuildPhase.FullDump));
//        }
//
//        return appPayload;
//    }

    public static SaveWorkflowNodeRequest createWorkflowNode(String name, String jobParams
            , Optional<PEWorkflowDAG.Node> existWfNode, SaveJobInfoRequest jobRequest, K8SDataXPowerJobJobTemplate jobTpl) {

        PowerJobClient powerJobClient = getTISPowerJob();
        existWfNode.ifPresent((node) -> {
            jobRequest.setId(node.getJobId());
        });
//dataxProcessor.identityValue() + "_" + selectedTab.getName()
        jobRequest.setJobName(name);
        //  JsonUtil.toString(mrParams)
        jobRequest.setJobParams(jobParams);

        Long jobId = result(powerJobClient.saveJob(jobRequest));
        //jobIdMaintainer.addJob(selectedTab, jobId);

        SaveWorkflowNodeRequest wfNode = jobTpl.createWorkflowNode();// new SaveWorkflowNodeRequest();
        existWfNode.ifPresent((node) -> {
            wfNode.setId(node.getNodeId());
        });
        wfNode.setJobId(jobId);
        wfNode.setNodeName(name);
        wfNode.setNodeParams(jobRequest.getJobParams());
        return wfNode;
    }

    public static Map<ISelectedTab, SelectedTabTriggers>
    createWfNodes(IDataxProcessor dataxProcessor, DataXJobSubmit submit, RpcServiceReference statusRpc) {
        // JobMap2WorkflowMaintainer jobIdMaintainer = new JobMap2WorkflowMaintainer();
//        K8SDataXPowerJobJobTemplate jobTpl
//                = (K8SDataXPowerJobJobTemplate) DataXJobWorker.getJobWorker(K8S_DATAX_INSTANCE_NAME, Optional.of(DataXJobWorker.K8SWorkerCptType.JobTpl));
        Map<ISelectedTab, SelectedTabTriggers> selectedTabTriggers = Maps.newHashMap();
        PowerJobExecContext execChainContext = null;

        //  List<SaveWorkflowNodeRequest> wfNodes = Lists.newArrayList();
        SelectedTabTriggers tabTriggers = null;
        // SaveJobInfoRequest jobRequest = null;
        PowerJobTskTriggers tskTriggers = null;
//        Long jobId = null;
//        boolean containPostTrigger = false;
//        JSONObject mrParams = null;
//        SaveWorkflowNodeRequest wfNode = null;

        for (IDataxReader reader : dataxProcessor.getReaders(null)) {
            for (ISelectedTab selectedTab : reader.getSelectedTabs()) {

                execChainContext = new PowerJobExecContext();
                tskTriggers = new PowerJobTskTriggers();
                execChainContext.setAppname(dataxProcessor.identityValue());
                execChainContext.setTskTriggers(tskTriggers);
                execChainContext.setAttribute(JobCommon.KEY_TASK_ID, -1);


                tskTriggers = new PowerJobTskTriggers();
                execChainContext.setTskTriggers(tskTriggers);


                DAGSessionSpec sessionSpec = new DAGSessionSpec();
                tabTriggers = DAGSessionSpec.buildTaskTriggers(
                        execChainContext, dataxProcessor, submit, statusRpc, selectedTab, selectedTab.getName(),
                        sessionSpec);

                selectedTabTriggers.put(selectedTab, tabTriggers);
            }
        }
        return selectedTabTriggers;
        // return Triple.of(wfNodes, jobIdMaintainer, containPostTrigger ? FullbuildPhase.JOIN : FullbuildPhase.FullDump);
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
