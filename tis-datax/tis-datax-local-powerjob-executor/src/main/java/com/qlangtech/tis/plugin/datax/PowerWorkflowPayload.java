package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.assemble.TriggerType;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.PowerjobTriggerBuildResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.phasestatus.PhaseStatusCollection;
import com.qlangtech.tis.fullbuild.phasestatus.impl.AbstractChildProcessStatus;
import com.qlangtech.tis.fullbuild.phasestatus.impl.DumpPhaseStatus;
import com.qlangtech.tis.fullbuild.phasestatus.impl.JoinPhaseStatus;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.biz.dal.pojo.ApplicationCriteria;
import com.qlangtech.tis.manage.common.CreateNewTaskResult;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobJobTemplate;
import com.qlangtech.tis.plugin.datax.powerjob.PowerJobWrokerMemorySpec;
import com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.IDAGSessionSpec;
import com.qlangtech.tis.powerjob.IDataFlowTopology;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.realtime.yarn.rpc.SynResTarget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.DAGSessionSpec;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.workflow.dao.IWorkFlowDAO;
import com.qlangtech.tis.workflow.pojo.WorkFlow;
import com.qlangtech.tis.workflow.pojo.WorkFlowCriteria;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import com.tis.hadoop.rpc.StatusRpcClientFactory.AssembleSvcCompsite;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.request.http.SaveJobInfoRequest;
import tech.powerjob.common.request.http.SaveWorkflowNodeRequest;
import tech.powerjob.common.request.http.SaveWorkflowRequest;
import tech.powerjob.common.response.WorkflowInfoDTO;
import tech.powerjob.common.response.WorkflowNodeInfoDTO;

import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.qlangtech.tis.fullbuild.IFullBuildContext.KEY_WORKFLOW_ID;
import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.KEY_START_INITIALIZE_SUFFIX;
import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.createWorkflowNode;
import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.getCommonDAOContext;
import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.vistWorkflowNodes;
import static com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient.result;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/2
 */
public abstract class PowerWorkflowPayload {

    private static final Logger logger = LoggerFactory.getLogger(PowerWorkflowPayload.class);
    protected final IDataxProcessor dataxProcessor;
    protected final JobMap2WorkflowMaintainer jobIdMaintainer;
    protected final IControlMsgHandler module;
    protected final DistributedPowerJobDataXJobSubmit submit;
    protected final ICommonDAOContext commonDAOContext;
    private static final String EXEC_RANGE = "execRange";

    public PowerWorkflowPayload(DistributedPowerJobDataXJobSubmit submit, IControlMsgHandler module, IDataxProcessor dataxProcessor
            , ICommonDAOContext commonDAOContext, JobMap2WorkflowMaintainer jobIdMaintainer) {
        this.module = module;
        this.dataxProcessor = dataxProcessor;
        this.jobIdMaintainer = jobIdMaintainer;
        this.submit = submit;
        this.commonDAOContext = commonDAOContext;
    }

    public static PowerWorkflowPayload createApplicationPayload(DistributedPowerJobDataXJobSubmit submit, IControlMsgHandler module, String appName) {
        ICommonDAOContext commonDAOContext = getCommonDAOContext(module);
//        if (!(module instanceof IPluginContext)) {
//            throw new IllegalStateException("type of module:" + module.getClass() + " must be type of " + IPluginContext.class);
//        }
        DataxProcessor dataxProcessor = (DataxProcessor) DataxProcessor.load(null, appName);
        return new ApplicationPayload(submit, module, appName, commonDAOContext, dataxProcessor);
    }

    public static PowerWorkflowPayload createTISWorkflowPayload(DistributedPowerJobDataXJobSubmit submit, IControlMsgHandler module, IDataFlowTopology topology) {

        ICommonDAOContext commonDAOContext = getCommonDAOContext(module);

        List<ISqlTask> parseNodes = topology.getParseNodes();
        JobMap2WorkflowMaintainer jobIdMaintainer = new JobMap2WorkflowMaintainer() {

            public void addJob(ISelectedTab selectedTab, Long jobId) {
                Objects.requireNonNull(jobId, "jobId can not be null");
                if (!(selectedTab instanceof DataFlowDataXProcessor.TopologySelectedTab)) {
                    throw new IllegalStateException("selectedTab:" + selectedTab.getClass().getName() + " must be type :"
                            + DataFlowDataXProcessor.TopologySelectedTab.class.getName());
                }
                this.addDumpNode2JobIdMap(((DataFlowDataXProcessor.TopologySelectedTab) selectedTab).getTopologyId(), jobId);
            }

            @Override
            protected PEWorkflowDAG createWorkflowDAG(List<PEWorkflowDAG.Node> nodes, List<PEWorkflowDAG.Edge> edges) {

                IDAGSessionSpec dagSpec = topology.getDAGSessionSpec();

                dagSpec.buildSpec((dpt) -> {
                    edges.add(new PEWorkflowDAG.Edge(getWfIdByJobName(dpt.getLeft())
                            , getWfIdByJobName(dpt.getRight())));
//                    System.out.println(dpt.getLeft() + "->" + dpt.getRight());
                });

                return super.createWorkflowDAG(nodes, edges);
            }

            @Override
            public List<SaveWorkflowNodeRequest> beforeCreateWorkflowDAG(K8SDataXPowerJobJobTemplate jobTpl) {
                // TODO: 更新时需要找到之前的node
                List<SaveWorkflowNodeRequest> joinNodeReqs = Lists.newArrayList();
                Optional<PEWorkflowDAG.Node> existWfNode = Optional.empty();
                for (ISqlTask sqlTask : parseNodes) {

                    try (StringWriter writer = new StringWriter()) {
                        SqlTaskNodeMeta.persistSqlTask(writer, sqlTask);
                        SaveJobInfoRequest sqlJoinRequest = jobTpl.createSqlProcessJobRequest();
                        JSONObject wfNodeParams = ISqlTask.json(sqlTask);

                        SaveWorkflowNodeRequest wfNodeReq = createWorkflowNode(//
                                submit.getTISPowerJob(), topology.getName() + "_" + sqlTask.getExportName()
                                , String.valueOf(writer.getBuffer()), Optional.of(JsonUtil.toString(wfNodeParams)), existWfNode, sqlJoinRequest, jobTpl);
                        this.addJob(sqlTask, wfNodeReq.getJobId());
                        joinNodeReqs.add(wfNodeReq);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return joinNodeReqs;
            }
        };


        return new TISWorkflowPayload(submit, module, topology.getName(), commonDAOContext
                , (DataFlowDataXProcessor) DataxProcessor.load(null, StoreResourceType.DataFlow, topology.getName()), jobIdMaintainer);
    }


    private Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>
    createWfNodes() {
        RpcServiceReference rpcStub = StatusRpcClientFactory.getMockStub();
        Map<ISelectedTab, SelectedTabTriggers> selectedTabTriggers = Maps.newHashMap();
        PowerJobExecContext execChainContext = null;

        SelectedTabTriggers tabTriggers = null;
        PowerJobTskTriggers tskTriggers = null;

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
                        execChainContext, dataxProcessor, submit, rpcStub, selectedTab, selectedTab.getName(),
                        sessionSpec);

                selectedTabTriggers.put(selectedTab, tabTriggers);
            }
        }

        if (dataxProcessor.getResType() == StoreResourceType.DataFlow) {
            dataxProcessor.identityValue();
        }

        return Pair.of(selectedTabTriggers, Collections.emptyMap());
    }

    /**
     * 保存powerjob workflow
     *
     * @return
     */
    public PowerWorkflowPayload.PowerJobWorkflow saveJob() {
        PowerWorkflowPayload.PowerJobWorkflow powerJobWorkflowId = this.getPowerJobWorkflowId(false);


        RpcServiceReference rpcStub = StatusRpcClientFactory.getMockStub();
        K8SDataXPowerJobJobTemplate worker = Objects.requireNonNull(K8SDataXPowerJobJobTemplate.getAppRelevantDataXJobWorkerTemplate(dataxProcessor), "worker can not be empty");
        Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> selectedTabTriggers = createWfNodes();


        WorkflowUnEffectiveJudge unEffectiveJudge = null;

        if (powerJobWorkflowId != null) {
            unEffectiveJudge = powerJobWorkflowId.isUnEffective(getTISPowerJob(), selectedTabTriggers);
        }
        /**
         * =========================
         *innerSaveJob
         * =========================
         */
        this.innerSaveJob(Optional.of(selectedTabTriggers)
                , Optional.ofNullable(unEffectiveJudge), worker, rpcStub);
        if (powerJobWorkflowId == null) {
            powerJobWorkflowId = this.getPowerJobWorkflowId(true);
        }
        return powerJobWorkflowId;
    }

    public final TISPowerJobClient getTISPowerJob() {
        return this.submit.getTISPowerJob();
    }

    public PowerjobTriggerBuildResult triggerPowerjobWorkflow(ICommonDAOContext daoContext, Optional<Long> workflowInstanceIdOpt
            , RpcServiceReference statusRpc, StatusRpcClientFactory.AssembleSvcCompsite feedback) {
        Objects.requireNonNull(statusRpc, "statusRpc can not be null");
        PowerWorkflowPayload.PowerJobWorkflow powerJobWorkflowId = this.getPowerJobWorkflowId(false);

        Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> selectedTabTriggers = null;
        WorkflowUnEffectiveJudge unEffectiveJudge = null;
        if (powerJobWorkflowId == null || powerJobWorkflowId.isDisabled()
//                || /**是否已经失效*/(unEffectiveJudge = powerJobWorkflowId.isUnEffective(
//                getTISPowerJob(), selectedTabTriggers = createWfNodes())).isUnEffective()
        ) {
//            if () {
            if (powerJobWorkflowId != null) {
                /**是否已经失效*/(unEffectiveJudge = powerJobWorkflowId.isUnEffective(
                        getTISPowerJob(), selectedTabTriggers = createWfNodes())).isUnEffective();
            }
            // 如果之前还没有打开分布式调度，现在打开了，powerjob workflow还没有创建，现在创建
            this.innerCreatePowerjobWorkflow(Optional.ofNullable(selectedTabTriggers), Optional.ofNullable(unEffectiveJudge));
            powerJobWorkflowId = this.getPowerJobWorkflowId(true);
            //}

        }


        WorkflowInfoDTO wfInfo = result(this.submit.getTISPowerJob().fetchWorkflow(powerJobWorkflowId.getPowerjobWorkflowId()));

        final List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs = Lists.newArrayList();
        final List<ISqlTask.SqlTaskCfg> joinNodeCfgs = Lists.newArrayList();
        vistWorkflowNodes(this.dataxProcessor.identityValue(), wfInfo, new DistributedPowerJobDataXJobSubmit.WorkflowVisit() {
            @Override
            public void vistStartInitNode(PEWorkflowDAG.Node node) {
                return;
            }

            @Override
            public void vistJoinWorkerNode(ISqlTask.SqlTaskCfg cfg, PEWorkflowDAG.Node node) {
                joinNodeCfgs.add(cfg);
            }

            @Override
            public void vistDumpWorkerNode(PEWorkflowDAG.Node node) {
                triggerCfgs.add(SelectedTabTriggers.deserialize(JSONObject.parseObject(node.getNodeParams())));
            }
        });


        PowerJobExecContext chainContext = createPowerJobExecContext();
        chainContext.setExecutePhaseRange(powerJobWorkflowId.getExecutePhaseRange());
        //
        /**===================================================================
         * 创建 TIS的taskId
         ===================================================================*/
        CreateNewTaskResult newTaskResult
                = daoContext.createNewDataXTask(chainContext, workflowInstanceIdOpt.isPresent() ? TriggerType.CRONTAB : TriggerType.MANUAL);

        final Integer tisTaskId = newTaskResult.getTaskid();

        if (CollectionUtils.isEmpty(triggerCfgs)) {
            throw new IllegalStateException("powerjob workflowId:" + powerJobWorkflowId.getPowerjobWorkflowId()
                    + " relevant nodes triggerCfgs can not be null empty");
        }

        PhaseStatusCollection statusCollection = createPhaseStatus(powerJobWorkflowId, triggerCfgs, joinNodeCfgs, tisTaskId);
        feedback.initSynJob(statusCollection);

        JSONObject instanceParams = createInstanceParams(statusRpc, tisTaskId);
        // 取得powerjob instanceId
        Long workflowInstanceId = workflowInstanceIdOpt.orElseGet(() -> {
            /****************************************
             * 手动触发的情况
             ****************************************/
            Long createWorkflowInstanceId = result(this.submit.getTISPowerJob().runWorkflow(wfInfo.getId(), JsonUtil.toString(instanceParams), 0));
            logger.info("create workflow instanceId:{}", createWorkflowInstanceId);
            return createWorkflowInstanceId;
        });


        WorkFlowBuildHistoryPayload buildHistoryPayload = new WorkFlowBuildHistoryPayload(tisTaskId, this.commonDAOContext);

        buildHistoryPayload.setPowerJobWorkflowInstanceId(workflowInstanceId);

        PowerjobTriggerBuildResult buildResult = new PowerjobTriggerBuildResult(true, instanceParams);
        buildResult.taskid = tisTaskId;

        initializeService(commonDAOContext);
        triggrWorkflowJobs.offer(buildHistoryPayload);
        if (checkWorkflowJobsLock.tryLock()) {

            scheduledExecutorService.schedule(() -> {
                checkWorkflowJobsLock.lock();
                try {
                    int count = 0;
                    WorkFlowBuildHistoryPayload pl = null;
                    List<WorkFlowBuildHistoryPayload> checkWf = Lists.newArrayList();
                    ExecResult execResult = null;
                    TISPowerJobClient powerJobClient = this.getTISPowerJob();
                    while (true) {
                        while ((pl = triggrWorkflowJobs.poll()) != null) {
                            checkWf.add(pl);
                            count++;
                        }

                        if (CollectionUtils.isEmpty(checkWf)) {
                            logger.info("the turn all of the powerjob workflow job has been terminated,jobs count:{}", count);
                            return;
                        }

                        // WorkflowInstanceInfoDTO wfStatus = null;
                        Iterator<WorkFlowBuildHistoryPayload> it = checkWf.iterator();
                        WorkFlowBuildHistoryPayload p;
                        int allWfJobsCount = checkWf.size();
                        int removed = 0;
                        while (it.hasNext()) {
                            p = it.next();
                            if ((execResult = p.processExecHistoryRecord(powerJobClient)) != null) {
                                // 说明结束了
                                it.remove();
                                removed++;
                                // 正常结束？ 还是失败导致？
                                if (execResult != ExecResult.SUCCESS) {

                                }
                                triggrWorkflowJobs.taskFinal(p, execResult);
                            }
                        }
                        logger.info("start to wait next time to check job status,to terminate status count:{},allWfJobsCount:{}", removed, allWfJobsCount);
                        try {
                            Thread.sleep(4000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } finally {
                    checkWorkflowJobsLock.unlock();
                }
            }, 5, TimeUnit.SECONDS);
            checkWorkflowJobsLock.unlock();
        }


        return buildResult;
    }

    protected final JSONObject createInstanceParams(RpcServiceReference statusRpc, Integer tisTaskId) {
        try {
            JSONObject instanceParams = IExecChainContext.createInstanceParams(tisTaskId, dataxProcessor, false, Optional.empty());

            DataXJobWorker worker = DataXJobWorker.getK8SDataXPowerJobWorker();
            ReplicasSpec replicasSpec = worker.getReplicasSpec();
            instanceParams.put(JobParams.KEY_JAVA_MEMORY_SPEC
                    , replicasSpec.toJavaMemorySpec(Optional.of(PowerJobWrokerMemorySpec.dataXExecutorMemoryProportion())));


            AssembleSvcCompsite svc = statusRpc.get();
            PhaseStatusCollection statusCollection
                    = svc.statReceiveSvc.loadPhaseStatusFromLatest(SynResTarget.pipeline(dataxProcessor.identityValue()));
            if (statusCollection != null) {

               // DumpPhaseStatus dumpPhase = statusCollection.getDumpPhase();

            }


            return instanceParams;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract PowerJobExecContext createPowerJobExecContext();

    private PhaseStatusCollection createPhaseStatus(PowerWorkflowPayload.PowerJobWorkflow powerJobWorkflowId
            , List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs //
            , List<ISqlTask.SqlTaskCfg> joinNodeCfgs //
            , Integer tisTaskId) {
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

        for (ISqlTask.SqlTaskCfg joinTskCfg //
                : Objects.requireNonNull(joinNodeCfgs, "joinNodeCfgs can not be null")) {
            setInitStatus(joinPhase.getTaskStatus(joinTskCfg.getExportName()));
        }

        return statusCollection;
    }

    private void setInitStatus(AbstractChildProcessStatus status) {
        status.setFaild(false);
        status.setWaiting(true);
        status.setComplete(false);
    }

    private static void initializeService(ICommonDAOContext daoContext) {
        try {
            if (scheduledExecutorService == null || triggrWorkflowJobs == null) {
                synchronized (DistributedPowerJobDataXJobSubmit.class) {
                    if (scheduledExecutorService == null) {
                        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
                    }

                    if (triggrWorkflowJobs == null) {
                        triggrWorkflowJobs = TriggrWorkflowJobs.create(daoContext);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private transient static ScheduledExecutorService scheduledExecutorService;
    private transient static final ReentrantLock checkWorkflowJobsLock = new ReentrantLock();
    private transient static TriggrWorkflowJobs triggrWorkflowJobs;

    public void innerCreatePowerjobWorkflow(

            Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>> selectedTabTriggers //
            , Optional<WorkflowUnEffectiveJudge> unEffectiveOpt) {
        RpcServiceReference statusRpc = StatusRpcClientFactory.getMockStub();
        // dataxApp 相关的模版
        Optional<K8SDataXPowerJobJobTemplate> jobTpl = Optional.of(K8SDataXPowerJobJobTemplate.getAppRelevantDataXJobWorkerTemplate(dataxProcessor));

        innerSaveJob(selectedTabTriggers, unEffectiveOpt, jobTpl.get()
//                        .orElseGet(() -> {
//            // 为空,调用全局模版
//            return (K8SDataXPowerJobJobTemplate) DataXJobWorker.getJobWorker(
//                    TargetResName.K8S_DATAX_INSTANCE_NAME, Optional.of(DataXJobWorker.K8SWorkerCptType.JobTpl));
//        })
                , statusRpc);
    }

    //    private PowerWorkflowPayload innerSaveJob(
//            Optional<Map<ISelectedTab, SelectedTabTriggers>> selectedTabTriggers, Optional<WorkflowUnEffectiveJudge> unEffectiveOpt
//            , K8SDataXPowerJobJobTemplate jobTpl, DataXJobSubmit submit, RpcServiceReference statusRpc) {
//        return innerSaveJob(selectedTabTriggers, unEffectiveOpt, jobTpl, submit, statusRpc);
//    }

    /**
     * 执行Powerjob工作流保存流程
     *
     * @param selectedTabTriggers
     * @param unEffectiveOpt
     * @param jobTpl
     * @param statusRpc
     * @return
     */
    private void innerSaveJob(
            Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>> selectedTabTriggers //
            , Optional<WorkflowUnEffectiveJudge> unEffectiveOpt
            , K8SDataXPowerJobJobTemplate jobTpl, RpcServiceReference statusRpc) {
        PowerJobClient powerJobClient = getTISPowerJob();

        Objects.requireNonNull(jobTpl, "jobTpl can not be null");
        Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> topologNode = selectedTabTriggers.orElseGet(() -> createWfNodes());
        Map<ISelectedTab, SelectedTabTriggers> createWfNodesResult = topologNode.getKey();
        WorkflowUnEffectiveJudge unEffectiveJudge = unEffectiveOpt.orElseGet(() -> new WorkflowUnEffectiveJudge());

        JSONObject mrParams = null;

        SelectedTabTriggers tabTriggers = null;
        ISelectedTab selectedTab = null;

        boolean containPostTrigger = false;

        // Long jobId = null;
        List<SaveWorkflowNodeRequest> wfNodes = Lists.newArrayList();
        Optional<PEWorkflowDAG.Node> changedWfNode = null;
        for (Map.Entry<ISelectedTab, SelectedTabTriggers> entry : createWfNodesResult.entrySet()) {
            tabTriggers = entry.getValue();
            selectedTab = entry.getKey();
            mrParams = tabTriggers.createMRParams();

            if (tabTriggers.getPostTrigger() != null) {
                containPostTrigger = true;
            }

            SaveJobInfoRequest jobRequest = jobTpl.createSynJobRequest();
            changedWfNode = unEffectiveJudge.getExistWfNode(selectedTab.getName());

            SaveWorkflowNodeRequest wfNode = createWorkflowNode(this.getTISPowerJob(), dataxProcessor.identityValue() + "_" + selectedTab.getName()
                    , JsonUtil.toString(mrParams), changedWfNode, jobRequest, jobTpl);
            wfNodes.add(wfNode);
            jobIdMaintainer.addJob(selectedTab, wfNode.getJobId());
        }


        //===============================================================
        // process startNode
        final String startNodeName = dataxProcessor.identityValue() + KEY_START_INITIALIZE_SUFFIX;

        final SaveJobInfoRequest initJobRequest = jobTpl.createInitializeJobRequest();
        unEffectiveJudge.getStartInitNode().ifPresent((existStarNode) -> {
            initJobRequest.setId(existStarNode.getJobId());
        });
        initJobRequest.setJobName(startNodeName);

        JSONObject initNode = createInitNodeJson();

        initJobRequest.setJobParams(JsonUtil.toString(initNode));

        SaveWorkflowNodeRequest startWfNode = jobTpl.createWorkflowNode();
        startWfNode.setJobId(result(powerJobClient.saveJob(initJobRequest)));
        startWfNode.setNodeName(startNodeName);
        startWfNode.setNodeParams(initJobRequest.getJobParams());

        wfNodes.add(startWfNode);
        //===============================================================
        wfNodes.addAll(jobIdMaintainer.beforeCreateWorkflowDAG(jobTpl));
        List<WorkflowNodeInfoDTO> savedWfNodes
                = result(powerJobClient.saveWorkflowNode(wfNodes));


        for (PEWorkflowDAG.Node deleteNode : unEffectiveJudge.getDeletedWfNodes()) {
            result(powerJobClient.deleteJob(deleteNode.getJobId()));
        }

        jobIdMaintainer.setStartInitJob(savedWfNodes.stream().filter((n) -> startWfNode.getJobId() == (long) n.getJobId()).findFirst());
        jobIdMaintainer.addWorkflow(savedWfNodes);

        PowerWorkflowPayload.PowerJobWorkflow powerWf = this.getPowerJobWorkflowId(false);

        SaveWorkflowRequest req = jobTpl.createWorkflowRequest(dataxProcessor);

        req.setId(powerWf != null ? powerWf.getPowerjobWorkflowId() : null);

        PEWorkflowDAG peWorkflowDAG = jobIdMaintainer.createWorkflowDAG();
        req.setDag(peWorkflowDAG);
        Long saveWorkflowId = result(powerJobClient.saveWorkflow(req));

        if (powerWf == null) {
            this.setPowerJobWorkflowId(saveWorkflowId
                    , new ExecutePhaseRange(FullbuildPhase.FullDump, containPostTrigger ? FullbuildPhase.JOIN : FullbuildPhase.FullDump));
        }

    }

    protected JSONObject createInitNodeJson() {
        JSONObject initNode = new JSONObject();
        initNode.put(DataxUtils.DATAX_NAME, dataxProcessor.identityValue());
        // 是否是dataflow的处理类型
        initNode.put(DataxUtils.TIS_WORK_FLOW_CHANNEL, dataxProcessor.getResType() == StoreResourceType.DataFlow);
        return initNode;
    }

    public PowerJobWorkflow getPowerJobWorkflowId(boolean validateWorkflowId) {
        JSONObject payload = getAppPayload();
        Long workflowId = payload.getLong(KEY_WORKFLOW_ID);
        if (validateWorkflowId) {
            Objects.requireNonNull(workflowId
                    , "param " + KEY_WORKFLOW_ID + " can not be null");
        }

        if (workflowId == null) {
            return null;
        }

        JSONArray execRange = Objects.requireNonNull(payload.getJSONArray(EXEC_RANGE)
                , "key:" + EXEC_RANGE + " relevant props can not be null");

        if (execRange.size() != 2) {
            throw new IllegalStateException("execRange.size() must be 2 ,but now is " + JsonUtil.toString(execRange));
        }

        return new PowerJobWorkflow(this.getTargetEntityName(), workflowId
                , new ExecutePhaseRange( //
                FullbuildPhase.parse(execRange.getString(0)) //
                , FullbuildPhase.parse(execRange.getString(1))));
    }

    /**
     * application.getProjectName()
     *
     * @return
     */
    protected abstract String getTargetEntityName();

    public class PowerJobWorkflow {

        private final Long powerjobWorkflowId;
        private final ExecutePhaseRange executePhaseRange;
        private final String appName;
        private WorkflowInfoDTO wfDTO;

        public PowerJobWorkflow(String appName, Long powerjobWorkflowId, ExecutePhaseRange executePhaseRange) {
            if (StringUtils.isEmpty(appName)) {
                throw new IllegalArgumentException("param appName can not be null");
            }
            this.powerjobWorkflowId = powerjobWorkflowId;
            this.executePhaseRange = executePhaseRange;
            this.appName = appName;
            try {
                this.wfDTO = TISPowerJobClient.result(getTISPowerJob().fetchWorkflow(powerjobWorkflowId));
            } catch (Throwable e) {
                logger.warn("powerjobWorkflowId:" + powerjobWorkflowId + ",error:" + e.getMessage());
                //throw new RuntimeException(e);
            }
        }

        public Long getPowerjobWorkflowId() {
            return powerjobWorkflowId;
        }

        public boolean isDisabled() {

            return (wfDTO == null || !wfDTO.getEnable());
        }

        /**
         * 是否已经失效
         *
         * @param powerClient
         * @param topologNodes
         * @return
         */
        public WorkflowUnEffectiveJudge isUnEffective(PowerJobClient powerClient, Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> topologNodes) {
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

                vistWorkflowNodes(this.appName, this.wfDTO, new DistributedPowerJobDataXJobSubmit.WorkflowVisit() {
                    @Override
                    public void vistStartInitNode(PEWorkflowDAG.Node node) {
                        unEffectiveJudge.setStatInitNode(node);
                    }

                    @Override
                    public void vistJoinWorkerNode(ISqlTask.SqlTaskCfg cfg, PEWorkflowDAG.Node node) {
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
                    public void vistDumpWorkerNode(PEWorkflowDAG.Node wfNode) {
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

        public ExecutePhaseRange getExecutePhaseRange() {
            return executePhaseRange;
        }
    }


    protected final JSONObject getAppPayload() {
        JSONObject payload = null;
        String payloadContent = null;
        try {
            payloadContent = getPayloadContent();
            payload = JSONObject.parseObject(payloadContent);
        } catch (Throwable e) {
            logger.warn("payloadContent:" + payloadContent, e);
        }
        return payload == null ? new JSONObject() : payload;
    }

    protected abstract String getPayloadContent();


    public void setPowerJobWorkflowId(Long workflowId, ExecutePhaseRange executePhaseRange) {

        JSONObject appPayload = getAppPayload();
        createPayload(workflowId, executePhaseRange, appPayload);
        setPowerJobWorkflowPayload(appPayload);

    }

    public static JSONObject createPayload( //
                                            Long powerjobWorkflowId, ExecutePhaseRange executePhaseRange, JSONObject appPayload) {
        appPayload.put(KEY_WORKFLOW_ID, powerjobWorkflowId);
        appPayload.put(EXEC_RANGE, new String[]{Objects.requireNonNull(
                executePhaseRange, "param executePhaseRange can not be null").getStart().getName()
                , executePhaseRange.getEnd().getName()});
        return appPayload;
    }


    protected abstract void setPowerJobWorkflowPayload(JSONObject appPayload);


    private static class TISWorkflowPayload extends PowerWorkflowPayload {

        private final String tisWorkflowName;
        private final IWorkFlowDAO workFlowDAO;

        private WorkFlow tisWorkflow;

        private TISWorkflowPayload(DistributedPowerJobDataXJobSubmit submit, IControlMsgHandler module, String tisWorkflowName
                , ICommonDAOContext commonDAOContext, IDataxProcessor dataxProcessor, JobMap2WorkflowMaintainer jobIdMaintainer) {
            super(submit, module, dataxProcessor, commonDAOContext, jobIdMaintainer);
            if (StringUtils.isEmpty(tisWorkflowName)) {
                throw new IllegalArgumentException("param  tisWorkflowName can not be empty");
            }
            this.tisWorkflowName = tisWorkflowName;
            this.workFlowDAO = Objects.requireNonNull(commonDAOContext.getWorkFlowDAO()
                    , "param workFlowDAO can not be null");
        }

        @Override
        protected PowerJobExecContext createPowerJobExecContext() {
            PowerJobExecContext chainContext = new PowerJobExecContext();
            // chainContext.setAppname(this.dataxProcessor.identityValue());
            chainContext.setWorkflowId(this.load().getId());
            return chainContext;
        }

        @Override
        protected String getTargetEntityName() {
            return this.tisWorkflowName;
        }

        private WorkFlow load() {
            if (this.tisWorkflow == null) {
                WorkFlowCriteria criteria = new WorkFlowCriteria();
                criteria.createCriteria().andNameEqualTo(this.tisWorkflowName);
                List<WorkFlow> workFlows = workFlowDAO.selectByExample(criteria);
                for (WorkFlow wf : workFlows) {
                    this.tisWorkflow = wf;
                    break;
                }
                Objects.requireNonNull(this.tisWorkflow
                        , "workflowName:" + this.tisWorkflowName + " relevant tisWorkflow can not be null");
            }
            return this.tisWorkflow;
        }

        @Override
        protected JSONObject createInitNodeJson() {
            JSONObject initNode = super.createInitNodeJson();
            initNode.put(KEY_WORKFLOW_ID, this.load().getId());
            return initNode;
        }

        @Override
        protected String getPayloadContent() {
            // this.tisWorkflow = null;
            return this.load().getGitPath();
        }

        @Override
        protected void setPowerJobWorkflowPayload(JSONObject appPayload) {

            WorkFlow wf = new WorkFlow();
            wf.setOpTime(new Date());
            wf.setGitPath(JsonUtil.toString(Objects.requireNonNull(appPayload, "appPayload can not be null")));
            WorkFlow beUpdate = this.load();
            WorkFlowCriteria criteria = new WorkFlowCriteria();
            criteria.createCriteria().andIdEqualTo(beUpdate.getId());

            this.load().setGitPath(wf.getGitPath());

            if (this.workFlowDAO.updateByExampleSelective(wf, criteria) < 1) {
                throw new IllegalStateException("app:" + beUpdate.getName() + " update workflowId in payload faild");
            }
        }
    }

    /**
     * @author 百岁 (baisui@qlangtech.com)
     * @date 2023/11/10
     */
    private static class ApplicationPayload extends PowerWorkflowPayload {

        private final String appName;
        private final IApplicationDAO applicationDAO;

        private Application app;


        private ApplicationPayload(DistributedPowerJobDataXJobSubmit submit, IControlMsgHandler module
                , String appName, ICommonDAOContext commonDAOContext, DataxProcessor dataxProcessor) {
            super(submit, module, dataxProcessor, commonDAOContext, new JobMap2WorkflowMaintainer());
            this.applicationDAO = Objects.requireNonNull(commonDAOContext.getApplicationDAO(), "applicationDAO can not be null");
            this.appName = appName;
        }

        @Override
        protected PowerJobExecContext createPowerJobExecContext() {
            PowerJobExecContext execContext = new PowerJobExecContext();
            execContext.setAppname(this.appName);
            return execContext;
        }

        private Application load() {
            if (this.app == null) {
                this.app = Objects.requireNonNull(//
                        applicationDAO.selectByName(appName), "appName:" + appName + " relevant app can not be null");
            }
            return this.app;
        }

        @Override
        protected String getTargetEntityName() {
            return this.load().getProjectName();
        }

        @Override
        protected String getPayloadContent() {
            //  this.app = null;
            return this.load().getFullBuildCronTime();
        }

        @Override
        protected void setPowerJobWorkflowPayload(JSONObject appPayload) {
            Application app = new Application();
            app.setFullBuildCronTime(JsonUtil.toString(Objects.requireNonNull(appPayload, "appPayload can not be null")));

            Application application = this.load();
            application.setFullBuildCronTime(app.getFullBuildCronTime());
            ApplicationCriteria appCriteria = new ApplicationCriteria();
            appCriteria.createCriteria().andAppIdEqualTo(application.getAppId());
            if (applicationDAO.updateByExampleSelective(app, appCriteria) < 1) {
                throw new IllegalStateException("app:" + application.getProjectName() + " update workflowId in payload faild");
            }
            this.app = null;
        }
    }
}
