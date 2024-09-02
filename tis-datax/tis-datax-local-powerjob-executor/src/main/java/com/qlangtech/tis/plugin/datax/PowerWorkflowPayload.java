package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.datax.PowerWorkflowPayload.PowerJobWorkflow;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobJobTemplate;
import com.qlangtech.tis.plugin.datax.powerjob.PowerjobWorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.IDAGSessionSpec;
import com.qlangtech.tis.powerjob.IDataFlowTopology;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.workflow.dao.IWorkFlowDAO;
import com.qlangtech.tis.workflow.pojo.WorkFlow;
import com.qlangtech.tis.workflow.pojo.WorkFlowCriteria;
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
import tech.powerjob.common.request.http.SaveWorkflowRequest;
import tech.powerjob.common.response.WorkflowInfoDTO;
import tech.powerjob.common.response.WorkflowNodeInfoDTO;

import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.qlangtech.tis.fullbuild.IFullBuildContext.KEY_WORKFLOW_ID;
import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.KEY_START_INITIALIZE_SUFFIX;
import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.createWorkflowNode;
import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.getCommonDAOContext;
import static com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient.result;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/2
 * @see com.qlangtech.tis.plugin.datax.PowerWorkflowPayload.ApplicationPayload
 * @see com.qlangtech.tis.plugin.datax.PowerWorkflowPayload.TISWorkflowPayload
 */
public abstract class PowerWorkflowPayload extends BasicWorkflowPayload<PowerJobWorkflow> {


    private static final Logger logger = LoggerFactory.getLogger(PowerWorkflowPayload.class);

    protected final JobMap2WorkflowMaintainer jobIdMaintainer;
    protected final IControlMsgHandler module;
    //   protected final ICommonDAOContext commonDAOContext;

    public PowerWorkflowPayload(BasicDistributedSPIDataXJobSubmit submit, IControlMsgHandler module, IDataxProcessor dataxProcessor
            , ICommonDAOContext commonDAOContext, JobMap2WorkflowMaintainer jobIdMaintainer) {
        super(dataxProcessor, commonDAOContext, submit);
        this.module = module;
        this.jobIdMaintainer = jobIdMaintainer;
        //  this.commonDAOContext = commonDAOContext;
    }

    @Override
    protected Long runSPIWorkflow(BasicWorkflowInstance spiWorkflowId, JSONObject instanceParams) {
        return result(this.getTISPowerJob()
                .runWorkflow(spiWorkflowId.getSPIWorkflowId() /** wfInfo.getId()*/, JsonUtil.toString(instanceParams), 0));
    }

    public static PowerWorkflowPayload createApplicationPayload(BasicDistributedSPIDataXJobSubmit submit, IControlMsgHandler module, String appName) {
        ICommonDAOContext commonDAOContext = getCommonDAOContext(module);
//        if (!(module instanceof IPluginContext)) {
//            throw new IllegalStateException("type of module:" + module.getClass() + " must be type of " + IPluginContext.class);
//        }
        DataxProcessor dataxProcessor = (DataxProcessor) DataxProcessor.load(null, appName);
        return new ApplicationPayload(submit, module, appName, commonDAOContext, dataxProcessor);
    }

    public static PowerWorkflowPayload createTISWorkflowPayload(BasicDistributedSPIDataXJobSubmit submit, IControlMsgHandler module, IDataFlowTopology topology) {

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
                Optional<IWorkflowNode> existWfNode = Optional.empty();
                for (ISqlTask sqlTask : parseNodes) {

                    try (StringWriter writer = new StringWriter()) {
                        SqlTaskNodeMeta.persistSqlTask(writer, sqlTask);
                        SaveJobInfoRequest sqlJoinRequest = jobTpl.createSqlProcessJobRequest();
                        JSONObject wfNodeParams = ISqlTask.json(sqlTask);

                        SaveWorkflowNodeRequest wfNodeReq = createWorkflowNode(//
                                DistributedPowerJobDataXJobSubmit.getTISPowerJob(), topology.getName() + "_" + sqlTask.getExportName()
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

//    @Override
//    protected PowerJobWorkflow loadWorkflowSPI() {
//        return this.loadWorkflowSPI(false);
//    }

//    /**
//     * @param validateWorkflowId 是否校验证
//     * @return
//     */
//    public PowerJobWorkflow loadWorkflowSPI(boolean validateWorkflowId) {
//        JSONObject payload = getAppPayload();
//        Long spiWorkflowId = payload.getLong(KEY_WORKFLOW_ID);
//        if (validateWorkflowId) {
//            Objects.requireNonNull(spiWorkflowId
//                    , "param " + KEY_WORKFLOW_ID + " can not be null");
//        }
//
//        if (spiWorkflowId == null) {
//            return null;
//        }
//
//        JSONArray execRange = Objects.requireNonNull(payload.getJSONArray(EXEC_RANGE)
//                , "key:" + EXEC_RANGE + " relevant props can not be null");
//
//        if (execRange.size() != 2) {
//            throw new IllegalStateException("execRange.size() must be 2 ,but now is " + JsonUtil.toString(execRange));
//        }
//
//        return new PowerJobWorkflow(this.getTargetEntityName(), spiWorkflowId
//                , new ExecutePhaseRange( //
//                FullbuildPhase.parse(execRange.getString(0)) //
//                , FullbuildPhase.parse(execRange.getString(1))));
//    }

    /**
     * 保存powerjob workflow
     *
     * @return
     */
    public PowerWorkflowPayload.PowerJobWorkflow saveJob() {
        PowerWorkflowPayload.PowerJobWorkflow powerJobWorkflowId = this.loadWorkflowSPI(false);


        RpcServiceReference rpcStub = StatusRpcClientFactory.getMockStub();
        K8SDataXPowerJobJobTemplate worker = Objects.requireNonNull(K8SDataXPowerJobJobTemplate.getAppRelevantDataXJobWorkerTemplate(dataxProcessor), "worker can not be empty");
        Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>> selectedTabTriggers = createWfNodes();


        WorkflowUnEffectiveJudge unEffectiveJudge = null;

        if (powerJobWorkflowId != null) {
            unEffectiveJudge = powerJobWorkflowId.isUnEffective(selectedTabTriggers);
        }
        /**
         * =========================
         *innerSaveJob
         * =========================
         */
        this.innerSaveJob(Optional.of(selectedTabTriggers)
                , Optional.ofNullable(unEffectiveJudge), worker, rpcStub);
        if (powerJobWorkflowId == null) {
            powerJobWorkflowId = this.loadWorkflowSPI(true);
        }
        return powerJobWorkflowId;
    }

    public final TISPowerJobClient getTISPowerJob() {
        return DistributedPowerJobDataXJobSubmit.getTISPowerJob();// this.submit.getTISPowerJob();
    }

//    @Override
//    protected List<IWorkflowNode> getDAGNodes(BasicWorkflowInstance powerJobWorkflowId) {
//        return powerJobWorkflowId.getWorkflowNodes();
//        WorkflowInfoDTO wfInfo = result(this.getTISPowerJob().fetchWorkflow(powerJobWorkflowId.getSPIWorkflowId()));
//        List<IWorkflowNode> wfNodes = DistributedPowerJobDataXJobSubmit.convertWorkflowNodes(wfInfo);
//        return wfNodes;
//    }


//    /**
//     * @param daoContext
//     * @param workflowInstanceIdOpt TIS 中触发历史taskId
//     * @param feedback
//     * @return
//     */
//    public PowerjobTriggerBuildResult triggerWorkflow(ICommonDAOContext daoContext, Optional<Long> workflowInstanceIdOpt
//            , StatusRpcClientFactory.AssembleSvcCompsite feedback) {
//        //  Objects.requireNonNull(statusRpc, "statusRpc can not be null");
//
//        WorkflowSPIInitializer<PowerJobWorkflow> workflowInitializer = new WorkflowSPIInitializer(this);
//
//        PowerJobWorkflow powerJobWorkflowId = workflowInitializer.initialize();
//
//        List<IWorkflowNode> wfNodes = getDAGNodes(powerJobWorkflowId);
//        final List<SelectedTabTriggers.SelectedTabTriggersConfig> triggerCfgs = Lists.newArrayList();
//        final List<ISqlTask.SqlTaskCfg> joinNodeCfgs = Lists.newArrayList();
//        vistWorkflowNodes(this.dataxProcessor.identityValue(), wfNodes, new WorkflowNodeVisit() {
//            @Override
//            public void vistStartInitNode(IWorkflowNode node) {
//                return;
//            }
//
//            @Override
//            public void vistJoinWorkerNode(ISqlTask.SqlTaskCfg cfg, IWorkflowNode node) {
//                joinNodeCfgs.add(cfg);
//            }
//
//            @Override
//            public void vistDumpWorkerNode(IWorkflowNode node) {
//                triggerCfgs.add(SelectedTabTriggers.deserialize(JSONObject.parseObject(node.getNodeParams())));
//            }
//        });
//
//
//        SPIExecContext chainContext = createSPIExecContext();
//        chainContext.setExecutePhaseRange(powerJobWorkflowId.getExecutePhaseRange());
//        //
//        /**===================================================================
//         * 创建 TIS的taskId
//         ===================================================================*/
//        CreateNewTaskResult newTaskResult
//                = daoContext.createNewDataXTask(chainContext, workflowInstanceIdOpt.isPresent() ? TriggerType.CRONTAB : TriggerType.MANUAL);
//
//        final Integer tisTaskId = newTaskResult.getTaskid();
//
//        if (CollectionUtils.isEmpty(triggerCfgs)) {
//            throw new IllegalStateException("powerjob workflowId:" + powerJobWorkflowId.getSPIWorkflowId()
//                    + " relevant nodes triggerCfgs can not be null empty");
//        }
//
//        PhaseStatusCollection statusCollection = createPhaseStatus(powerJobWorkflowId, triggerCfgs, joinNodeCfgs, tisTaskId);
//        feedback.initSynJob(statusCollection);
//
//        JSONObject instanceParams = createInstanceParams(tisTaskId);
//        // 取得powerjob instanceId
//        Long workflowInstanceIdOfSPI = workflowInstanceIdOpt.orElseGet(() -> {
//            /****************************************
//             * 手动触发的情况
//             ****************************************/
//            Long createWorkflowInstanceId = result(this.getTISPowerJob()
//                    .runWorkflow(powerJobWorkflowId.getSPIWorkflowId() /** wfInfo.getId()*/, JsonUtil.toString(instanceParams), 0));
//            logger.info("create workflow instanceId:{}", createWorkflowInstanceId);
//            return createWorkflowInstanceId;
//        });
//
//
//        WorkFlowBuildHistoryPayload buildHistoryPayload = new WorkFlowBuildHistoryPayload(tisTaskId, this.commonDAOContext);
//
//        buildHistoryPayload.setSPIWorkflowInstanceId(workflowInstanceIdOfSPI);
//
//        PowerjobTriggerBuildResult buildResult = new PowerjobTriggerBuildResult(true, instanceParams);
//        buildResult.taskid = tisTaskId;
//
//        initializeService(commonDAOContext);
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
//                    TISPowerJobClient powerJobClient = this.getTISPowerJob();
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


    @Override
    protected WorkFlowBuildHistoryPayload createBuildHistoryPayload(Integer tisTaskId) {
        return new PowerjobWorkFlowBuildHistoryPayload(this.dataxProcessor, tisTaskId, this.commonDAOContext, this.getTISPowerJob());
    }


    @Override
    public void innerCreatePowerjobWorkflow(
            boolean updateProcess,
            Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>> selectedTabTriggers //
            , Optional<WorkflowUnEffectiveJudge> unEffectiveOpt) {
        RpcServiceReference statusRpc = StatusRpcClientFactory.getMockStub();
        // dataxApp 相关的模版
        Optional<K8SDataXPowerJobJobTemplate> jobTpl = Optional.of(K8SDataXPowerJobJobTemplate.getAppRelevantDataXJobWorkerTemplate(dataxProcessor));

        innerSaveJob(selectedTabTriggers, unEffectiveOpt, jobTpl.get(), statusRpc);
    }


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
        Optional<IWorkflowNode> changedWfNode = null;
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


        for (IWorkflowNode deleteNode : unEffectiveJudge.getDeletedWfNodes()) {
            result(powerJobClient.deleteJob(deleteNode.getJobId()));
        }

        jobIdMaintainer.setStartInitJob(savedWfNodes.stream().filter((n) -> startWfNode.getJobId() == (long) n.getJobId()).findFirst());
        jobIdMaintainer.addWorkflow(savedWfNodes);

        PowerWorkflowPayload.PowerJobWorkflow powerWf = this.loadWorkflowSPI(false);

        SaveWorkflowRequest req = jobTpl.createWorkflowRequest(dataxProcessor);

        req.setId(powerWf != null ? powerWf.getSPIWorkflowId() : null);

        PEWorkflowDAG peWorkflowDAG = jobIdMaintainer.createWorkflowDAG();
        req.setDag(peWorkflowDAG);
        Long saveWorkflowId = result(powerJobClient.saveWorkflow(req));

        if (powerWf == null) {
            this.setSPIWorkflowId(saveWorkflowId
                    , new ExecutePhaseRange(FullbuildPhase.FullDump, containPostTrigger ? FullbuildPhase.JOIN : FullbuildPhase.FullDump));
        }

    }

    protected abstract String getTargetEntityName();

    @Override
    protected PowerJobWorkflow createWorkflowInstance(Long spiWorkflowId, ExecutePhaseRange execRange) {
        return new PowerJobWorkflow(this.getTargetEntityName(), spiWorkflowId
                , execRange);
    }


    public class PowerJobWorkflow extends BasicWorkflowInstance {


        private WorkflowInfoDTO wfDTO;

        public PowerJobWorkflow(String appName, Long powerjobWorkflowId, ExecutePhaseRange executePhaseRange) {
            super(executePhaseRange, appName, powerjobWorkflowId);
            if (StringUtils.isEmpty(appName)) {
                throw new IllegalArgumentException("param appName can not be null");
            }
            //  this.spiWorkflowId = powerjobWorkflowId;
            try {
                this.wfDTO = TISPowerJobClient.result(getTISPowerJob().fetchWorkflow(powerjobWorkflowId));
            } catch (Throwable e) {
                logger.warn("powerjobWorkflowId:" + powerjobWorkflowId + ",error:" + e.getMessage());
                //throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isDisabled() {
            return (wfDTO == null || !wfDTO.getEnable());
        }

        @Override
        public List<IWorkflowNode> getWorkflowNodes() {
            return DistributedPowerJobDataXJobSubmit.convertWorkflowNodes(this.wfDTO);
        }


    }


    private static class TISWorkflowPayload extends PowerWorkflowPayload {

        private final String tisWorkflowName;
        private final IWorkFlowDAO workFlowDAO;

        private WorkFlow tisWorkflow;

        private TISWorkflowPayload(BasicDistributedSPIDataXJobSubmit submit, IControlMsgHandler module, String tisWorkflowName
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
        protected SPIExecContext createSPIExecContext() {
            SPIExecContext chainContext = new SPIExecContext();
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
        protected void setSPIWorkflowPayload(JSONObject appPayload) {

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
        private final SPIWorkflowPayloadApplicationStore applicationStore;

        private Application app;


        private ApplicationPayload(BasicDistributedSPIDataXJobSubmit submit, IControlMsgHandler module
                , String appName, ICommonDAOContext commonDAOContext, DataxProcessor dataxProcessor) {
            super(submit, module, dataxProcessor, commonDAOContext, new JobMap2WorkflowMaintainer());

            this.applicationStore = new SPIWorkflowPayloadApplicationStore(appName, commonDAOContext);
            // this.applicationDAO = Objects.requireNonNull(commonDAOContext.getApplicationDAO(), "applicationDAO can not be null");
            this.appName = appName;
        }

        @Override
        protected SPIExecContext createSPIExecContext() {
            SPIExecContext execContext = new SPIExecContext();
            execContext.setAppname(this.appName);
            return execContext;
        }

//        private Application load() {
//            if (this.app == null) {
//                this.app = Objects.requireNonNull(//
//                        applicationDAO.selectByName(appName), "appName:" + appName + " relevant app can not be null");
//            }
//            return this.app;
//        }

        @Override
        protected String getTargetEntityName() {
            return Objects.requireNonNull(this.appName, "appName can not be null");
            //  return this.load().getProjectName();
        }

        @Override
        protected String getPayloadContent() {
            //  this.app = null;
            return this.applicationStore.getPayloadContent(); //this.load().getFullBuildCronTime();
        }

        @Override
        protected void setSPIWorkflowPayload(JSONObject appPayload) {
            this.applicationStore.setSPIWorkflowPayload(appPayload);
//            Application app = new Application();
//            app.setFullBuildCronTime(JsonUtil.toString(Objects.requireNonNull(appPayload, "appPayload can not be null")));
//
//            Application application = this.load();
//            application.setFullBuildCronTime(app.getFullBuildCronTime());
//            ApplicationCriteria appCriteria = new ApplicationCriteria();
//            appCriteria.createCriteria().andAppIdEqualTo(application.getAppId());
//            if (applicationDAO.updateByExampleSelective(app, appCriteria) < 1) {
//                throw new IllegalStateException("app:" + application.getProjectName() + " update workflowId in payload faild");
//            }
//            this.app = null;
        }
    }

}
