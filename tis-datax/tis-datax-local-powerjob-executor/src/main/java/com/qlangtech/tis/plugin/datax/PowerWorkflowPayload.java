package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.datax.StoreResourceType;
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

    public static PowerWorkflowPayload createApplicationPayload(BasicDistributedSPIDataXJobSubmit submit, IControlMsgHandler module, DataXName appName) {
        ICommonDAOContext commonDAOContext = getCommonDAOContext(module);
//        if (!(module instanceof IPluginContext)) {
//            throw new IllegalStateException("type of module:" + module.getClass() + " must be type of " + IPluginContext.class);
//        }
        DataxProcessor dataxProcessor = (DataxProcessor) DataxProcessor.load(null, appName);
        return new ApplicationPayload(submit, module, appName.getPipelineName(), commonDAOContext, dataxProcessor);
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
        }
    }

}
