package com.qlangtech.tis.plugin.akka;

import akka.actor.ActorRef;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.assemble.TriggerType;
import com.qlangtech.tis.coredefine.module.action.DistributeJobTriggerBuildResult;
import com.qlangtech.tis.dag.TISActorSystem;
import com.qlangtech.tis.dag.actor.message.StartWorkflow;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.exec.impl.DataXPipelineExecContext;
import com.qlangtech.tis.fullbuild.phasestatus.PhaseStatusCollection;
import com.qlangtech.tis.manage.common.CreateNewTaskResult;
import com.qlangtech.tis.plugin.datax.BasicWorkflowInstance;
import com.qlangtech.tis.plugin.datax.BasicWorkflowPayload;
import com.qlangtech.tis.plugin.datax.PowerJobTskTriggers;
import com.qlangtech.tis.plugin.datax.SPIExecContext;
import com.qlangtech.tis.plugin.datax.WorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;
import com.qlangtech.tis.sql.parser.DAGSessionSpec;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.qlangtech.tis.workflow.pojo.WorkflowDAGFileManager;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/31
 */
public class AkkaPipelinePayload extends BasicWorkflowPayload<AkkaWorkflow> {

    private final static Logger logger = LoggerFactory.getLogger(AkkaPipelinePayload.class);
    private final DataXName pipelineName;
    private final RpcServiceReference rpcServiceRef;
    // private final Optional<PhaseStatusCollection> latestWorkflowHistory;
    private final DAORestDelegateFacade commonDAOContext;

    public AkkaPipelinePayload(RpcServiceReference rpcServiceRef, IDataxProcessor dataxProcessor
            , DAORestDelegateFacade commonDAOContext, DistributedAKKAJobDataXJobSubmit submit //
                               //        , Optional<PhaseStatusCollection> latestWorkflowHistory
    ) {
        super(dataxProcessor, submit);
        this.pipelineName = dataxProcessor.getDataXName();// new DataXName(dataxProcessor.identityValue(), dataxProcessor.getResType());
        this.pipelineName.assetCheckType(StoreResourceType.DataApp);
        this.rpcServiceRef = rpcServiceRef;
        //   this.latestWorkflowHistory = latestWorkflowHistory;
        this.commonDAOContext = commonDAOContext;
    }

    @Override
    public DistributeJobTriggerBuildResult triggerWorkflow(IExecChainContext execChainContext,
                                                           RpcServiceReference feedback) {
        PowerJobTskTriggers taskTriggers = new PowerJobTskTriggers();
        execChainContext.setTskTriggers(taskTriggers);
        DataXCfgGenerator.GenerateCfgs cfgFileNames
                = this.dataxProcessor.getDataxCfgFileNames(null, com.qlangtech.tis.plugin.trigger.JobTrigger.getTriggerFromHttpParam(execChainContext));
        Pair<DAGSessionSpec, List<Pair<ISelectedTab, SelectedTabTriggers>>> spec = DAGSessionSpec.createDAGSessionSpec(
                execChainContext, rpcServiceRef, this.dataxProcessor, cfgFileNames, submit);
        DAGSessionSpec sessionSpec = spec.getLeft();
        DistributeJobTriggerBuildResult buildResult = submitToAkkaCluster(execChainContext, sessionSpec);
        List<Pair<ISelectedTab, SelectedTabTriggers>> triggerCfgs = spec.getRight();
        PhaseStatusCollection statusCollection = BasicWorkflowPayload.createPhaseStatus(triggerCfgs, /**joinNodeCfgs*/Collections.emptyList(), buildResult.getTaskid());
        feedback.initSynJob(statusCollection);
        return buildResult;
    }


    /**
     * 提交 DAG 任务到 AKKA 集群执行
     *
     * @param sessionSpec DAG 会话规范
     * @return 执行结果
     * @throws Exception 执行异常
     */
    private DistributeJobTriggerBuildResult submitToAkkaCluster(IExecChainContext execChainContext, DAGSessionSpec sessionSpec) {
        TISActorSystem tisActorSystem = TISActorSystem.get();

        if (!tisActorSystem.isInitialized()) {
            throw new IllegalStateException("TISActorSystem not initialized yet, please check " +
                    "ConsoleInitilizeListener");
        }

        // 2. 创建工作流实例记录
        WorkFlowBuildHistory taskHistory = new WorkFlowBuildHistory();
        taskHistory.setAppName(pipelineName.getPipelineName());
        taskHistory.setStartTime(new Date());
        // taskHistory.setInstanceStatus(InstanceStatus.WAITING.getDesc());
        taskHistory.setState(ExecResult.DOING.getByteVal());
        taskHistory.setCreateTime(new Date());
        IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO = commonDAOContext.getWorkFlowBuildHistoryDAO();
        // 注意：这里需要确保 workflowBuildHistoryDAO 已注入
        if (workflowBuildHistoryDAO == null) {
            throw new IllegalStateException("workflowBuildHistoryDAO is not injected");
        }
        try {
            // 3. 将 DAGSessionSpec 转换为 PEWorkflowDAG
            PEWorkflowDAG dag = Objects.requireNonNull(sessionSpec.getDAG() //
                    , "pipeline:" + pipelineName.getPipelineName() + " relevant dag can not be null");

            // 4. 保存 DAG 定义到文件系统
            WorkflowDAGFileManager fileManager //
                    = new WorkflowDAGFileManager(this.dataxProcessor.getDataXWorkDir(null), true);
            File dagSpecPath = fileManager.saveDagSpec(pipelineName.getPipelineName(), dag);

            // 5. 更新实例记录，将 dagSpecPath 存储在 dagRuntime 字段中
            taskHistory.setDagRuntime(dagSpecPath.getAbsolutePath());

            DataXPipelineExecContext chainContext = new DataXPipelineExecContext(pipelineName.getPipelineName(), System.currentTimeMillis());
            chainContext.setExecutePhaseRange(new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN));

            CreateNewTaskResult newTask = IExecChainContext.createNewTask(chainContext, TriggerType.MANUAL, dagSpecPath);
            /**============================================================
             * 创建新的task实例
             ============================================================*/
            taskHistory.setId(newTask.getTaskid());

            if (newTask.getPreTaskId() != null) {
                PhaseStatusCollection statusCollection
                        = PhaseStatusCollection.getTaskPhaseReference(newTask.getPreTaskId());
              //  statusCollection.getBuildPhase();
            }


            // 6. 获取 DAGSchedulerActor 引用
            ActorRef workflowInstanceRegion = tisActorSystem.getWorkflowInstanceRegion();
            if (workflowInstanceRegion == null) {
                throw new IllegalStateException("DAGSchedulerActor not found in TISActorSystem");
            }

            // 7. 构建初始化参数
            Map<String, String> initParams = new HashMap<>();
            initParams.put("indexName", pipelineName.getPipelineName());
            initParams.put("taskId", String.valueOf(taskHistory.getId()));
            // 可以添加更多参数

            // 8. 发送 StartWorkflow 消息到 DAGSchedulerActor
            StartWorkflow startMsg = new StartWorkflow(taskHistory.getId(), pipelineName);
            startMsg.setDryRun(false);
            startMsg.setTriggerTimestamp(execChainContext.getPartitionTimestampWithMillis());
            startMsg.setPluginCfgsMetas(IExecChainContext.manifestOfDataX(dataxProcessor));
            // startMsg.setMaxConcurrentTasks();
            workflowInstanceRegion.tell(startMsg, ActorRef.noSender());

            logger.info("Workflow submitted to AKKA cluster: instanceId={}, indexName={}", taskHistory.getId(), pipelineName.getPipelineName());

            JSONObject instanceParams = new JSONObject();
            // 9. 等待工作流执行完成（轮询方式）
            DistributeJobTriggerBuildResult
                    buildResult = new DistributeJobTriggerBuildResult(true, instanceParams);
            buildResult.setPreviousTaskId(newTask.getPreTaskId());
            buildResult.taskid = taskHistory.getId();
            return buildResult;
        } catch (Exception e) {
            logger.error("Failed to submit workflow to AKKA cluster: instanceId={}", taskHistory.getId(), e);

            // 标记实例为失败
            try {
                if (taskHistory.getId() != null) {
                    //  taskHistory.setId(taskHistory.getId());
                    // taskHistory.setInstanceStatus(InstanceStatus.FAILED.getDesc());
                    taskHistory.setState((byte) ExecResult.FAILD.getValue());
                    taskHistory.setEndTime(new Date());
                    workflowBuildHistoryDAO.updateByPrimaryKeySelective(taskHistory);
                }
            } catch (Exception updateEx) {
                logger.error("Failed to update instance status to FAILED", updateEx);
            }

            throw e;
        }
    }

//    /**
//     * 将 DAGSessionSpec 转换为 PEWorkflowDAG
//     *
//     * @param sessionSpec DAG 会话规范
//     * @return PEWorkflowDAG 对象
//     */
//    private PEWorkflowDAG convertToPEWorkflowDAG(DAGSessionSpec sessionSpec) {
//        PEWorkflowDAG dag = new PEWorkflowDAG();
//        dag.setNodes(new ArrayList<>());
//        dag.setEdges(new ArrayList<>());
//
//        Map<String, TaskAndMilestone> taskMap = sessionSpec.getTaskMap();
//        if (MapUtils.isEmpty(taskMap)) {
//            //  logger.warn("TaskMap is empty, creating empty DAG");
//            //            return dag;
//            throw new IllegalStateException("TaskMap is empty, creating empty DAG");
//        }
//
//        // 节点 ID 计数器
//        AtomicLong nodeIdCounter = new AtomicLong(1);
//        Map<String, Long> taskNameToNodeId = new HashMap<>();
//
//        // 1. 创建所有节点
//        for (Map.Entry<String, TaskAndMilestone> entry : taskMap.entrySet()) {
//            String taskName = entry.getKey();
//
//            // 跳过 milestone 节点
//            if (taskName.startsWith(TaskAndMilestone.MILESTONE_PREFIX)) {
//                continue;
//            }
//
//            Long nodeId = nodeIdCounter.getAndIncrement();
//            taskNameToNodeId.put(taskName, nodeId);
//
//            PEWorkflowDAG.Node node = new PEWorkflowDAG.Node();
//            node.setNodeId(nodeId);
//            node.setNodeName(taskName);
//            node.setNodeType(WorkflowNodeType.TASK.getV());
//            node.setJobId(nodeId); // 使用 nodeId 作为 jobId
//            node.setEnable(true);
//            node.setSkipWhenFailed(false); // 默认不跳过失败
//            node.setStatus(InstanceStatus.WAITING.getV());
//
//            dag.getNodes().add(node);
//
//            logger.debug("Created DAG node: nodeId={}, nodeName={}", nodeId, taskName);
//        }
//
//        // 2. 根据 DAGSessionSpec 的 dptNodes 创建边
//        // 使用 buildSpec 的依赖关系来构建边
//        Set<Pair<String, String>> dependencies = new HashSet<>();
//        sessionSpec.buildSpec((dpt) -> {
//            dependencies.add(dpt);
//        });
//
//        for (Pair<String, String> dep : dependencies) {
//            String fromTaskName = dep.getLeft();
//            String toTaskName = dep.getRight();
//
//            // 跳过 milestone 节点
//            if (fromTaskName.startsWith(TaskAndMilestone.MILESTONE_PREFIX) || toTaskName.startsWith(TaskAndMilestone.MILESTONE_PREFIX)) {
//                continue;
//            }
//
//            Long fromNodeId = taskNameToNodeId.get(fromTaskName);
//            Long toNodeId = taskNameToNodeId.get(toTaskName);
//
//            if (fromNodeId != null && toNodeId != null) {
//                PEWorkflowDAG.Edge edge = new PEWorkflowDAG.Edge();
//                edge.setFrom(fromNodeId);
//                edge.setTo(toNodeId);
//                edge.setEnable(true);
//                dag.getEdges().add(edge);
//
//                logger.debug("Created DAG edge: from={} to={}", fromTaskName, toTaskName);
//            }
//        }
//
//        logger.info("Converted DAGSessionSpec to PEWorkflowDAG: nodes={}, edges={}", dag.getNodes().size(),
//                dag.getEdges().size());
//
//        return dag;
//    }

    @Override
    protected String getPayloadContent() {
        return "";
    }

    @Override
    public void innerCreatePowerjobWorkflow(
            boolean updateProcess, Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>//
                    , Map<String, ISqlTask>>> selectedTabTriggers, Optional<WorkflowUnEffectiveJudge> unEffectiveOpt) {

    }

    @Override
    protected SPIExecContext createSPIExecContext() {
        return null;
    }

    @Override
    protected WorkFlowBuildHistoryPayload createBuildHistoryPayload(Integer tisTaskId) {
        return null;
    }

    @Override
    protected Long runSPIWorkflow(BasicWorkflowInstance spiWorkflowId, JSONObject instanceParams) {
        return 0L;
    }

    @Override
    protected void setSPIWorkflowPayload(JSONObject appPayload) {

    }

    @Override
    protected AkkaWorkflow createWorkflowInstance(Long spiWorkflowId, ExecutePhaseRange execRange) {


        throw new UnsupportedOperationException();
    }
}
