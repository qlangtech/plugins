package com.qlangtech.tis.dag.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dag.actor.message.CancelWorkflow;
import com.qlangtech.tis.dag.actor.message.DispatchTask;
import com.qlangtech.tis.dag.actor.message.NodeCompleted;
import com.qlangtech.tis.dag.actor.message.QueryWorkflowStatus;
import com.qlangtech.tis.dag.actor.message.StartWorkflow;
import com.qlangtech.tis.dag.actor.message.UpdateContext;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmitParams;
import com.qlangtech.tis.datax.LifeCycleHook;
import com.qlangtech.tis.datax.WorkflowRuntimeStatus;
import com.qlangtech.tis.fullbuild.phasestatus.PhaseStatusCollection;
import com.qlangtech.tis.fullbuild.phasestatus.impl.DumpPhaseStatus;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.TaskSoapUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.powerjob.algorithm.WorkflowDAGUtils;
import com.qlangtech.tis.powerjob.model.InstanceStatus;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;
import com.qlangtech.tis.powerjob.model.WorkflowNodeType;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.qlangtech.tis.datax.DataXJobInfo.KEY_ALL_ROWS_APPROXIMATELY;

/**
 * Workflow Instance Actor
 * 每个workflow实例对应一个Actor，负责该实例的完整生命周期管理
 * <p>
 * 核心职责：
 * 1. 缓存WorkFlowBuildHistory和DAG定义，避免重复数据库查询
 * 2. 处理节点完成消息，更新DAG状态
 * 3. 计算就绪节点并分发任务
 * 4. 管理workflow生命周期，完成后自动销毁
 * <p>
 * 性能优势：
 * - 消除重复数据库查询（每个workflow只需2-3次查询）
 * - 消除重复DAG加载和计算
 * - 不同workflow实例完全并行，无锁竞争
 * - 同一workflow内串行处理，天然避免并发问题
 * <p>
 * 集群支持：
 * - 通过Cluster Sharding实现分布式部署
 * - 基于workflowInstanceId进行分片路由
 * - 支持故障自动恢复
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-02-01
 */
public class WorkflowInstanceActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowInstanceActor.class);

    //private DataXName dataXName;
    /**
     * Workflow实例ID（从消息中提取，作为Actor的身份标识）
     */
    private Integer taskId;
    private StartWorkflow startWorkflow;

    /**
     * 缓存的WorkFlowBuildHistory实例
     */
    private WorkFlowBuildHistory instance;

    /**
     * 缓存的DAG定义（包含运行时状态）
     */
    private PEWorkflowDAG dag;

    /**
     * 等待执行的任务队列
     * 用于控制并发执行的任务数量，避免对数据库造成过大压力
     */
    private final Queue<PEWorkflowDAG.Node> waitingQueue = new LinkedList<>();

    /**
     * 正在执行的任务集合（nodeId）
     * 用于跟踪当前正在执行的任务
     */
    private final Set<Long> runningTasks = new HashSet<>();

    /**
     * 最大并发任务数
     * 默认值：5
     * 可通过配置或StartWorkflow消息参数覆盖
     */
    private int maxConcurrentTasks = 5;
    private RpcServiceReference rpcRef;
    private Optional<PhaseStatusCollection> statusCollection;

    /**
     * NodeDispatcherActor引用，用于分发任务
     */
    private final ActorRef nodeDispatcher;

    /**
     * DAO依赖
     */
    private final IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO;

    /**
     * 空闲超时时间（分钟）
     * 如果workflow长时间没有消息，自动销毁Actor释放内存
     */
    // private static final int IDLE_TIMEOUT_MINUTES = 30;
    public WorkflowInstanceActor(ActorRef nodeDispatcher, IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO) {
        this.nodeDispatcher = nodeDispatcher;
        this.workflowBuildHistoryDAO = workflowBuildHistoryDAO;
    }

    public static Props props(ActorRef nodeDispatcher, IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO) {
        return Props.create(WorkflowInstanceActor.class, () -> new WorkflowInstanceActor(nodeDispatcher,
                workflowBuildHistoryDAO));
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        DataXJobSubmitParams submitParams = DataXJobSubmitParams.getDftIfEmpty();
        // 设置空闲超时，避免长时间运行的workflow占用内存
        getContext().setReceiveTimeout(submitParams.getTaskExpireHours());
        logger.info("WorkflowInstanceActor started, waiting for initialization message");
        this.maxConcurrentTasks = submitParams.pipelineParallelism;
        this.statusCollection = Optional.empty();
        this.rpcRef = TaskWorkerActor.getRpcClient();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        logger.info("WorkflowInstanceActor stopped: workflowInstanceId={}", taskId);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(StartWorkflow.class, this::handleStartWorkflow) //
                .match(NodeCompleted.class, this::handleNodeCompleted) //
                .match(UpdateContext.class, this::handleUpdateContext) //
                .match(CancelWorkflow.class, this::handleCancelWorkflow) //
                .match(QueryWorkflowStatus.class, this::handleQueryWorkflowStatus) //
                .match(ReceiveTimeout.class, this::handleTimeout) //
                .matchAny(msg -> logger.warn("Received unknown message: {}", msg)).build();
    }

    /**
     * 处理启动workflow消息
     * 这是Actor收到的第一条消息，用于初始化状态
     */
    private void handleStartWorkflow(StartWorkflow msg) {
        this.taskId = msg.getTaskId();
        Integer preTaskId = msg.getPreTaskId();
        if (preTaskId != null) {
            this.statusCollection = Optional.of(this.rpcRef.loadPhaseStatusFromLatest(preTaskId));
        }
        // this.dataXName = Objects.requireNonNull(msg.getDataXName(), "dataXName can not be null");
        this.startWorkflow = Objects.requireNonNull(msg, "msg can not be  null");
        logger.info("Handling StartWorkflow: workflowInstanceId={}", taskId);

        try {
            // 1. 从数据库加载workflow实例
            this.instance = workflowBuildHistoryDAO.loadFromWriteDB(taskId);
            if (instance == null) {
                throw new IllegalStateException("Workflow instance not found: " + taskId);
            }

            // 2. 加载DAG定义
            this.dag = loadDAGFromInstance(instance);
            if (dag == null || dag.getNodes() == null || dag.getNodes().isEmpty()) {
                throw new IllegalStateException("DAG definition is empty for workflow: " + taskId);
            }

            // 3. 验证DAG合法性
            if (!WorkflowDAGUtils.valid(dag)) {
                throw new IllegalStateException("Invalid DAG for workflow: " + taskId);
            }

            // 4. 初始化DAG运行时状态（所有节点标记为WAITING）
            initializeDAGRuntime(dag);

            // 5. 配置最大并发任务数
            configureMaxConcurrentTasks(msg);

            // 6. 更新实例状态为RUNNING
            instance.setState(ExecResult.DOING.getByteVal());
            instance.setStartTime(new Date());
            workflowBuildHistoryDAO.updateByPrimaryKeySelective(instance);

            // 7. 计算初始就绪节点并分发
            List<PEWorkflowDAG.Node> readyNodes = WorkflowDAGUtils.listReadyNodes(dag);
            logger.info("Initial ready nodes count: {}, workflowInstanceId={}", readyNodes.size(), taskId);

            dispatchReadyNodes(msg, readyNodes);

            // 8. 持久化DAG运行时状态
            persistDAGRuntime();

            logger.info("Workflow started successfully: workflowInstanceId={}", taskId);

        } catch (Exception e) {
            logger.error("Failed to start workflow: workflowInstanceId={}", taskId, e);
            // 标记workflow为失败
            if (instance != null) {
                instance.setState(ExecResult.FAILD.getByteVal());//.setInstanceStatus(InstanceStatus.FAILED.getDesc());
                instance.setEndTime(new Date());
                workflowBuildHistoryDAO.updateByPrimaryKeySelective(instance);
            }
            // 停止Actor
            getContext().stop(getSelf());
        }
    }

    /**
     * 处理节点完成消息（核心流转逻辑）
     * 这是最频繁调用的方法，优化重点
     */
    private void handleNodeCompleted(NodeCompleted msg) {
        logger.info("Handling NodeCompleted: workflowInstanceId={}, nodeId={}, status={}",
                msg.getWorkflowInstanceId(), msg.getNodeId(), msg.getStatus());

        try {
            // 1. 从运行集合中移除
            runningTasks.remove(msg.getNodeId());

            logger.info("Task completed: nodeId={}, runningTasks={}/{}, waitingQueue={}", msg.getNodeId(),
                    runningTasks.size(), maxConcurrentTasks, waitingQueue.size());

            // 2. 查找节点
            PEWorkflowDAG.Node node = findNode(dag, msg.getNodeId());
            if (node == null) {
                logger.error("Node not found: nodeId={}, workflowInstanceId={}", msg.getNodeId(), taskId);
                return;
            }

            // 3. 更新节点状态（直接在内存中更新，无需数据库查询）
            updateNodeStatus(node, msg.getStatus(), msg.getResult());

            // 4. 检查失败策略
            if (InstanceStatus.FAILED == msg.getStatus()) {
                if (node.getSkipWhenFailed() == null || !node.getSkipWhenFailed()) {
                    // 不允许跳过，终止workflow
                    logger.warn("Node failed and skipWhenFailed=false, terminating workflow: nodeId={}, " +
                            "workflowInstanceId={}", msg.getNodeId(), taskId);

                    terminateWorkflow(ExecResult.FAILD);
                    return;
                }
                logger.info("Node failed but skipWhenFailed=true, continuing workflow: nodeId={}, " +
                        "workflowInstanceId={}", msg.getNodeId(), taskId);
            }

            // 5. 持久化DAG运行时状态
            persistDAGRuntime();

            // 6. 判断workflow是否全部完成
            if (isWorkflowCompleted(dag)) {
                completeWorkflow();
                return;
            }

            // 7. 计算新的就绪节点
            List<PEWorkflowDAG.Node> readyNodes = WorkflowDAGUtils.listReadyNodes(dag);
            logger.info("New ready nodes count: {}, workflowInstanceId={}", readyNodes.size(), taskId);

            // 8. 分发就绪节点（会加入等待队列）
            dispatchReadyNodes(Objects.requireNonNull(this.startWorkflow, "startWorkflow can not be null"), readyNodes);

            // 9. 尝试从等待队列分发新任务（填补刚完成的任务空位）
            // 当 readyNodes 为空时，dispatchReadyNodes 会 early return 不调用 tryDispatchTasks
            // 因此这里必须独立调用，确保已完成任务释放的并发槽位能被等待中的任务利用
            tryDispatchTasks(Objects.requireNonNull(this.startWorkflow, "startWorkflow can not be null"));

            // 10. CONTROL 节点在 dispatchReadyNodes 中被同步标记为 SUCCEED，
            // 需要重新检查 workflow 是否已全部完成
            if (isWorkflowCompleted(dag)) {
                completeWorkflow();
                return;
            }

            logger.info("Node completed processed successfully: workflowInstanceId={}, nodeId={}", taskId
                    , msg.getNodeId());

        } catch (Exception e) {
            logger.error("Failed to process node completed: workflowInstanceId={}, nodeId={}", taskId,
                    msg.getNodeId(), e);
        }
    }

    /**
     * 处理更新上下文消息
     */
    private void handleUpdateContext(UpdateContext msg) {
        logger.info("Handling UpdateContext: workflowInstanceId={}", msg.getWorkflowInstanceId());

        try {
            // 合并上下文数据
            Map<String, String> context = parseContext(instance.getWfContext());
            context.putAll(msg.getContextData());

            // 持久化
            instance.setWfContext(JSON.toJSONString(context));
            workflowBuildHistoryDAO.updateByPrimaryKeySelective(instance);

            logger.info("Context updated: workflowInstanceId={}", taskId);

        } catch (Exception e) {
            logger.error("Failed to update context: workflowInstanceId={}", taskId, e);
        }
    }

    /**
     * 处理取消workflow消息
     */
    private void handleCancelWorkflow(CancelWorkflow msg) {
        logger.info("Handling CancelWorkflow: workflowInstanceId={}", msg.getWorkflowInstanceId());

        try {
            // 标记为STOPPED
            instance.setState(ExecResult.CANCEL.getByteVal());//.setInstanceStatus(InstanceStatus.STOPPED.getDesc());
            instance.setEndTime(new Date());
            workflowBuildHistoryDAO.updateByPrimaryKeySelective(instance);

            logger.info("Workflow cancelled: workflowInstanceId={}", taskId);

            // 停止Actor
            getContext().stop(getSelf());

        } catch (Exception e) {
            logger.error("Failed to cancel workflow: workflowInstanceId={}", taskId, e);
        }
    }

    /**
     * handle query workflow status from WorkflowInstanceActor in-memory state
     * build WorkflowRuntimeStatus from dag, waitingQueue, runningTasks without DB query
     */
    public void handleQueryWorkflowStatus(QueryWorkflowStatus msg) {
        logger.debug("Handling QueryWorkflowStatus: taskId={}", msg.getWorkflowInstanceId());

        try {
            WorkflowRuntimeStatus status = new WorkflowRuntimeStatus();
            status.setInstanceId(msg.getWorkflowInstanceId());

            if (this.instance != null) {
                ExecResult execStatus = ExecResult.parse(this.instance.getState());

                status.setStatus(String.valueOf(execStatus));
                if (this.instance.getStartTime() != null) {
                    status.setStartTime(this.instance.getStartTime().getTime());
                }
                if (this.instance.getOpTime() != null) {
                    status.setUpdateTime(this.instance.getOpTime().getTime());
                }
            } else {
                getSender().tell(new akka.actor.Status.Failure(
                        TisException.create("workflow instance not initialized yet, taskId:" + msg.getWorkflowInstanceId())), getSelf());
                // Actor was auto-created by Cluster Sharding without proper StartWorkflow initialization, stop it
                getContext().stop(getSelf());
                return;
            }

            status.appendDAG(this.dag);

            getSender().tell(status, getSelf());

        } catch (Exception e) {
            logger.error("Failed to query workflow status: taskId={}", msg.getWorkflowInstanceId(), e);
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
        }
    }

    /**
     * 处理空闲超时
     */
    private void handleTimeout(ReceiveTimeout msg) {
        logger.warn("Workflow instance idle timeout, stopping actor: workflowInstanceId={}", taskId);
        getContext().stop(getSelf());
    }

    // ==================== 辅助方法 ====================

    /**
     * 从WorkFlowBuildHistory加载DAG定义
     */
    private PEWorkflowDAG loadDAGFromInstance(WorkFlowBuildHistory instance) {
        File dagRuntimeJson = new File(instance.getDagRuntime());
        if (!dagRuntimeJson.exists()) {
            throw new IllegalStateException("DAG runtime is empty for workflow: " + instance.getId());
        }
        try {
            return JSON.parseObject(FileUtils.readFileToString(dagRuntimeJson, TisUTF8.get()), PEWorkflowDAG.class);
        } catch (IOException e) {
            throw new RuntimeException("path:" + dagRuntimeJson.getAbsolutePath(), e);
        }
    }

    /**
     * 初始化DAG运行时状态
     */
    private void initializeDAGRuntime(PEWorkflowDAG dag) {
        for (PEWorkflowDAG.Node node : dag.getNodes()) {
            if (node.getEnable() == null || node.getEnable()) {
                node.setStatus(InstanceStatus.WAITING);
            } else {
                // 禁用的节点标记为SUCCEED，不影响后续流程
                node.setStatus(InstanceStatus.SUCCEED);
            }
        }
    }

    /**
     * 查找节点
     */
    private PEWorkflowDAG.Node findNode(PEWorkflowDAG dag, Long nodeId) {
        return dag.getNodes().stream().filter(n -> n.getNodeId().equals(nodeId)).findFirst().orElse(null);
    }

    /**
     * 更新节点状态
     */
    private void updateNodeStatus(PEWorkflowDAG.Node node, InstanceStatus status, String result) {
        node.setStatus(status);
        node.setResult(result);
        node.setFinishedTime(new Date().toString());
    }

    /**
     * 判断workflow是否完成
     */
    private boolean isWorkflowCompleted(PEWorkflowDAG dag) {
        return dag.getNodes().stream().filter(n -> n.getEnable() == null || n.getEnable()).allMatch(n -> {
            InstanceStatus status = n.getStatus();
            return InstanceStatus.SUCCEED == status || InstanceStatus.FAILED == status;
        });
    }

    /**
     * 完成workflow
     */
    private void completeWorkflow() {
        logger.info("Workflow completed: workflowInstanceId={}", taskId);

        // 判断最终状态
        boolean hasFailure =
                dag.getNodes().stream().anyMatch(n -> InstanceStatus.FAILED == n.getStatus());
        ExecResult execResult = hasFailure ? ExecResult.FAILD : ExecResult.SUCCESS;
        //InstanceStatus finalStatus = hasFailure ? InstanceStatus.FAILED : InstanceStatus.SUCCEED;

//        instance.setInstanceStatus(finalStatus.getDesc());
//        instance.setEndTime(new Date());
//        workflowBuildHistoryDAO.updateByPrimaryKeySelective(instance);

        persistDAGRuntime();

        TaskSoapUtils.createTaskComplete(this.taskId, execResult, this.dag);

        logger.info("Workflow final status: {}, workflowInstanceId={}", execResult, taskId);

        // 停止Actor释放内存
        getContext().stop(getSelf());
    }

    /**
     * 终止workflow
     */
    private void terminateWorkflow(ExecResult status) {
        logger.warn("Terminating workflow: workflowInstanceId={}, status={}", taskId, status);

        // instance.setInstanceStatus(status.getDesc());
        instance.setState((byte) status.getValue());
        instance.setEndTime(new Date());
        workflowBuildHistoryDAO.updateByPrimaryKeySelective(instance);

        persistDAGRuntime();

        // 停止Actor
        getContext().stop(getSelf());
    }

    /**
     * 分发就绪节点
     */
    private void dispatchReadyNodes(StartWorkflow startWorkflow, List<PEWorkflowDAG.Node> readyNodes) {
        if (readyNodes == null || readyNodes.isEmpty()) {
            return;
        }

        // 先处理控制节点（同步执行，循环直到没有新的控制节点）
        List<PEWorkflowDAG.Node> controlNodes =
                readyNodes.stream().filter(n -> WorkflowNodeType.CONTROL == n.getNodeType()).collect(Collectors.toList());

        while (!controlNodes.isEmpty()) {
            // 同步处理所有控制节点
            handleControlNodes(controlNodes);

            // 重新计算就绪节点，可能有新的控制节点就绪
            List<PEWorkflowDAG.Node> newReadyNodes = WorkflowDAGUtils.listReadyNodes(dag);
            controlNodes = newReadyNodes.stream()
                    .filter(n -> WorkflowNodeType.CONTROL == n.getNodeType())
                    .collect(Collectors.toList());

            // 更新 readyNodes 为最新的就绪节点
            readyNodes = newReadyNodes;
        }

        // 过滤出任务节点
        List<PEWorkflowDAG.Node> taskNodes =
                readyNodes.stream().filter(n -> WorkflowNodeType.TASK == n.getNodeType()).collect(Collectors.toList());

        // 设置状态为QUEUED并加入队列
        for (PEWorkflowDAG.Node taskNode : taskNodes) {
            taskNode.setStatus(InstanceStatus.QUEUED);  // 立即改变状态，避免重复加入队列
            waitingQueue.add(taskNode);
            logger.info("Task added to waiting queue: nodeId={}, status=QUEUED", taskNode.getNodeId());
        }

        logger.info("Added {} tasks to waiting queue, current queue size: {}, running tasks: {}/{}", taskNodes.size()
                , waitingQueue.size(), runningTasks.size(), maxConcurrentTasks);

        // 尝试分发任务（受并发限制）
        tryDispatchTasks(startWorkflow);
    }

    /**
     * 处理控制节点
     * 控制节点同步执行，立即完成
     * 在TIS中，控制节点是虚拟节点，不需要实际执行任何逻辑，只需标记为成功
     */
    private void handleControlNodes(List<PEWorkflowDAG.Node> controlNodes) {
        if (controlNodes == null || controlNodes.isEmpty()) {
            return;
        }

        logger.info("Handling {} control nodes", controlNodes.size());

        for (PEWorkflowDAG.Node controlNode : controlNodes) {
            logger.info("Processing control node: nodeId={}, nodeName={}", controlNode.getNodeId(),
                    controlNode.getNodeName());

            // 设置开始时间
            controlNode.setStartTime(new Date().toString());

            // 控制节点立即标记为成功
            controlNode.setStatus(InstanceStatus.SUCCEED);
            controlNode.setResult("Control node completed successfully");

            // 设置完成时间
            controlNode.setFinishedTime(new Date().toString());

            logger.info("Control node completed: nodeId={}, nodeName={}", controlNode.getNodeId(),
                    controlNode.getNodeName());
        }

        // 持久化状态变更
        persistDAGRuntime();
    }

    /**
     * 尝试从等待队列分发任务
     * 受maxConcurrentTasks限制，避免对数据库造成过大压力
     */
    private void tryDispatchTasks(StartWorkflow startWorkflow) {
        while (!waitingQueue.isEmpty() && runningTasks.size() < maxConcurrentTasks) {
            PEWorkflowDAG.Node node = waitingQueue.poll();
            if (node == null) {
                break;
            }

            // 状态转换: QUEUED → RUNNING
            if (node.getStatus() != InstanceStatus.QUEUED) {
                logger.warn("Unexpected node status when dispatching: nodeId={}, expected=QUEUED, actual={}",
                        node.getNodeId(), node.getStatus());
            }
            node.setStatus(InstanceStatus.RUNNING);
            node.setStartTime(new Date().toString());

            // 加入运行集合
            runningTasks.add(node.getNodeId());

            if (node.getExecRole() == LifeCycleHook.Dump) {
                JSONObject nodeParams = node.getNodeParams();
                this.statusCollection.map((sc) -> {
                            DataXJobInfo dumpJobInfo = node.getDataXJobInfo();
                            DumpPhaseStatus.TableDumpStatus dumpStatus = sc.getDumpPhase().getTable(dumpJobInfo.getJobFileName());
                            if (dumpStatus != null) {
                                //  nodeParams.put(KEY_ALL_ROWS_APPROXIMATELY, dumpStatus.getAllRows());
                                logger.info("dataX job file:{},history taskId:{},approximately rows count:{}", dumpJobInfo.getJobFileName(), sc.getTaskid(), dumpStatus.getAllRows());
                            }
                            return dumpStatus;
                        }
                ).ifPresentOrElse((dumpStatus) -> {
                    nodeParams.put(KEY_ALL_ROWS_APPROXIMATELY, dumpStatus.getAllRows());
                }, () -> {
                    nodeParams.put(KEY_ALL_ROWS_APPROXIMATELY, -1);
                });
            }

            // 发送分发消息到NodeDispatcherActor
            DispatchTask dispatchMsg = new DispatchTask(node, Objects.requireNonNull(startWorkflow, "startWorkflow can not be null"));
            nodeDispatcher.tell(dispatchMsg, getSelf());

            logger.info("Dispatched task: nodeId={}, status=RUNNING, runningTasks={}/{}, waitingQueue={}",
                    node.getNodeId(), runningTasks.size(), maxConcurrentTasks, waitingQueue.size());
        }

        // 持久化状态变更
        persistDAGRuntime();
    }

    /**
     * 持久化DAG运行时状态到数据库
     */
    private void persistDAGRuntime() {
//        try {
//            String dagRuntimeJson = JSON.toJSONString(dag);
//            instance.setDagRuntime(dagRuntimeJson);
//            workflowBuildHistoryDAO.updateByPrimaryKeySelective(instance);
//        } catch (Exception e) {
//            logger.error("Failed to persist DAG runtime: workflowInstanceId={}", workflowInstanceId, e);
//        }
    }

    /**
     * 配置最大并发任务数
     * 优先级：消息参数 > 工作流上下文配置 > 默认值
     *
     * @param msg StartWorkflow消息
     */
    private void configureMaxConcurrentTasks(StartWorkflow msg) {
        // 1. 优先使用消息中的配置
        if (msg.getMaxConcurrentTasks() != null && msg.getMaxConcurrentTasks() > 0) {
            this.maxConcurrentTasks = msg.getMaxConcurrentTasks();
            logger.info("Using maxConcurrentTasks from message: {}", maxConcurrentTasks);
            return;
        }

        // 2. 尝试从工作流上下文读取配置
        String wfContext = instance.getWfContext();
        if (StringUtils.isNotEmpty(wfContext)) {
            try {
                Map<String, String> context = JSON.parseObject(wfContext, Map.class);
                String maxConcurrent = context.get("maxConcurrentTasks");
                if (StringUtils.isNotEmpty(maxConcurrent)) {
                    this.maxConcurrentTasks = Integer.parseInt(maxConcurrent);
                    logger.info("Using maxConcurrentTasks from workflow context: {}", maxConcurrentTasks);
                    return;
                }
            } catch (Exception e) {
                logger.warn("Failed to parse maxConcurrentTasks from workflow context", e);
            }
        }

        // 3. 使用默认值
        logger.info("Using default maxConcurrentTasks: {}", maxConcurrentTasks);
    }

    /**
     * 解析上下文JSON
     */
    private Map<String, String> parseContext(String contextJson) {
        if (StringUtils.isEmpty(contextJson)) {
            return new java.util.HashMap<>();
        }
        return JSON.parseObject(contextJson, Map.class);
    }
}
