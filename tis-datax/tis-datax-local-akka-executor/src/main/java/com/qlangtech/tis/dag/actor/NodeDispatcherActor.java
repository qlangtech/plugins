package com.qlangtech.tis.dag.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.routing.ClusterRouterPool;
import akka.cluster.routing.ClusterRouterPoolSettings;
import akka.routing.Broadcast;
import akka.routing.RoundRobinPool;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.tis.dag.actor.message.AbstractTaskExecutionMessage;
import com.qlangtech.tis.dag.actor.message.CancelTask;
import com.qlangtech.tis.dag.actor.message.CancelTasks;
import com.qlangtech.tis.dag.actor.message.DispatchTask;
import com.qlangtech.tis.dag.actor.message.NodeCompleted;
import com.qlangtech.tis.dag.actor.message.NodeTimeout;
import com.qlangtech.tis.dag.actor.message.QueryActiveWorkers;
import com.qlangtech.tis.dag.actor.message.TaskExecutionMessage;
import com.qlangtech.tis.dag.actor.message.TaskStarted;
import com.qlangtech.tis.datax.ActorSystemStatus;
import com.qlangtech.tis.datax.DataXJobSubmitParams;
import com.qlangtech.tis.powerjob.model.InstanceStatus;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.workflow.dao.IDAGNodeExecutionDAO;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.pojo.DagNodeExecution;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 节点分发器 Actor
 * 负责任务分发路由、超时监控和消息转发
 * <p>
 * 核心职责：
 * 1. 初始化任务路由器（ClusterRouterPool）
 * 2. 分发任务到 Worker
 * 3. 设置超时监控
 * 4. 记录任务执行位置
 * 5. 转发NodeCompleted消息到WorkflowInstanceRegion
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class NodeDispatcherActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(NodeDispatcherActor.class);

    /**
     * 默认任务超时时间（毫秒）
     */
    // private static final long DEFAULT_TIMEOUT_MILLIS = 3600000L; // 1 hour

    /**
     * 任务路由器（ClusterRouterPool）
     */
    private ActorRef taskRouter;
    private DataXJobSubmitParams submitParams;
    private String clusterSelfAddress;
    /**
     * WorkflowInstance Cluster Sharding Region 引用
     * 通过 SetWorkflowInstanceRegion 消息在 Sharding 初始化后注入
     * 用于替代直接 entity ActorRef 转发消息，确保消息被正确路由
     */
    private ActorRef workflowInstanceRegion;

    /**
     * Active worker registry: taskId_nodeId -> WorkerInfo(startTime, sender)
     */
    private final Map<String, ActiveWorkerEntry> activeWorkerRegistry = new ConcurrentHashMap<>();

    /**
     * DAGNodeExecutionDAO
     */
    private final IDAGNodeExecutionDAO dagNodeExecutionDAO;

    /**
     * WorkFlowBuildHistoryDAO
     */
    private final IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO;

    public NodeDispatcherActor(IDAGNodeExecutionDAO dagNodeExecutionDAO,
                               IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO) {
        this.dagNodeExecutionDAO = dagNodeExecutionDAO;
        this.workflowBuildHistoryDAO = workflowBuildHistoryDAO;
    }

    public static Props props(IDAGNodeExecutionDAO dagNodeExecutionDAO,
                              IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO) {
        return Props.create(NodeDispatcherActor.class, dagNodeExecutionDAO, workflowBuildHistoryDAO);
    }

    /**
     * 初始化路由器
     * 统一使用 ClusterRouterPool，单节点集群时自动退化为本地路由
     */
    @Override
    public void preStart() throws Exception {
        super.preStart();
        this.taskRouter = createTaskRouter();
        this.submitParams = DataXJobSubmitParams.getDftIfEmpty();
        this.clusterSelfAddress = String.valueOf(Cluster.get(getContext().getSystem()).selfAddress());
        logger.info("NodeDispatcherActor started, task router created");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(SetWorkflowInstanceRegion.class, msg -> {
                    this.workflowInstanceRegion = msg.getRegion();
                    logger.info("WorkflowInstanceRegion set: {}", workflowInstanceRegion.path());
                }) //
                .match(DispatchTask.class, this::handleDispatchTask) //
                .match(NodeCompleted.class, this::handleNodeCompleted) //
                .match(NodeTimeout.class, this::handleNodeTimeout) //
                .match(TaskStarted.class, this::handleTaskStarted) //
                .match(CancelTasks.class, this::handleCancelTasks) //
                .match(QueryActiveWorkers.class, this::handleQueryActiveWorkers) //
                .matchAny(msg -> logger.warn("Received unknown message: {}", msg)).build();
    }

    /**
     * 分发任务
     * <p>
     * 实现步骤：
     * 1. 创建任务执行上下文
     * 2. 加载工作流上下文
     * 3. 创建或更新节点执行记录（状态为 RUNNING）
     * 4. 路由到 Worker
     * 5. 设置超时监控
     *
     * @param msg 分发任务消息
     */
    private void handleDispatchTask(DispatchTask msg) {
        PEWorkflowDAG.Node node = msg.getNode();
        node.checkIfDumpNode();
        logger.info("Handling DispatchTask: instanceId={}, nodeId={}", msg.getTaskId(),
                node.getNodeId());

        try {
            // 1. 加载工作流上下文
            Map<String, String> workflowContext = Maps.newHashMap(); // parseContext(instance.getWfContext());

            // 2. 创建节点执行记录
            DagNodeExecution execution = getDagNodeExecution(msg);
            dagNodeExecutionDAO.insertSelective(execution);

            AbstractTaskExecutionMessage workflowCfg //
                    = Objects.requireNonNull(msg.getStartWorkflow(), "workflowCfg can not be null");
            // 3. 创建任务执行消息
            TaskExecutionMessage taskMsg = new TaskExecutionMessage(msg.getTaskId(), node, workflowContext, msg.getDataXName());
            taskMsg.setTimeoutMillis(submitParams.getTaskExpireHours().toMillis());
            taskMsg.setPluginCfgsMetas(workflowCfg.getPluginCfgsMetas());
            taskMsg.setDryRun(workflowCfg.isDryRun());
            taskMsg.setTriggerTimestamp(workflowCfg.getTriggerTimestamp());


            // 4. 路由到 Worker
            // 使用getSelf()作为sender，让TaskWorkerActor回复给NodeDispatcherActor
            // 这样NodeDispatcherActor可以清除activeWorkerRegistry并取消超时，再转发给WorkflowInstanceActor
            taskRouter.tell(taskMsg, getSelf());

            // 5. Register active worker
            String workerKey = msg.getTaskId() + "_" + node.getNodeId();
            activeWorkerRegistry.put(workerKey, new ActiveWorkerEntry(msg.getTaskId(), node.getNodeId(), System.currentTimeMillis()));

            // 6. 设置超时监控
            scheduleTimeout(msg.getTaskId(), node.getNodeId(), submitParams.getTaskExpireHours());

            logger.info("Task dispatched: instanceId={}, nodeId={}", msg.getTaskId(),
                    node.getNodeId());

        } catch (Exception e) {
            logger.error("Failed to dispatch task: instanceId={}, nodeId={}", msg.getTaskId(),
                    node.getNodeId(), e);
            // 通知 WorkflowInstanceActor 任务分发失败
            // 通过 Cluster Sharding Region 转发，确保消息被正确路由
            NodeCompleted failureMsg = new NodeCompleted(msg.getTaskId(), node.getNodeId(),
                    InstanceStatus.FAILED, "Failed to dispatch task: " + e.getMessage());
            workflowInstanceRegion.tell(failureMsg, getSelf());
        }
    }

    private DagNodeExecution getDagNodeExecution(DispatchTask msg) {
        PEWorkflowDAG.Node node = msg.getNode();
        DagNodeExecution execution = new DagNodeExecution();
        execution.setWorkflowInstanceId(msg.getTaskId());
        execution.setNodeId(node.getNodeId());
        execution.setNodeName(node.getNodeName());
        execution.setNodeType(node.getNodeType().toString());
        execution.setStatus(InstanceStatus.RUNNING.getDesc());
        execution.setStartTime(new Date());
        execution.setWorkerAddress(getLocalAddress());
        execution.setSkipWhenFailed(node.getSkipWhenFailed());
        execution.setEnable(node.getEnable());
        return execution;
    }

    /**
     * 处理节点超时消息
     * <p>
     * 当任务执行超时时，通知 WorkflowInstanceActor 该节点失败，
     * 避免 Worker 假死导致整个 workflow 永远挂起
     *
     * @param msg 节点超时消息
     */
    private void handleNodeTimeout(NodeTimeout msg) {
        String workerKey = msg.getWorkflowInstanceId() + "_" + msg.getNodeId();
        ActiveWorkerEntry entry = activeWorkerRegistry.remove(workerKey);

        if (entry == null) {
            // 任务已经正常完成，超时消息可以忽略
            logger.debug("NodeTimeout ignored (task already completed): workflowInstanceId={}, nodeId={}",
                    msg.getWorkflowInstanceId(), msg.getNodeId());
            return;
        }

        logger.error("Task execution timeout: workflowInstanceId={}, nodeId={}, elapsed={}ms",
                msg.getWorkflowInstanceId(), msg.getNodeId(),
                System.currentTimeMillis() - entry.startTime);

        // 向 WorkflowInstanceActor 发送失败消息（通过 Cluster Sharding Region 转发）
        NodeCompleted failureMsg = new NodeCompleted(
                msg.getWorkflowInstanceId(),
                msg.getNodeId(),
                InstanceStatus.FAILED,
                "Task execution timeout after " + (System.currentTimeMillis() - entry.startTime) + "ms"
        );
        workflowInstanceRegion.tell(failureMsg, getSelf());
    }

    /**
     * 处理节点完成消息
     * <p>
     * TaskWorkerActor 完成任务后回复给 NodeDispatcherActor（因为 tell 时 sender=getSelf()）
     * NodeDispatcherActor 负责：
     * 1. 从 activeWorkerRegistry 移除条目（使后续超时定时器被忽略）
     * 2. 转发 NodeCompleted 给原始请求方 WorkflowInstanceActor
     *
     * @param msg 节点完成消息
     */
    private void handleNodeCompleted(NodeCompleted msg) {
        String workerKey = msg.getWorkflowInstanceId() + "_" + msg.getNodeId();
        ActiveWorkerEntry entry = activeWorkerRegistry.remove(workerKey);

        if (entry == null) {
            // 可能是超时已经触发过了，忽略重复的完成消息
            logger.warn("NodeCompleted but no active worker entry found (possibly already timed out): "
                    + "workflowInstanceId={}, nodeId={}, status={}", msg.getWorkflowInstanceId(), msg.getNodeId(), msg.getStatus());
            return;
        }

        logger.info("NodeCompleted: workflowInstanceId={}, nodeId={}, status={}, forwarding to WorkflowInstanceActor via shard region",
                msg.getWorkflowInstanceId(), msg.getNodeId(), msg.getStatus());

        // 通过 Cluster Sharding Region 转发给 WorkflowInstanceActor，确保消息被正确路由
        workflowInstanceRegion.tell(msg, getSelf());
    }

    /**
     * Handle TaskStarted message from TaskWorkerActor.
     * Updates the activeWorkerRegistry with the real cluster address
     * of the worker node that is executing the task.
     *
     * @param msg TaskStarted message containing actual worker address
     */
    private void handleTaskStarted(TaskStarted msg) {
        String workerKey = msg.getTaskId() + "_" + msg.getNodeId();
        ActiveWorkerEntry entry = activeWorkerRegistry.get(workerKey);
        if (entry != null) {
            entry.workerAddress = msg.getWorkerAddress();
            logger.info("TaskStarted: taskId={}, nodeId={}, workerAddress={}",
                    msg.getTaskId(), msg.getNodeId(), msg.getWorkerAddress());
        } else {
            logger.warn("TaskStarted received but no active worker entry found: taskId={}, nodeId={}",
                    msg.getTaskId(), msg.getNodeId());
        }
    }

    /**
     * Handle cancel tasks request
     * Remove all active worker entries for the given taskId so that
     * subsequent NodeCompleted / NodeTimeout messages from those workers will be silently ignored.
     *
     * @param msg CancelTasks message containing taskId and running nodeIds
     */
    private void handleCancelTasks(CancelTasks msg) {
        int removed = 0;
        for (Long nodeId : msg.getRunningNodeIds()) {
            String workerKey = msg.getTaskId() + "_" + nodeId;
            ActiveWorkerEntry entry = activeWorkerRegistry.remove(workerKey);
            if (entry != null) {
                removed++;
            }
        }

        // Broadcast CancelTask to all TaskWorkerActor routees via the router
        // Each routee checks if it is executing the matching taskId and interrupts its thread
        taskRouter.tell(new Broadcast(new CancelTask(msg.getTaskId())), getSelf());

        logger.info("CancelTasks processed: taskId={}, requested={}, removed={}, broadcast sent",
                msg.getTaskId(), msg.getRunningNodeIds().size(), removed);
    }

    /**
     * 创建任务路由器
     * 使用 ClusterRouterPool 支持单节点和多节点场景
     * <p>
     * 实现说明：
     * - 使用 RoundRobinPool 轮询策略
     * - 本地 Worker 数量：10
     * - 集群最大 Worker 数量：100
     * - 允许本地路由：true
     * - 使用角色：worker
     *
     * @return 任务路由器
     */
    private ActorRef createTaskRouter() {
        logger.info("Creating task router with ClusterRouterPool...");

        try {
            DataXJobSubmitParams params = DataXJobSubmitParams.getDftIfEmpty();
            int maxPerNode = Objects.requireNonNull(params.maxInstancesPerNode, "");
            int maxTotal = params.maxTotalNrOfInstances;

            ClusterRouterPoolSettings settings = new ClusterRouterPoolSettings(
                    maxTotal,      // maxTotalNrOfInstances: 集群总共最多Worker实例数
                    maxPerNode,    // maxInstancesPerNode: 每个节点最多Worker实例数
                    true,          // allowLocalRoutees: 允许本地路由
                    Sets.newHashSet("worker")  // useRole: 只在worker角色节点上创建routee
            );

            ActorRef router = getContext().actorOf(new ClusterRouterPool(new RoundRobinPool(maxPerNode),
                    settings).props(TaskWorkerActor.props()), "task-worker-cluster-pool");

            logger.info("Task router created successfully: {}, maxPerNode={}, maxTotal={}, useRole=worker",
                    router.path(), maxPerNode, maxTotal);
            return router;

        } catch (Exception e) {
            logger.error("Failed to create task router", e);
            throw new IllegalStateException("Failed to create task router", e);
        }
    }

    /**
     * 设置超时监控
     * <p>
     * 实现说明：
     * 使用 Akka Scheduler 在指定时间后发送 NodeTimeout 消息给自己
     * 然后由自己转发给 WorkflowInstanceActor
     *
     * @param workflowInstanceId 工作流实例 ID
     * @param nodeId             节点 ID
     * @param timeout            超时时间（毫秒）
     */
    private void scheduleTimeout(Integer workflowInstanceId, Long nodeId, Duration timeout) {
        getContext().getSystem().scheduler().scheduleOnce(timeout, getSelf(), new NodeTimeout(workflowInstanceId, nodeId),
                getContext().getDispatcher(), getSelf());

        logger.debug("Timeout scheduled: instanceId={}, nodeId={}, timeout={}ms", workflowInstanceId, nodeId,
                timeout.toMillis());
    }

    /**
     * 获取本地地址
     *
     * @return 本地地址
     */
    private String getLocalAddress() {
        //try {
        return NetUtils.getHost();
//            InetAddress address = InetAddress.getLocalHost();
//            return address.getHostAddress();
//        } catch (Exception e) {
//            logger.warn("Failed to get local address", e);
//            return "unknown";
//        }
    }

    /**
     * 解析工作流上下文
     *
     * @param wfContext 工作流上下文 JSON 字符串
     * @return 上下文 Map
     */
    private Map<String, String> parseContext(String wfContext) {
        if (StringUtils.isEmpty(wfContext)) {
            return new HashMap<>();
        }
        try {
            return JSON.parseObject(wfContext, Map.class);
        } catch (Exception e) {
            logger.warn("Failed to parse workflow context: {}", wfContext, e);
            return new HashMap<>();
        }
    }

    /**
     * Handle query active workers request
     * Returns current active worker list to the sender
     */
    private void handleQueryActiveWorkers(QueryActiveWorkers msg) {
        List<ActorSystemStatus.ActiveWorkerInfo> workers = new ArrayList<>();
        for (Map.Entry<String, ActiveWorkerEntry> entry : activeWorkerRegistry.entrySet()) {
            ActiveWorkerEntry workerEntry = entry.getValue();
            ActorSystemStatus.ActiveWorkerInfo info = new ActorSystemStatus.ActiveWorkerInfo();
            info.setActorPath(taskRouter != null ? String.valueOf(taskRouter.path()) : "unknown");
            info.setTaskId(workerEntry.taskId);
            info.setWorkerAddress(workerEntry.workerAddress != null
                    ? workerEntry.workerAddress : this.clusterSelfAddress);
            info.setNodeId(workerEntry.nodeId);
            info.setStartTime(workerEntry.startTime);
            workers.add(info);
        }
        getSender().tell(workers, getSelf());
    }

    /**
     * Active worker entry for tracking dispatched tasks
     */
    private static class ActiveWorkerEntry {
        final Integer taskId;
        final Long nodeId;
        final long startTime;
        /**
         * Actual cluster address of the worker node, set by TaskStarted message
         */
        volatile String workerAddress;

        ActiveWorkerEntry(Integer taskId, Long nodeId, long startTime) {
            this.taskId = taskId;
            this.nodeId = nodeId;
            this.startTime = startTime;
        }
    }

    /**
     * 内部消息：注入 WorkflowInstance Cluster Sharding Region 引用
     * <p>
     * 解决循环依赖问题：NodeDispatcherActor 先于 ClusterSharding 创建，
     * 因此需要在 Sharding 初始化完成后通过此消息注入 region 引用。
     */
    public static class SetWorkflowInstanceRegion implements Serializable {
        private static final long serialVersionUID = 1L;
        private final ActorRef region;

        public SetWorkflowInstanceRegion(ActorRef region) {
            this.region = Objects.requireNonNull(region, "region can not be null");
        }

        public ActorRef getRegion() {
            return region;
        }
    }
}
