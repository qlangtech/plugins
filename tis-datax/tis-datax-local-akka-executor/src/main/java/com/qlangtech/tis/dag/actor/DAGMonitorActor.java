package com.qlangtech.tis.dag.actor;

import akka.actor.AbstractActor;
import akka.actor.Props;
import com.qlangtech.tis.dag.actor.message.QueryRunningQueue;
import com.qlangtech.tis.dag.actor.message.QueryWaitingQueue;
import com.qlangtech.tis.dag.actor.message.QueryWorkflowStatus;
import com.qlangtech.tis.workflow.dao.IDAGNodeExecutionDAO;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.pojo.DagNodeExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DAG 监控器 Actor
 * 负责实时监控、状态查询、队列统计
 *
 * 核心职责：
 * 1. 查询工作流状态
 * 2. 查询等待队列
 * 3. 查询执行队列
 * 4. 队列统计
 *
 * 设计说明：
 * - 只读操作，不修改工作流状态
 * - 支持实时查询和历史查询
 * - 提供多维度的队列统计信息
 * - 用于 Web UI 展示和监控告警
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class DAGMonitorActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(DAGMonitorActor.class);

    private final IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO;
    private final IDAGNodeExecutionDAO dagNodeExecutionDAO;

    public DAGMonitorActor(IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO,
                          IDAGNodeExecutionDAO dagNodeExecutionDAO) {
        this.workflowBuildHistoryDAO = workflowBuildHistoryDAO;
        this.dagNodeExecutionDAO = dagNodeExecutionDAO;
    }

    public static Props props(IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO,
                             IDAGNodeExecutionDAO dagNodeExecutionDAO) {
        return Props.create(DAGMonitorActor.class,
                () -> new DAGMonitorActor(workflowBuildHistoryDAO, dagNodeExecutionDAO));
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        logger.info("DAGMonitorActor started: {}", getSelf().path());
    }

    @Override
    public void postStop() throws Exception {
        logger.info("DAGMonitorActor stopped: {}", getSelf().path());
        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(QueryWorkflowStatus.class, this::handleQueryWorkflowStatus)
                .match(QueryWaitingQueue.class, this::handleQueryWaitingQueue)
                .match(QueryRunningQueue.class, this::handleQueryRunningQueue)
                .matchAny(msg -> logger.warn("Received unknown message: {}", msg))
                .build();
    }

    /**
     * 处理查询工作流状态
     *
     * 实现步骤：
     * 1. 查询工作流实例信息
     * 2. 查询所有节点执行记录
     * 3. 构建 WorkflowRuntimeStatus 对象
     * 4. 返回给调用者
     *
     * 返回信息包括：
     * - 工作流实例 ID
     * - 工作流状态（WAITING/RUNNING/SUCCEED/FAILED）
     * - 所有节点的状态列表
     * - 开始时间、更新时间
     * - 统计信息（总节点数、完成数、失败数等）
     *
     * @param msg 查询消息
     */
    private void handleQueryWorkflowStatus(QueryWorkflowStatus msg) {
        logger.debug("Handling QueryWorkflowStatus: instanceId={}", msg.getWorkflowInstanceId());

        try {
            // TODO: 实现查询工作流状态

            // 1. 查询工作流实例
            // WorkFlowBuildHistory instance = workflowBuildHistoryDAO.selectByPrimaryKey(msg.getWorkflowInstanceId());
            // if (instance == null) {
            //     logger.warn("Workflow instance not found: instanceId={}", msg.getWorkflowInstanceId());
            //     getSender().tell(null, getSelf());
            //     return;
            // }

            // 2. 查询所有节点执行记录
            // List<DAGNodeExecution> nodes = dagNodeExecutionDAO.selectByWorkflowInstanceId(msg.getWorkflowInstanceId());

            // 3. 构建 WorkflowRuntimeStatus 对象
            // WorkflowRuntimeStatus status = new WorkflowRuntimeStatus(
            //     instance.getId(),
            //     instance.getInstanceStatus()
            // );

            // 4. 设置节点状态列表
           //  List<NodeStatus> nodeStatusList = convertToNodeStatus(nodes);
            // status.setNodes(nodeStatusList);

            // 5. 设置时间信息
            // status.setStartTime(instance.getStartTime() != null ? instance.getStartTime().getTime() : null);
            // status.setUpdateTime(instance.getOpTime() != null ? instance.getOpTime().getTime() : null);
            // if (instance.getEndTime() != null) {
            //     status.setEndTime(instance.getEndTime().getTime());
            // }

            // 6. 计算统计信息
            // Map<String, Integer> statistics = calculateStatistics(nodes);
            // status.setTotalNodes(statistics.get("total"));
            // status.setWaitingNodes(statistics.get("waiting"));
            // status.setRunningNodes(statistics.get("running"));
            // status.setSucceedNodes(statistics.get("succeed"));
            // status.setFailedNodes(statistics.get("failed"));

            // 7. 返回结果
            // getSender().tell(status, getSelf());

            logger.debug("Workflow status queried successfully: instanceId={}", msg.getWorkflowInstanceId());

        } catch (Exception e) {
            logger.error("Failed to query workflow status: instanceId={}", msg.getWorkflowInstanceId(), e);
            // 返回错误信息
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
        }
    }

    /**
     * 处理查询等待队列
     *
     * 实现步骤：
     * 1. 查询所有 WAITING 状态的节点
     * 2. 按优先级或创建时间排序
     * 3. 返回节点列表
     *
     * 用途：
     * - Web UI 展示等待队列
     * - 监控等待队列长度
     * - 分析任务调度瓶颈
     *
     * @param msg 查询消息
     */
    private void handleQueryWaitingQueue(QueryWaitingQueue msg) {
        logger.debug("Handling QueryWaitingQueue: instanceId={}", msg.getWorkflowInstanceId());

        try {
            // TODO: 实现查询等待队列

            // 1. 查询等待节点
            // List<DAGNodeExecution> waitingNodes;
            // if (msg.getWorkflowInstanceId() != null) {
            //     // 查询指定工作流的等待节点
            //     waitingNodes = dagNodeExecutionDAO.selectWaitingNodes(msg.getWorkflowInstanceId());
            // } else {
            //     // 查询所有工作流的等待节点
            //     waitingNodes = dagNodeExecutionDAO.selectAllWaitingNodes();
            // }

            // 2. 按创建时间排序（可选）
            // waitingNodes.sort(Comparator.comparing(DAGNodeExecution::getCreateTime));

            // 3. 转换为 NodeStatus 列表
            // List<NodeStatus> nodeStatusList = convertToNodeStatus(waitingNodes);

            // 4. 返回结果
            // getSender().tell(nodeStatusList, getSelf());

            logger.debug("Waiting queue queried successfully: instanceId={}, count={}",
                    msg.getWorkflowInstanceId(), 0);

        } catch (Exception e) {
            logger.error("Failed to query waiting queue: instanceId={}", msg.getWorkflowInstanceId(), e);
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
        }
    }

    /**
     * 处理查询执行队列
     *
     * 实现步骤：
     * 1. 查询所有 RUNNING 状态的节点
     * 2. 支持按 Worker 地址过滤
     * 3. 返回节点列表
     *
     * 用途：
     * - Web UI 展示执行队列
     * - 监控各 Worker 的负载
     * - 分析任务执行情况
     *
     * @param msg 查询消息
     */
    private void handleQueryRunningQueue(QueryRunningQueue msg) {
        logger.debug("Handling QueryRunningQueue: instanceId={}, workerAddress={}",
                msg.getWorkflowInstanceId(), msg.getWorkerAddress());

        try {
            // TODO: 实现查询执行队列

            // 1. 查询运行节点
            // List<DAGNodeExecution> runningNodes;
            // if (msg.getWorkerAddress() != null) {
            //     // 查询指定 Worker 上的运行节点
            //     runningNodes = dagNodeExecutionDAO.selectRunningNodesByWorker(msg.getWorkerAddress());
            // } else if (msg.getWorkflowInstanceId() != null) {
            //     // 查询指定工作流的运行节点
            //     runningNodes = dagNodeExecutionDAO.selectRunningNodes(msg.getWorkflowInstanceId());
            // } else {
            //     // 查询所有工作流的运行节点
            //     runningNodes = dagNodeExecutionDAO.selectAllRunningNodes();
            // }

            // 2. 转换为 NodeStatus 列表
            // List<NodeStatus> nodeStatusList = convertToNodeStatus(runningNodes);

            // 3. 返回结果
            // getSender().tell(nodeStatusList, getSelf());

            logger.debug("Running queue queried successfully: instanceId={}, workerAddress={}, count={}",
                    msg.getWorkflowInstanceId(), msg.getWorkerAddress(), 0);

        } catch (Exception e) {
            logger.error("Failed to query running queue: instanceId={}, workerAddress={}",
                    msg.getWorkflowInstanceId(), msg.getWorkerAddress(), e);
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
        }
    }

    /**
     * 转换为 NodeStatus 列表
     *
     * 将 DAGNodeExecution 实体转换为 NodeStatus 消息对象
     *
     * @param executions 节点执行记录列表
     * @return NodeStatus 列表
     */
//    private List<NodeStatus> convertToNodeStatus(List<DagNodeExecution> executions) {
//        if (executions == null || executions.isEmpty()) {
//            return new ArrayList<>();
//        }
//
//        return executions.stream().map(execution -> {
//            NodeStatus nodeStatus = new NodeStatus();
//            nodeStatus.setNodeId(execution.getNodeId());
//            nodeStatus.setNodeName(execution.getNodeName());
//            nodeStatus.setNodeType(execution.getNodeType());
//            nodeStatus.setStatus(execution.getStatus());
//            nodeStatus.setResult(execution.getResult());
//            nodeStatus.setStartTime(execution.getStartTime() != null ? execution.getStartTime().getTime() : null);
//            nodeStatus.setFinishedTime(execution.getFinishedTime() != null ? execution.getFinishedTime().getTime() : null);
//            nodeStatus.setWorkerAddress(execution.getWorkerAddress());
//            nodeStatus.setRetryTimes(execution.getRetryTimes());
//
//            return nodeStatus;
//        }).collect(Collectors.toList());
//    }

    /**
     * 计算统计信息
     *
     * 统计各状态的节点数量
     *
     * @param nodes 节点执行记录列表
     * @return 统计信息 Map
     */
    private Map<String, Integer> calculateStatistics(List<DagNodeExecution> nodes) {
        Map<String, Integer> statistics = new HashMap<>();

        // TODO: 实现统计逻辑
        // int total = nodes.size();
        // int waiting = 0;
        // int running = 0;
        // int succeed = 0;
        // int failed = 0;
        //
        // for (DAGNodeExecution node : nodes) {
        //     String status = node.getStatus();
        //     if ("WAITING".equals(status)) {
        //         waiting++;
        //     } else if ("RUNNING".equals(status)) {
        //         running++;
        //     } else if ("SUCCEED".equals(status)) {
        //         succeed++;
        //     } else if ("FAILED".equals(status)) {
        //         failed++;
        //     }
        // }
        //
        // statistics.put("total", total);
        // statistics.put("waiting", waiting);
        // statistics.put("running", running);
        // statistics.put("succeed", succeed);
        // statistics.put("failed", failed);

        statistics.put("total", 0);
        statistics.put("waiting", 0);
        statistics.put("running", 0);
        statistics.put("succeed", 0);
        statistics.put("failed", 0);

        return statistics;
    }

    /**
     * 查询队列统计信息
     *
     * 提供全局的队列统计，用于监控面板
     *
     * @return 队列统计信息
     */
    public Map<String, Object> getQueueStatistics() {
        Map<String, Object> statistics = new HashMap<>();

        try {
            // TODO: 实现队列统计
            // 1. 统计所有等待节点数
            // int totalWaiting = dagNodeExecutionDAO.countWaitingNodes();
            // statistics.put("totalWaiting", totalWaiting);

            // 2. 统计所有运行节点数
            // int totalRunning = dagNodeExecutionDAO.countRunningNodes();
            // statistics.put("totalRunning", totalRunning);

            // 3. 统计各 Worker 的负载
            // Map<String, Integer> workerLoad = dagNodeExecutionDAO.countRunningNodesByWorker();
            // statistics.put("workerLoad", workerLoad);

            // 4. 统计运行中的工作流数
            // int runningWorkflows = workflowBuildHistoryDAO.countRunningWorkflows();
            // statistics.put("runningWorkflows", runningWorkflows);

            statistics.put("totalWaiting", 0);
            statistics.put("totalRunning", 0);
            statistics.put("runningWorkflows", 0);

        } catch (Exception e) {
            logger.error("Failed to get queue statistics", e);
        }

        return statistics;
    }
}
