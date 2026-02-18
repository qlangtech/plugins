package com.qlangtech.tis.dag.actor;

import akka.actor.AbstractActor;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.dag.actor.message.NodeCompleted;
import com.qlangtech.tis.dag.actor.message.TaskExecutionMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.RpcUtils;
import com.qlangtech.tis.datax.executor.BasicTISTableDumpProcessor;
import com.qlangtech.tis.datax.powerjob.SplitTabSync;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.powerjob.TriggersConfig;
import com.qlangtech.tis.powerjob.model.InstanceStatus;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.qlangtech.tis.datax.DataXJobInfo.KEY_ALL_ROWS_APPROXIMATELY;
import static com.qlangtech.tis.datax.executor.BasicTISTableDumpProcessor.reportError;

/**
 * 任务执行 Worker Actor
 * 负责执行具体的数据处理任务
 * <p>
 * 核心职责：
 * 1. 接收任务执行消息
 * 2. 加载任务上下文
 * 3. 执行 DataflowTask.run() 方法
 * 4. 捕获执行结果和异常
 * 5. 发送 NodeCompleted 消息到 DAGScheduler
 * <p>
 * 设计说明：
 * - 每个 Worker 独立执行任务，互不干扰
 * - 使用 Supervisor 策略处理任务执行失败
 * - 支持任务超时控制（由 NodeDispatcherActor 负责）
 * - 任务执行结果通过 NodeCompleted 消息返回
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class TaskWorkerActor extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(TaskWorkerActor.class);

    public static Props props() {
        return Props.create(TaskWorkerActor.class, TaskWorkerActor::new);
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        logger.info("TaskWorkerActor started: {}", getSelf().path());
    }

    @Override
    public void postStop() throws Exception {
        logger.info("TaskWorkerActor stopped: {}", getSelf().path());
        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TaskExecutionMessage.class, this::handleTaskExecution)
                .matchAny(msg -> logger.warn("Received unknown message: {}", msg))
                .build();
    }

    /**
     * 处理任务执行
     * <p>
     * 实现步骤：
     * 1. 加载任务上下文
     * 2. 执行 DataflowTask.run() 方法
     * 3. 捕获执行结果
     * 4. 发送 NodeCompleted 消息（成功）
     * 5. 捕获异常，发送 NodeCompleted 消息（失败）
     * <p>
     * 实现说明：
     * - 任务执行是同步的，阻塞当前 Worker
     * - 异常会被捕获并转换为 NodeCompleted 失败消息
     * - 执行结果通过 getSender() 返回给 NodeDispatcherActor
     * - 通过 getSender() 直接回复给 WorkflowInstanceActor
     *
     * @param msg 任务执行消息
     */
    private void handleTaskExecution(TaskExecutionMessage msg) throws Exception {
        PEWorkflowDAG.Node node = msg.getNode();
        logger.info("Handling TaskExecution: instanceId={}, nodeId={}",
                msg.getTaskId(), node.getNodeId());


        long startTime = System.currentTimeMillis();

        try {
            // 1. 加载任务（根据 jobId 查找 DataflowTask）
            // 说明：jobId 对应 TIS 中的数据流任务定义
//            Object task = loadTask(msg.getNode().getJobId());
//            if (task == null) {
//                throw new IllegalStateException("Task not found: jobId=" + msg.getNode().getJobId());
//            }

            // 2. 准备任务上下文
            // 说明：包含工作流上下文、节点参数、共享数据等
            logger.info("Preparing task context: instanceId={}, nodeId={}",
                    msg.getTaskId(), node.getNodeId());

            // 3. 执行任务
            // 说明：这里应该调用TIS的DataXJobSubmit来执行实际的DataX任务
            logger.info("Executing task: instanceId={}, nodeId={}",
                    msg.getTaskId(), node.getNodeId());

            // TODO: 实际的任务执行应该调用DataXJobSubmit
            // 这里先做一个占位实现
            executeTaskInternal(msg);

            // 4. 记录执行时间
            long executionTime = System.currentTimeMillis() - startTime;
            logger.info("Task executed successfully: instanceId={}, nodeId={}, executionTime={}ms",
                    msg.getTaskId(), node.getNodeId(), executionTime);

            // 5. 发送成功消息
            // 说明：NodeCompleted 消息会被发送回 WorkflowInstanceActor（通过getSender()）
            NodeCompleted successMsg = new NodeCompleted(
                    msg.getTaskId(),
                    node.getNodeId(),
                    InstanceStatus.SUCCEED,
                    "Task completed in " + executionTime + "ms"
            );

            getSender().tell(successMsg, getSelf());

        } catch (Exception e) {
            // 6. 捕获异常，记录错误日志
            long executionTime = System.currentTimeMillis() - startTime;
            logger.error("Task execution failed: instanceId={}, nodeId={}, executionTime={}ms",
                    msg.getTaskId(), node.getNodeId(), executionTime, e);

            // 7. 发送失败消息
            // 说明：包含错误信息和堆栈，用于故障诊断
            String errorMessage = e.getMessage() + "\n" + extractStackTrace(e);
            NodeCompleted failureMsg = new NodeCompleted(
                    msg.getTaskId(),
                    msg.getNode().getNodeId(),
                    InstanceStatus.FAILED,
                    errorMessage
            );

            getSender().tell(failureMsg, getSelf());
        }
    }

    private static RpcServiceReference statusRpc;

    public static RpcServiceReference getRpcClient() throws Exception {
        if (statusRpc == null) {
            statusRpc = StatusRpcClientFactory.getService(ITISCoordinator.create());
            StatusRpcClientFactory.AssembleSvcCompsite.statusRpc = statusRpc;
        }
        return statusRpc;
    }

    /**
     * 执行任务的内部实现
     *
     * @param msg 任务执行消息
     */
    private void executeTaskInternal(TaskExecutionMessage msg) throws Exception {
        // 这里应该根据节点类型执行不同的任务
        // 对于TIS来说，主要是执行DataX任务
        // msg.getTaskId();

        // 实际实现应该：
        // 1. 根据jobId加载DataX配置
        // 2. 创建DataXJobSubmit实例
        // 3. 执行任务并等待完成
        // 4. 返回执行结果
        PEWorkflowDAG.Node node = msg.getNode();
        logger.info("Executing DataX job: nodeName={}, workflowContext={}",
                node.getNodeName(), msg.getWorkflowContext());
        // RpcServiceReference rpcClient = getRpcClient();
        AbstractExecContext execContext = TaskExecutionMessage.deserializeInstanceParams(
                TriggersConfig.create(msg.getDataXName()) //
                , msg //
                , (ctx) -> {
                } //
                , (snapshot) -> {
                }); // getExecContext(msg);


        // execContext.getTaskId();
        RpcServiceReference rpc = getRpcClient();
        //  IRemoteTaskTrigger taskTrigger = null;
        Integer taskId = msg.getTaskId();
        switch (node.getExecRole()) {
            case Dump: {
                JSONObject params = node.getNodeParams();
                node.checkIfDumpNode();
                Integer allRows = Objects.requireNonNull(params.getInteger(KEY_ALL_ROWS_APPROXIMATELY) //
                        , "key:" + KEY_ALL_ROWS_APPROXIMATELY + " can not be null");
                DataXJobInfo dataXJobInfo = node.getDataXJobInfo();
                try {
                    new SplitTabSync(dataXJobInfo, allRows).execSync(execContext, rpc);
                } catch (Exception e) {
                    BasicTISTableDumpProcessor.reportError(e, taskId, rpc);
                    throw new RuntimeException(e);
                }
                break;
            }
            case Post: {
                try {
                    RpcUtils.setJoinStatus(taskId, false, false, rpc, node.getNodeName());
                    executePostOrPrep(node, execContext);
                    RpcUtils.setJoinStatus(taskId, true, false, rpc, node.getNodeName());
                    break;
                } catch (Exception e) {
                    RpcUtils.setJoinStatus(taskId, true, true, rpc, node.getNodeName());
                    throw new RuntimeException(e);
                }
            }
            case Prep: {
                try {
                    rpc.reportDumpJobStatus(false, false, false, taskId, node.getNodeName(), -1, -1);
                    executePostOrPrep(node, execContext);
                    rpc.reportDumpJobStatus(false, true, false, taskId, node.getNodeName(), -1, -1);
                    break;
                } catch (Exception e) {
                    logger.error("pretrigger:" + node.getNodeName() + " faild", e);
                    reportError(e, taskId, rpc);
                    rpc.reportDumpJobStatus(true, true, false, taskId, node.getNodeName(), -1, -1);
                }
            }
            case Process: {

                break;
            }
            default:
                throw new IllegalStateException("illegal execute Role:" + node.getExecRole());
        }


        // 这里暂时只是一个占位实现
        // 实际应该调用：
        // DataXJobSubmit submit = DataXJobSubmit.getDataXJobSubmit(...);
        // submit.submitJob(...);
        //  Thread.sleep(100); // 模拟任务执行
    }

    private static void executePostOrPrep(PEWorkflowDAG.Node node, AbstractExecContext execContext) {
        String tableName = node.getNodeParam(SelectedTabTriggers.KEY_TABLE);
        IRemoteTaskTrigger taskTrigger
                = BasicTISTableDumpProcessor.createDataXJob(execContext, Pair.of(node.getNodeName(), node.getExecRole()), tableName);
        Objects.requireNonNull(taskTrigger, "taskTrigger can not be null").run();
    }

    /**
     * 获取异常堆栈信息
     *
     * @param e 异常
     * @return 堆栈信息字符串
     */
    private String extractStackTrace(Exception e) {
        try {
            java.io.StringWriter sw = new java.io.StringWriter();
            java.io.PrintWriter pw = new java.io.PrintWriter(sw);
            e.printStackTrace(pw);
            return sw.toString();
        } catch (Exception ex) {
            return "Failed to extract stack trace: " + ex.getMessage();
        }
    }

    /**
     * Supervisor 策略配置
     * 任务执行失败时的处理策略
     * <p>
     * 策略说明：
     * - OneForOneStrategy: 只影响失败的子 Actor，不影响其他子 Actor
     * - 最大重试次数: 3 次
     * - 时间窗口: 1 分钟内最多重试 3 次
     * - Exception: 重启 Actor（restart）
     * - 其他错误: 向上传播（escalate）
     * <p>
     * 注意：
     * - TaskWorkerActor 通常不创建子 Actor，所以这个策略主要用于演示
     * - 实际的任务重试逻辑由 WorkflowInstanceActor 控制
     * - 如果需要更复杂的重试策略，应该在 WorkflowInstanceActor 中实现
     */
    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new OneForOneStrategy(
                3, // 最大重试次数
                Duration.create(1, TimeUnit.MINUTES), // 时间窗口
                DeciderBuilder
                        .match(Exception.class, e -> {
                            logger.warn("Child actor failed with exception, restarting: {}", e.getMessage());
                            return SupervisorStrategy.restart();
                        })
                        .matchAny(o -> {
                            logger.error("Child actor failed with unknown error", o);
                            return SupervisorStrategy.escalate();
                        })
                        .build()
        );
    }
}
