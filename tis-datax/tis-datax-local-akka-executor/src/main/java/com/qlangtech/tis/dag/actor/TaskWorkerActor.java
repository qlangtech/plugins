package com.qlangtech.tis.dag.actor;

import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.dag.actor.message.CancelTask;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
public class TaskWorkerActor extends AbstractActorWithStash {

    private static final Logger logger = LoggerFactory.getLogger(TaskWorkerActor.class);

    /**
     * Current executing taskId (null when idle)
     */
    private volatile Integer currentTaskId;

    /**
     * Thread running the current task, used for interrupt on cancel
     */
    private volatile Thread executionThread;

    /**
     * Current executing trigger, used for cancel to kill subprocess
     */
    private volatile IRemoteTaskTrigger currentTrigger;

    /**
     * Dedicated thread pool for async task execution, so the Actor thread is not blocked
     * and can still process CancelTask messages.
     */
    private final ExecutorService taskExecutor = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "task-worker-exec"));

    /**
     * Internal message: sent by the async execution thread back to self
     * to signal that the task has finished (success or failure).
     */
    private static class TaskFinished {
        final NodeCompleted result;

        TaskFinished(NodeCompleted result) {
            this.result = result;
        }
    }

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
        taskExecutor.shutdownNow();
        logger.info("TaskWorkerActor stopped: {}", getSelf().path());
        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return idle();
    }

    /**
     * Idle state: accept new tasks and cancel requests.
     */
    private Receive idle() {
        return receiveBuilder()
                .match(TaskExecutionMessage.class, this::handleTaskExecution)
                .match(CancelTask.class, this::handleCancelTask)
                .matchAny(msg -> logger.warn("Received unknown message: {}", msg))
                .build();
    }

    /**
     * Busy state: only process CancelTask and internal TaskFinished messages.
     * Any incoming TaskExecutionMessage is stashed and will be replayed after
     * the current task completes.
     *
     * @param replyTo the ActorRef to send the NodeCompleted result to
     */
    private Receive busy(ActorRef replyTo) {
        return receiveBuilder()
                .match(CancelTask.class, this::handleCancelTask)
                .match(TaskFinished.class, finished -> {
                    this.currentTaskId = null;
                    this.executionThread = null;
                    this.currentTrigger = null;
                    replyTo.tell(finished.result, getSelf());
                    unstashAll();
                    getContext().unbecome();
                })
                .match(TaskExecutionMessage.class, msg -> {
                    logger.warn("Worker busy, stashing task: taskId={}", msg.getTaskId());
                    stash();
                })
                .matchAny(msg -> logger.warn("Received unknown message while busy: {}", msg))
                .build();
    }

    /**
     * Handle cancel task request (broadcast from NodeDispatcherActor via router).
     * If this worker is currently executing a task matching the given taskId,
     * interrupt the execution thread to abort it.
     */
    private void handleCancelTask(CancelTask msg) {
        if (msg.getTaskId().equals(this.currentTaskId) && this.executionThread != null) {
            logger.info("Cancelling running task: taskId={}, worker={}", msg.getTaskId(), getSelf().path());
            IRemoteTaskTrigger trigger = this.currentTrigger;
            if (trigger != null) {
                trigger.cancel();
            }
            this.executionThread.interrupt();
        }
    }

    /**
     * 处理任务执行
     * <p>
     * 将任务提交到独立线程池异步执行，同时切换 Actor 到 busy 状态。
     * busy 状态下 Actor 仍可处理 CancelTask 消息，从而实现运行期取消。
     * 新的 TaskExecutionMessage 会被 stash，待当前任务完成后自动重放。
     *
     * @param msg 任务执行消息
     */
    private void handleTaskExecution(TaskExecutionMessage msg) {
        PEWorkflowDAG.Node node = msg.getNode();
        logger.info("Handling TaskExecution: instanceId={}, nodeId={}",
                msg.getTaskId(), node.getNodeId());

        this.currentTaskId = msg.getTaskId();
        final ActorRef replyTo = getSender();
        final ActorRef self = getSelf();
        long startTime = System.currentTimeMillis();

        // Switch to busy behavior so CancelTask can be processed
        getContext().become(busy(replyTo));

        taskExecutor.submit(() -> {
            this.executionThread = Thread.currentThread();
            try {
                executeTaskInternal(msg);

                long executionTime = System.currentTimeMillis() - startTime;
                logger.info("Task executed successfully: instanceId={}, nodeId={}, executionTime={}ms",
                        msg.getTaskId(), node.getNodeId(), executionTime);

                NodeCompleted successMsg = new NodeCompleted(
                        msg.getTaskId(),
                        node.getNodeId(),
                        InstanceStatus.SUCCEED,
                        "Task completed in " + executionTime + "ms"
                );
                self.tell(new TaskFinished(successMsg), ActorRef.noSender());
            } catch (Exception e) {
                long executionTime = System.currentTimeMillis() - startTime;
                logger.error("Task execution failed: instanceId={}, nodeId={}, executionTime={}ms",
                        msg.getTaskId(), node.getNodeId(), executionTime, e);

                String errorMessage = e.getMessage() + "\n" + extractStackTrace(e);
                NodeCompleted failureMsg = new NodeCompleted(
                        msg.getTaskId(),
                        node.getNodeId(),
                        InstanceStatus.FAILED,
                        errorMessage
                );
                self.tell(new TaskFinished(failureMsg), ActorRef.noSender());
            }
        });
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
        PEWorkflowDAG.Node node = msg.getNode();
        logger.info("Executing DataX job: nodeName={}, workflowContext={}",
                node.getNodeName(), msg.getWorkflowContext());
        AbstractExecContext execContext = TaskExecutionMessage.deserializeInstanceParams(
                TriggersConfig.create(msg.getDataXName()) //
                , msg //
                , (ctx) -> {
                } //
                , (snapshot) -> {
                });

        RpcServiceReference rpc = getRpcClient();
        Integer taskId = msg.getTaskId();
        switch (node.getExecRole()) {
            case Dump: {
                JSONObject params = node.getNodeParams();
                node.checkIfDumpNode();
                Integer allRows = Objects.requireNonNull(params.getInteger(KEY_ALL_ROWS_APPROXIMATELY) //
                        , "key:" + KEY_ALL_ROWS_APPROXIMATELY + " can not be null");
                DataXJobInfo dataXJobInfo = node.getDataXJobInfo();
                try {
                    SplitTabSync splitTabSync = new SplitTabSync(dataXJobInfo, allRows);
                    IRemoteTaskTrigger tskTrigger = splitTabSync.createTrigger(execContext, rpc);
                    this.currentTrigger = tskTrigger;
                    tskTrigger.run();
                } catch (Exception e) {
                    BasicTISTableDumpProcessor.reportError(e, taskId, rpc);
                    throw new RuntimeException(e);
                } finally {
                    this.currentTrigger = null;
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
    }

    private void executePostOrPrep(PEWorkflowDAG.Node node, AbstractExecContext execContext) {
        String tableName = node.getNodeParam(SelectedTabTriggers.KEY_TABLE);
        IRemoteTaskTrigger taskTrigger
                = BasicTISTableDumpProcessor.createDataXJob(execContext, Pair.of(node.getNodeName(), node.getExecRole()), tableName);
        Objects.requireNonNull(taskTrigger, "taskTrigger can not be null");
        this.currentTrigger = taskTrigger;
        try {
            taskTrigger.run();
        } finally {
            this.currentTrigger = null;
        }
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