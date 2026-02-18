package com.qlangtech.tis.dag.actor.message;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.DataXJobSubmitParams;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.exec.impl.DataXPipelineExecContext;
import com.qlangtech.tis.exec.impl.WorkflowExecContext;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshot;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshotUtils;
import com.qlangtech.tis.powerjob.TriggersConfig;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static com.qlangtech.tis.exec.AbstractExecContext.resolveCfgsSnapshotConsumer;

/**
 * 任务执行消息
 * NodeDispatcherActor 发送给 TaskWorkerActor，请求执行具体任务
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class TaskExecutionMessage extends AbstractTaskExecutionMessage {

    private static final long serialVersionUID = 1L;

    public static AbstractExecContext deserializeInstanceParams(TriggersConfig triggerCfg, TaskExecutionMessage instanceParams,
                                                                Consumer<AbstractExecContext> execChainContextConsumer, Consumer<PluginAndCfgsSnapshot> cfgsSnapshotConsumer) {
        return TaskExecutionMessage.deserializeInstanceParams(triggerCfg, instanceParams, true,
                execChainContextConsumer, cfgsSnapshotConsumer);
    }

    public static AbstractExecContext deserializeInstanceParams(TriggersConfig triggerCfg, TaskExecutionMessage instanceParams) {
        return TaskExecutionMessage.deserializeInstanceParams(triggerCfg, instanceParams, false, (execChainContext) -> {
        }, (snapshot) -> {
            throw new UnsupportedOperationException("shall not be execute");
        });
    }

    /**
     * 反序列化
     *
     * @param taskExec
     * @return
     */
    static AbstractExecContext deserializeInstanceParams(TriggersConfig triggerCfg, TaskExecutionMessage taskExec,
                                                         boolean resolveCfgsSnapshotConsumer //
            , Consumer<AbstractExecContext> execChainContextConsumer,
                                                         Consumer<PluginAndCfgsSnapshot> cfgsSnapshotConsumer) {
        Integer taskId = taskExec.getTaskId();
//        Objects.requireNonNull(taskExec.getInteger(JobParams.KEY_TASK_ID),
//                JobParams.KEY_TASK_ID + " can not be null," + JsonUtil.toString(taskExec));
        boolean dryRun = taskExec.isDryRun(); // taskExec.getBooleanValue(IFullBuildContext.DRY_RUN);
        DataXJobSubmitParams submitParams = DataXJobSubmitParams.getDftIfEmpty();
        final String javaMemSpec = submitParams.getJavaMemorySpec();// taskExec.getString(JobParams.KEY_JAVA_MEMORY_SPEC);

        Long triggerTimestamp = Objects.requireNonNull(taskExec.getTriggerTimestamp(), "triggerTimestamp can not be null");

        // taskExec.getLong(DataxUtils.EXEC_TIMESTAMP);


        AbstractExecContext execChainContext = null;
        switch (triggerCfg.getResType()) {
            case DataFlow:
                WorkflowExecContext wfContext = new WorkflowExecContext(0, triggerTimestamp);
                wfContext.setWorkflowName(triggerCfg.getDataXName());
                execChainContext = wfContext;
                break;
            case DataApp:
                DataXName appName = taskExec.getDataXName();  //taskExec.getString(JobParams.KEY_COLLECTION);
                execChainContext = new DataXPipelineExecContext(appName.getPipelineName(), triggerTimestamp);
                break;
            default:
                throw new IllegalStateException("illegal resType:" + triggerCfg.getResType());
        }


        execChainContext.setJavaMemSpec(javaMemSpec);
        execChainContext.setCoordinator(ITISCoordinator.create());
        execChainContext.setDryRun(dryRun);
        execChainContext.setAttribute(JobCommon.KEY_TASK_ID, taskId);

        execChainContextConsumer.accept(execChainContext);


        // execChainContext.setLatestPhaseStatusCollection( );

        if (resolveCfgsSnapshotConsumer) {

            JSONObject instanceParams = new JSONObject();
            instanceParams.put(PluginAndCfgsSnapshotUtils.KEY_PLUGIN_CFGS_METAS, taskExec.getPluginCfgsMetas());
            instanceParams.put(JobParams.KEY_COLLECTION, execChainContext.getDataXName().getPipelineName());

            cfgsSnapshotConsumer.accept(resolveCfgsSnapshotConsumer(triggerCfg.getResType(), instanceParams));
        }

        return execChainContext;
    }


    /**
     * 要执行的节点
     */
    private PEWorkflowDAG.Node node;


    /**
     * 工作流上下文
     */
    private Map<String, String> workflowContext;

    /**
     * 任务超时时间（毫秒）
     */
    private Long timeoutMillis;

//    public TaskExecutionMessage(Integer taskId, PEWorkflowDAG.Node node, DataXName dataXName) {
//        super(taskId, dataXName);
//        this.node = node;
//        this.workflowContext = new HashMap<>();
//    }

    /**
     *
     * @param taskId
     * @param node
     * @param workflowContext
     * @param dataXName
     */
    public TaskExecutionMessage(Integer taskId, PEWorkflowDAG.Node node,
                                Map<String, String> workflowContext, DataXName dataXName) {
        super(taskId, dataXName);
        this.node = node;
        this.workflowContext = workflowContext != null ? workflowContext : new HashMap<>();
    }

    public PEWorkflowDAG.Node getNode() {
        return node;
    }

    public void setNode(PEWorkflowDAG.Node node) {
        this.node = node;
    }

    public Map<String, String> getWorkflowContext() {
        return workflowContext;
    }

    public void setWorkflowContext(Map<String, String> workflowContext) {
        this.workflowContext = workflowContext;
    }

    public Long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(Long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public String toString() {
        return "TaskExecutionMessage{" +
                "workflowInstanceId=" + this.getTaskId() +
                ", node=" + (node != null ? node.getNodeName() : "null") +
                ", timeoutMillis=" + timeoutMillis +
                '}';
    }
}
