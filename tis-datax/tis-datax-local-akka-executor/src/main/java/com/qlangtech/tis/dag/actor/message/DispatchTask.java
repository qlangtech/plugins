package com.qlangtech.tis.dag.actor.message;

import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.powerjob.model.PEWorkflowDAG;

import java.io.Serializable;
import java.util.Objects;

/**
 * 分发任务消息
 * DAGSchedulerActor 发送给 NodeDispatcherActor，请求分发任务到 Worker
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class DispatchTask implements Serializable {

    private static final long serialVersionUID = 1L;
    private DataXName dataXName;
    /**
     * 工作流实例 ID
     */
    private Integer taskId;
    private AbstractTaskExecutionMessage startWorkflow;
    /**
     * 要执行的节点
     */
    private PEWorkflowDAG.Node node;


    public DispatchTask(PEWorkflowDAG.Node node, AbstractTaskExecutionMessage startWorkflow) {
        this.startWorkflow = Objects.requireNonNull(startWorkflow, "startWorkflow can not be null");
        this.taskId = Objects.requireNonNull(startWorkflow.getTaskId(), "workflowInstanceId can not be null");
        this.dataXName = Objects.requireNonNull(startWorkflow.getDataXName(), "dataName can not be null");
        this.node = Objects.requireNonNull(node, "node can not be null");
    }

    public AbstractTaskExecutionMessage getStartWorkflow() {
        return this.startWorkflow;
    }

    public DataXName getDataXName() {
        return this.dataXName;
    }
// Getters and Setters

    public Integer getTaskId() {
        return this.taskId;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public PEWorkflowDAG.Node getNode() {
        return node;
    }

    public void setNode(PEWorkflowDAG.Node node) {
        this.node = node;
    }

    @Override
    public String toString() {
        return "DispatchTask{" +
                "workflowInstanceId=" + taskId +
                ", node=" + (node != null ? node.getNodeName() : "null") +
                '}';
    }
}
