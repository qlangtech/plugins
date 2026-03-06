package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * TaskWorkerActor sends this message back to NodeDispatcherActor immediately
 * upon receiving a task, reporting the actual cluster address of the worker node.
 * This allows the dispatcher to track which node each task is really running on.
 *
 * @author baisui(baisui@qlangtech.com)
 * @date 2026-03-06
 */
public class TaskStarted implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer taskId;
    private final Long nodeId;
    private final String workerAddress;

    public TaskStarted(Integer taskId, Long nodeId, String workerAddress) {
        this.taskId = taskId;
        this.nodeId = nodeId;
        this.workerAddress = workerAddress;
    }

    public Integer getTaskId() {
        return taskId;
    }

    public Long getNodeId() {
        return nodeId;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }

    @Override
    public String toString() {
        return "TaskStarted{taskId=" + taskId + ", nodeId=" + nodeId + ", workerAddress='" + workerAddress + "'}";
    }
}