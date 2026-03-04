package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

/**
 * Cancel active tasks message
 * Sent from WorkflowInstanceActor to NodeDispatcherActor to cancel all running workers for a given taskId
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-02-20
 */
public class CancelTasks implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer taskId;
    private final Set<Long> runningNodeIds;

    public CancelTasks(Integer taskId, Set<Long> runningNodeIds) {
        this.taskId = Objects.requireNonNull(taskId, "taskId can not be null");
        this.runningNodeIds = Objects.requireNonNull(runningNodeIds, "runningNodeIds can not be null");
    }

    public Integer getTaskId() {
        return taskId;
    }

    public Set<Long> getRunningNodeIds() {
        return runningNodeIds;
    }

    @Override
    public String toString() {
        return "CancelTasks{taskId=" + taskId + ", runningNodeIds=" + runningNodeIds + '}';
    }
}