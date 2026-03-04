package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * Cancel a single task message
 * Broadcast to all TaskWorkerActor routees via the router pool.
 * Each worker checks whether it is executing the matching taskId and interrupts if so.
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-02-20
 */
public class CancelTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer taskId;

    public CancelTask(Integer taskId) {
        this.taskId = taskId;
    }

    public Integer getTaskId() {
        return taskId;
    }

    @Override
    public String toString() {
        return "CancelTask{taskId=" + taskId + '}';
    }
}
