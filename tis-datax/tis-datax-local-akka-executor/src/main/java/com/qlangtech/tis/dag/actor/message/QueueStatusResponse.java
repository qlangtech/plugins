package com.qlangtech.tis.dag.actor.message;

import com.qlangtech.tis.datax.ActorSystemStatus;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 队列状态响应消息
 * WorkflowInstanceActor 返回的队列状态数据
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-07-28
 */
public class QueueStatusResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer workflowInstanceId;
    private List<ActorSystemStatus.QueuedTask> waitingQueue = new ArrayList<>();
    private List<ActorSystemStatus.RunningTask> runningTasks = new ArrayList<>();
    private int maxConcurrentTasks;

    public Integer getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public List<ActorSystemStatus.QueuedTask> getWaitingQueue() {
        return waitingQueue;
    }

    public void setWaitingQueue(List<ActorSystemStatus.QueuedTask> waitingQueue) {
        this.waitingQueue = waitingQueue;
    }

    public List<ActorSystemStatus.RunningTask> getRunningTasks() {
        return runningTasks;
    }

    public void setRunningTasks(List<ActorSystemStatus.RunningTask> runningTasks) {
        this.runningTasks = runningTasks;
    }

    public int getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    public void setMaxConcurrentTasks(int maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }
}
