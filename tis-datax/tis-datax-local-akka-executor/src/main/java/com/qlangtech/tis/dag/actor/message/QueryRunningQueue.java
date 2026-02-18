package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * 查询执行队列消息
 * 发送给 DAGMonitorActor，查询所有正在执行的节点
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class QueryRunningQueue implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 工作流实例 ID（可选，为 null 则查询所有）
     */
    private Integer workflowInstanceId;

    /**
     * Worker 地址（可选，为 null 则查询所有）
     */
    private String workerAddress;

    public QueryRunningQueue() {
    }

    public QueryRunningQueue(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public QueryRunningQueue(Integer workflowInstanceId, String workerAddress) {
        this.workflowInstanceId = workflowInstanceId;
        this.workerAddress = workerAddress;
    }

    // Getters and Setters

    public Integer getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }

    public void setWorkerAddress(String workerAddress) {
        this.workerAddress = workerAddress;
    }

    @Override
    public String toString() {
        return "QueryRunningQueue{" +
                "workflowInstanceId=" + workflowInstanceId +
                ", workerAddress='" + workerAddress + '\'' +
                '}';
    }
}
