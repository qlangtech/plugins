package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * 查询等待队列消息
 * 发送给 DAGMonitorActor，查询所有等待执行的节点
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class QueryWaitingQueue implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 工作流实例 ID（可选，为 null 则查询所有）
     */
    private Integer workflowInstanceId;

    public QueryWaitingQueue() {
    }

    public QueryWaitingQueue(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    // Getters and Setters

    public Integer getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    @Override
    public String toString() {
        return "QueryWaitingQueue{" +
                "workflowInstanceId=" + workflowInstanceId +
                '}';
    }
}
