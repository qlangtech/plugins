package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * 查询工作流状态消息
 * 发送给 DAGMonitorActor，查询工作流实例的执行状态
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class QueryWorkflowStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 工作流实例 ID
     */
    private Integer workflowInstanceId;

    public QueryWorkflowStatus() {
    }

    public QueryWorkflowStatus(Integer workflowInstanceId) {
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
        return "QueryWorkflowStatus{" +
                "workflowInstanceId=" + workflowInstanceId +
                '}';
    }
}
