package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * 取消工作流消息
 * 用户请求取消工作流时发送给 WorkflowInstance Sharding Region
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class CancelWorkflow implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 工作流实例 ID
     */
    private Integer workflowInstanceId;

    /**
     * 取消原因
     */
    private String reason;

    public CancelWorkflow() {
    }

    public CancelWorkflow(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public CancelWorkflow(Integer workflowInstanceId, String reason) {
        this.workflowInstanceId = workflowInstanceId;
        this.reason = reason;
    }

    // Getters and Setters

    public Integer getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "CancelWorkflow{" +
                "workflowInstanceId=" + workflowInstanceId +
                ", reason='" + reason + '\'' +
                '}';
    }
}
