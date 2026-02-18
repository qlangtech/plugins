package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * 节点超时消息
 * 节点执行超时后发送给 WorkflowInstanceActor
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class NodeTimeout implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 工作流实例 ID
     */
    private Integer workflowInstanceId;

    /**
     * 节点 ID
     */
    private Long nodeId;

    public NodeTimeout() {
    }

    public NodeTimeout(Integer workflowInstanceId, Long nodeId) {
        this.workflowInstanceId = workflowInstanceId;
        this.nodeId = nodeId;
    }

    // Getters and Setters

    public Integer getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public Long getNodeId() {
        return nodeId;
    }

    public void setNodeId(Long nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String toString() {
        return "NodeTimeout{" +
                "workflowInstanceId=" + workflowInstanceId +
                ", nodeId=" + nodeId +
                '}';
    }
}
