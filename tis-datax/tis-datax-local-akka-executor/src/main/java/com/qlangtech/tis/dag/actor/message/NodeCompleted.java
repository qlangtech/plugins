package com.qlangtech.tis.dag.actor.message;

import com.qlangtech.tis.powerjob.model.InstanceStatus;

import java.io.Serializable;

/**
 * 节点完成消息
 * TaskWorkerActor 执行完成后通过 getSender() 直接发送给 WorkflowInstanceActor
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class NodeCompleted implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 工作流实例 ID
     */
    private Integer workflowInstanceId;

    /**
     * 节点 ID
     */
    private Long nodeId;

    /**
     * 节点状态：SUCCEED/FAILED
     */
    private InstanceStatus status;

    /**
     * 执行结果
     */
    private String result;

    public NodeCompleted() {
    }

    public NodeCompleted(Integer workflowInstanceId, Long nodeId, InstanceStatus status) {
        this.workflowInstanceId = workflowInstanceId;
        this.nodeId = nodeId;
        this.status = status;
    }

    public NodeCompleted(Integer workflowInstanceId, Long nodeId, InstanceStatus status, String result) {
        this.workflowInstanceId = workflowInstanceId;
        this.nodeId = nodeId;
        this.status = status;
        this.result = result;
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

    public InstanceStatus getStatus() {
        return status;
    }

    public void setStatus(InstanceStatus status) {
        this.status = status;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "NodeCompleted{" +
                "workflowInstanceId=" + workflowInstanceId +
                ", nodeId=" + nodeId +
                ", status='" + status + '\'' +
                ", result='" + result + '\'' +
                '}';
    }
}
