package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 更新工作流上下文消息
 * 节点执行完成后可以更新工作流上下文，供后续节点使用
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class UpdateContext implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 工作流实例 ID
     */
    private Integer workflowInstanceId;

    /**
     * 上下文数据
     */
    private Map<String, String> contextData;

    public UpdateContext() {
        this.contextData = new HashMap<>();
    }

    public UpdateContext(Integer workflowInstanceId, Map<String, String> contextData) {
        this.workflowInstanceId = workflowInstanceId;
        this.contextData = contextData != null ? contextData : new HashMap<>();
    }

    // Getters and Setters

    public Integer getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public Map<String, String> getContextData() {
        return contextData;
    }

    public void setContextData(Map<String, String> contextData) {
        this.contextData = contextData;
    }

    @Override
    public String toString() {
        return "UpdateContext{" +
                "workflowInstanceId=" + workflowInstanceId +
                ", contextData=" + contextData +
                '}';
    }
}
