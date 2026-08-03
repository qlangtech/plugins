package com.qlangtech.tis.dag.actor.message;

import java.io.Serializable;

/**
 * 查询工作流实例的队列状态
 * 用于从 WorkflowInstanceActor 获取等待队列和运行队列信息
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-07-28
 */
public class QueryQueueStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Integer workflowInstanceId;

    public QueryQueueStatus(Integer workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public Integer getWorkflowInstanceId() {
        return workflowInstanceId;
    }
}
