package com.qlangtech.tis.dag.actor.message;

import akka.cluster.sharding.ShardRegion;

/**
 * Workflow Instance 消息提取器
 * 用于Akka Cluster Sharding，提取消息中的workflowInstanceId作为路由键
 *
 * 核心功能：
 * 1. 提取entityId：用于确定消息应该路由到哪个Actor实例
 * 2. 提取shardId：用于确定Actor实例应该分配到哪个分片
 *
 * 路由策略：
 * - entityId = workflowInstanceId（确保同一workflow的所有消息路由到同一个Actor）
 * - shardId = workflowInstanceId % 分片数（均匀分布到各个分片）
 *
 * 支持的消息类型：
 * - StartWorkflow
 * - NodeCompleted
 * - UpdateContext
 * - CancelWorkflow
 * - QueryWorkflowStatus
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-02-01
 */
public class WorkflowInstanceMessageExtractor extends ShardRegion.HashCodeMessageExtractor {

    /**
     * 分片数量
     * 建议设置为：节点数 * 10
     * 例如：3个节点 -> 30个分片，便于动态扩缩容
     */
    private static final int NUMBER_OF_SHARDS = 30;

    public WorkflowInstanceMessageExtractor() {
        super(NUMBER_OF_SHARDS);
    }

    /**
     * 提取entityId（Actor实例的唯一标识）
     * 同一个workflowInstanceId的所有消息都会路由到同一个Actor实例
     */
    @Override
    public String entityId(Object message) {
        if (message instanceof StartWorkflow) {
            return String.valueOf(((StartWorkflow) message).getTaskId());
        } else if (message instanceof NodeCompleted) {
            return String.valueOf(((NodeCompleted) message).getWorkflowInstanceId());
        } else if (message instanceof UpdateContext) {
            return String.valueOf(((UpdateContext) message).getWorkflowInstanceId());
        } else if (message instanceof CancelWorkflow) {
            return String.valueOf(((CancelWorkflow) message).getWorkflowInstanceId());
        } else if (message instanceof QueryWorkflowStatus) {
            return String.valueOf(((QueryWorkflowStatus) message).getWorkflowInstanceId());
        }
        return null;
    }

    /**
     * 提取消息本身（传递给Actor）
     * 直接返回原消息
     */
    @Override
    public Object entityMessage(Object message) {
        return message;
    }
}
