package com.qlangtech.tis.dag.actor.message;

import com.google.common.collect.Maps;
import com.qlangtech.tis.datax.DataXName;

import java.util.Map;
import java.util.Objects;

/**
 * 启动工作流消息
 * 发送给 WorkflowInstance Sharding Region，触发工作流执行
 *
 * @author 百岁(baisui@qlangtech.com)
 * @date 2026-01-29
 */
public class StartWorkflow extends AbstractTaskExecutionMessage {

    private static final long serialVersionUID = 1L;

    /**
     * 上一个taskId，通过它获得表记录总数
     */
    private Integer preTaskId;

    /**
     * 初始化参数
     */
    private Map<String, String> initParams;

    /**
     * 最大并发任务数
     * 用于控制同时执行的任务数量，避免对数据库造成过大压力
     * 如果不设置，使用默认值5
     */
    private Integer maxConcurrentTasks;


    public StartWorkflow(Integer taskId, DataXName dataXName) {
        this(taskId, dataXName, Maps.newHashMap());
    }

    public StartWorkflow(Integer taskId, DataXName dataXName, Map<String, String> initParams) {
        super(taskId, dataXName);
        this.initParams = Objects.requireNonNull(initParams, "initParams can not be null");
    }

    // Getters and Setters


    public Integer getPreTaskId() {
        return this.preTaskId;
    }

    public void setPreTaskId(Integer preTaskId) {
        this.preTaskId = preTaskId;
    }

    public Map<String, String> getInitParams() {
        return initParams;
    }

    public void setInitParams(Map<String, String> initParams) {
        this.initParams = initParams;
    }

    public Integer getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    public void setMaxConcurrentTasks(Integer maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    @Override
    public String toString() {
        return "{" + "taskId=" + this.getTaskId() + ", initParams=" + initParams +
                ", maxConcurrentTasks=" + maxConcurrentTasks + '}';
    }
}
