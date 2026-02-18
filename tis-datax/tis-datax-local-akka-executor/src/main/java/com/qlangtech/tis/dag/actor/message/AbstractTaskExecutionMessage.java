package com.qlangtech.tis.dag.actor.message;

import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.fullbuild.IFullBuildContext;

import java.io.Serializable;
import java.util.Objects;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/2/10
 * @see TaskExecutionMessage
 */
public abstract class AbstractTaskExecutionMessage implements Serializable {

    private String pluginCfgsMetas;
    private Long triggerTimestamp;
    /**
     * 工作流实例 ID
     */
    private Integer taskId;
    /**
     * @see IFullBuildContext#DRY_RUN
     */
    private boolean dryRun;
    private DataXName dataXName;

    public AbstractTaskExecutionMessage(Integer taskId, DataXName dataXName) {
        this.taskId = Objects.requireNonNull(taskId, "taskId can not be null");
        this.dataXName = Objects.requireNonNull(dataXName, "dataXName can not be null");
    }

    public Long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    public void setTriggerTimestamp(Long triggerTimestamp) {
        this.triggerTimestamp = triggerTimestamp;
    }

    /**
     * @return
     * @see IFullBuildContext#DRY_RUN
     */
    public boolean isDryRun() {
        return this.dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    // Getters and Setters
    public Integer getTaskId() {
        return this.taskId;
    }

    public DataXName getDataXName() {
        return this.dataXName;
    }
    
    public String getPluginCfgsMetas() {
        return this.pluginCfgsMetas;
    }

    public void setPluginCfgsMetas(String pluginCfgsMetas) {
        this.pluginCfgsMetas = pluginCfgsMetas;
    }
}
