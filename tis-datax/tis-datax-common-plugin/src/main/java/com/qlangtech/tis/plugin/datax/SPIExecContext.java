package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fullbuild.indexbuild.RemoteTaskTriggers;
import com.qlangtech.tis.fullbuild.phasestatus.IPhaseStatusCollection;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.order.center.IAppSourcePipelineController;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/8
 */
public class SPIExecContext implements IExecChainContext {

    private Integer workflowId;
    private ExecutePhaseRange executePhaseRange;
    private String appname;

    @Override
    public String getJavaMemSpec() {
        return null;
    }

    public void setExecutePhaseRange(ExecutePhaseRange executePhaseRange) {
        this.executePhaseRange = executePhaseRange;
    }

    public void setAppname(String appname) {
        this.appname = appname;
    }

    public void setWorkflowId(Integer workflowId) {
        this.workflowId = workflowId;
    }

    @Override
    public IDataxProcessor getProcessor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addAsynSubJob(AsynSubJob jobName) {

    }

    @Override
    public List<AsynSubJob> getAsynSubJobs() {
        return null;
    }

    @Override
    public boolean containAsynJob() {
        return false;
    }

    private RemoteTaskTriggers tskTriggers;

    @Override
    public void setTskTriggers(RemoteTaskTriggers tskTriggers) {
        this.tskTriggers = tskTriggers;
    }

    @Override
    public RemoteTaskTriggers getTskTriggers() {
        return Objects.requireNonNull(this.tskTriggers, "tskTriggers can not be null");
    }

    @Override
    public void cancelTask() {

    }

    @Override
    public ITISCoordinator getZkClient() {
        return null;
    }

    @Override
    public Integer getWorkflowId() {
        return this.workflowId;
    }

    @Override
    public String getWorkflowName() {
        return null;
    }

    @Override
    public ITISFileSystem getIndexBuildFileSystem() {
        return null;
    }

    @Override
    public void rebindLoggingMDCParams() {

    }

    @Override
    public boolean isDryRun() {
        return false;
    }

    @Override
    public int getIndexShardCount() {
        return 0;
    }

    private final Map<String, Object> attribute = new HashMap<>();

    @Override
    public <T> T getAttribute(String key) {
        return (T) this.attribute.get(key);
    }

    @Override
    public <T> T getAttribute(String key, Supplier<T> creator) {
        synchronized (attribute) {
            T attr = getAttribute(key);
            if (attr == null) {
                attr = creator.get();
                this.setAttribute(key, attr);
            }
            return attr;
        }
    }

    //  private Map<String, Object> attrs = new HashMap<>();

    @Override
    public void setAttribute(String key, Object v) {
        this.attribute.put(key, v);
    }

    @Override
    public IAppSourcePipelineController getPipelineController() {
        return null;
    }

    @Override
    public int getTaskId() {
        Integer taskId = Objects.requireNonNull(getAttribute(JobCommon.KEY_TASK_ID), JobCommon.KEY_TASK_ID + " can not be null");
        return taskId;
    }

    @Override
    public String getIndexName() {
        return this.appname;
    }

    @Override
    public boolean hasIndexName() {
        return StringUtils.isNotEmpty(this.appname);
    }

    @Override
    public ExecutePhaseRange getExecutePhaseRange() {
        return this.executePhaseRange;
    }

    @Override
    public String getString(String key) {
        return null;
    }

    @Override
    public boolean getBoolean(String key) {
        return false;
    }

    @Override
    public int getInt(String key) {
        return 0;
    }

    @Override
    public long getLong(String key) {
        return 0;
    }

    @Override
    public long getPartitionTimestampWithMillis() {
        return 0;
    }

    @Override
    public <T extends IPhaseStatusCollection> T loadPhaseStatusFromLatest() {
        return null;
    }
}
