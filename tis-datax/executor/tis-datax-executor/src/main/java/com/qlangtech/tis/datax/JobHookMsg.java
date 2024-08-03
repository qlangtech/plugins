package com.qlangtech.tis.datax;

import com.qlangtech.tis.plugin.StoreResourceType;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/7
 */
public class JobHookMsg implements IDataXTaskRelevant {

    // private IDataXBatchPost.LifeCycleHook lifeCycleHook;
    //private String tableName;
    private StoreResourceType resType;
    private String resName;
    private String jobName;
    private Integer taskId;
    private Long execEpochMilli;
    private Boolean dryRun;

    public JobHookMsg() {
    }

    /**
     * @param resName          可以是DataXName 或者 workflowName
     * @param resType
     * @param taskId
     * @param jobName
     * @param currentTimeStamp
     * @param dryRun           是否执行具体逻辑
     * @param creator
     * @param <T>
     * @return
     */
    protected static <T extends JobHookMsg> T createHookMsg( //
                                                             String resName, StoreResourceType resType,
                                                             Integer taskId, String jobName, Long currentTimeStamp,
                                                             Boolean dryRun, Supplier<T> creator) {
        if (StringUtils.isEmpty(resName)) {
            throw new IllegalArgumentException("dataXName can not be null");
        }
        //        if (StringUtils.isEmpty(tableName)) {
        //            throw new IllegalArgumentException("tableName can not be null");
        //        }
        if (StringUtils.isEmpty(jobName)) {
            throw new IllegalArgumentException("jobName can not be null");
        }

        T msg = creator.get();// new DataXLifecycleHookMsg();
        // lifecycleHookMsg.setTableName(tableName);
        // lifecycleHookMsg.setLifeCycleHook(lifeCycleHook);
        msg.setDryRun(dryRun);
        msg.setResType(Objects.requireNonNull(resType, "resType can not be null"));
        msg.setTaskId(Objects.requireNonNull(taskId, "taskId can not be null"));
        msg.setResName(resName);
        msg.setExecEpochMilli(Objects.requireNonNull(currentTimeStamp, "currentTimeStamp can not be " + "null"));
        msg.setJobName(jobName);
        return msg;
    }

    public Boolean getDryRun() {
        return dryRun;
    }

    public void setDryRun(Boolean dryRun) {
        this.dryRun = dryRun;
    }

    public void setResName(String resName) {
        this.resName = resName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setTaskId(Integer taskId) {
        this.taskId = taskId;
    }

    public void setExecEpochMilli(Long execEpochMilli) {
        this.execEpochMilli = execEpochMilli;
    }

    //    public String getTableName() {
    //        return this.tableName;
    //    }

    public StoreResourceType getResType() {
        return resType;
    }

    public void setResType(StoreResourceType resType) {
        this.resType = resType;
    }

    //    public void setTableName(String tableName) {
    //        this.tableName = tableName;
    //    }

    //    public IDataXBatchPost.LifeCycleHook getLifeCycleHook() {
    //        return Objects.requireNonNull(this.lifeCycleHook, "lifeCycleHook can not be null");
    //    }
    //
    //    public void setLifeCycleHook(IDataXBatchPost.LifeCycleHook lifeCycleHook) {
    //        this.lifeCycleHook = lifeCycleHook;
    //    }

    @Override
    public Integer getTaskId() {
        return Objects.requireNonNull(this.taskId, "taskid can not be null");
    }

    @Override
    public String getJobName() {
        return this.jobName;
    }

    @Override
    public String getDataXName() {
        return this.resName;
    }

    @Override
    public long getExecEpochMilli() {
        //  throw new UnsupportedOperationException();
        return Objects.requireNonNull(this.execEpochMilli, "execEpochMilli can not be null");
    }

    @Override
    public <T> void setAttr(Class<T> key, Object val) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getAttr(Class<T> key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTaskSerializeNum() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getFormatTime(TimeFormat format) {
        throw new UnsupportedOperationException();
    }
}