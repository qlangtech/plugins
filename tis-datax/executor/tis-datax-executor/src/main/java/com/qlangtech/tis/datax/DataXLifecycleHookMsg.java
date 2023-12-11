package com.qlangtech.tis.datax;

import com.qlangtech.tis.plugin.StoreResourceType;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/18
 */
public class DataXLifecycleHookMsg extends JobHookMsg {

    private IDataXBatchPost.LifeCycleHook lifeCycleHook;
    private String tableName;

    public DataXLifecycleHookMsg() {
    }


    public static DataXLifecycleHookMsg createDataXLifecycleHookMsg(IDataxProcessor processor, String tableName,
                                                                    Integer taskId, String jobName,
                                                                    Long currentTimeStamp,
                                                                    IDataXBatchPost.LifeCycleHook lifeCycleHook,
                                                                    Boolean dryRun) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("tableName can not be null");
        }

        return createHookMsg(processor.identityValue(), processor.getResType(), taskId, jobName, currentTimeStamp, dryRun,
                () -> {
            DataXLifecycleHookMsg hookMsg = new DataXLifecycleHookMsg();
            hookMsg.setTableName(tableName);
            hookMsg.setLifeCycleHook(Objects.requireNonNull(lifeCycleHook, "param lifeCycleHook"));
            return hookMsg;
        });
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public IDataXBatchPost.LifeCycleHook getLifeCycleHook() {
        return Objects.requireNonNull(this.lifeCycleHook, "lifeCycleHook can not be null");
    }

    public void setLifeCycleHook(IDataXBatchPost.LifeCycleHook lifeCycleHook) {
        this.lifeCycleHook = lifeCycleHook;
    }


}
