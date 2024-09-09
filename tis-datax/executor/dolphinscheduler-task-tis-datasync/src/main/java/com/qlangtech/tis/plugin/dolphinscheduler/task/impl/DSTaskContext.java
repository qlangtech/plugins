package com.qlangtech.tis.plugin.dolphinscheduler.task.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.qlangtech.tis.cloud.ICoordinator;
import com.qlangtech.tis.datax.DataXJobSubmit.InstanceType;
import com.qlangtech.tis.datax.IDataXTaskRelevant;
import com.qlangtech.tis.datax.executor.ITaskExecutorContext;
import com.qlangtech.tis.plugin.dolphinscheduler.task.TISDatasyncParameters;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 **/
public class DSTaskContext implements ITaskExecutorContext {
    private final TISDatasyncParameters parameters;
    private final TaskExecutionContext taskRequest;
    private final Map<String, String> dagWorkflowParams = Maps.newHashMap();
    private static final Logger logger = LoggerFactory.getLogger(DSTaskContext.class);
    private final File specifiedLocalLoggerPath;

    public DSTaskContext(TISDatasyncParameters parameters, TaskExecutionContext taskRequest) {
        this.parameters = Objects.requireNonNull(parameters, "param parameters can not be null");
        this.taskRequest = Objects.requireNonNull(taskRequest, "param taskRequest can not be null");
        this.specifiedLocalLoggerPath = new File(taskRequest.getLogPath());
        if (!this.specifiedLocalLoggerPath.exists()) {
            throw new IllegalStateException("logger path is not exist:" + this.specifiedLocalLoggerPath.getAbsolutePath());
        }
    }

    /**
     * 找到ServerHome目录
     *
     * @return
     */
    public File getDSServerHome() {

        File loggerPath = null;
        try {
            final Property dataXWorkDir =
                    taskRequest.getPrepareParamsMap().get(IDataXTaskRelevant.KEY_TIS_DATAX_WORK_DIR);
            if (dataXWorkDir != null) {
                File workDir = new File(dataXWorkDir.getValue());
                FileUtils.forceMkdir(workDir);
                return workDir;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        loggerPath = this.getSpecifiedLocalLoggerPath();
        File parent = loggerPath;
        while ((parent = parent.getParentFile()) != null) {
            if ("logs".equalsIgnoreCase(parent.getName())) {
                parent = parent.getParentFile();
                return parent;
            }
        }

        throw new IllegalStateException("can not find logs dir , path:" + loggerPath.getAbsolutePath());
    }

    @Override
    public File getSpecifiedLocalLoggerPath() {
        return this.specifiedLocalLoggerPath;
    }

    @Override
    public InstanceType getJobTriggerType() {
        return InstanceType.DS;
    }

    @Override
    public void appendData2WfContext(String key, Object value) {
        this.dagWorkflowParams.put(key
                , String.valueOf(Objects.requireNonNull(value, "value can not be null")));
    }

    @Override
    public JSONObject getInstanceParams() {
        JSONObject instanceParams = new JSONObject(); //JSONObject.parseObject(StringUtils.defaultString(parameters.getSourceLocationArn(), "{}"));

        parameters.getVarPoolMap().forEach((key, prop) -> {
            instanceParams.put(key, prop.getValue());
        });

        return instanceParams;
    }

    @Override
    public boolean isDisableGrpcRemoteServerConnect() {
        return getJobParams().getBooleanValue(ICoordinator.KEY_DISABLE_GRPC_REMOTE_SERVER_CONNECT);
    }

    @Override
    public JSONObject getJobParams() {
        JSONObject jobParams = JSONObject.parseObject(StringUtils.defaultString(parameters.getSourceLocationArn(), "{}"));
        return jobParams;
    }

    @Override
    public Long getWfInstanceId() {
        return new Long(taskRequest.getProcessInstanceId());
    }

    @Override
    public void infoLog(String var1, Object... var2) {
        logger.info(var1, var2);
    }

    @Override
    public void errorLog(String s, Exception e) {
        logger.error(s, e);
    }

    @Override
    public void finallyCommit() {
        logger.info("send dagWorkflowParams:{}", dagWorkflowParams);
        if (MapUtils.isNotEmpty(dagWorkflowParams)) {
            this.parameters.dealOutParam(dagWorkflowParams);
        }
    }
}
