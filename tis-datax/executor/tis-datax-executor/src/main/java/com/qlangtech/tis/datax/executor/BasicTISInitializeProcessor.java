/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.datax.executor;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.coredefine.module.action.PowerjobTriggerBuildResult;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.manage.common.HttpUtils.PostParam;
import com.qlangtech.tis.rpc.grpc.log.ILoggerAppenderClient.LogLevel;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.qlangtech.tis.datax.executor.BasicTISTableDumpProcessor.getRpcServiceReference;
import static com.qlangtech.tis.fullbuild.IFullBuildContext.KEY_WORKFLOW_ID;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-18 17:59
 **/
public class BasicTISInitializeProcessor {
    private static final Logger logger = LoggerFactory.getLogger(BasicTISInitializeProcessor.class);

    public static Integer parseTaskId(JSONObject instanceParams) {
        Integer taskId = Objects.requireNonNull(instanceParams.getInteger(JobParams.KEY_TASK_ID),
                JobParams.KEY_TASK_ID + " can not be null," + JsonUtil.toString(instanceParams));
        return taskId;
    }

    /**
     * 初始化DAG工作流
     *
     * @param context
     * @throws Exception
     */
    public void initializeProcess(final ITaskExecutorContext context) throws Exception {
        try {


            Pair<Boolean, JSONObject> instanceParams = BasicTISTableDumpProcessor.getInstanceParams(context);
            RpcServiceReference svc = getRpcServiceReference();
            if (!instanceParams.getLeft()) {
                // 说明是定时任务触发
                context.infoLog("trigger by crontab,now shall create taskId");
                InitializeNodeCfg initNodeCfg = InitializeNodeCfg.parse(context);

                IExecChainContext.TriggerNewTaskParam triggerParams =
                        new IExecChainContext.TriggerNewTaskParam(context.getWfInstanceId(), context.getJobTriggerType()
                                , initNodeCfg.getDataXName(), initNodeCfg.resourceType) {
                            @Override
                            public List<PostParam> params() {
                                List<HttpUtils.PostParam> params = super.params();
                                if (initNodeCfg.isTisDataflowType()) {
                                    // 为了满足 OfflineDatasourceAction.doExecuteWorkflow 执行
                                    params.add(new HttpUtils.PostParam("id", initNodeCfg.getWorkflowId()));
                                    params.add(new HttpUtils.PostParam(IFullBuildContext.DRY_RUN, false));
                                }
                                logger.info("triggerParams:{}", params.stream().map((param) -> {
                                    return param.getKey() + "->" + param.getValue();
                                }).collect(Collectors.joining(",")));
                                return params;
                            }
                        };
                /**=======================================================================
                 *TriggerNewTask
                 =======================================================================*/
                PowerjobTriggerBuildResult triggerResult = IExecChainContext.triggerNewTask(triggerParams);
                svc.appendLog(LogLevel.INFO, triggerResult.getTaskid()
                        , Optional.empty()
                        , "start to execute data synchronize pipeline:" + String.valueOf(initNodeCfg));

                context.infoLog("create task context,taskId:{},name:{}", triggerResult.getTaskid(), initNodeCfg.getDataXName());

                IDataxProcessor dataxProcessor = DataxProcessor.load(null, initNodeCfg.resourceType, initNodeCfg.dataXName);
                JSONObject iparams = IExecChainContext.createInstanceParams(triggerResult.getTaskid(), dataxProcessor,
                        false, Optional.of(triggerResult.getPluginCfgsMetas()));
                for (Map.Entry<String, Object> e : iparams.entrySet()) {
                    context.appendData2WfContext(e.getKey(), e.getValue());
                }
                context.appendData2WfContext(JobParams.KEY_JAVA_MEMORY_SPEC, triggerResult.getJavaMemorySpec());
                if (triggerResult.getPreviousTaskId() != null) {
                    context.appendData2WfContext(JobParams.KEY_PREVIOUS_TASK_ID, triggerResult.getPreviousTaskId());
                }
            } else {
                // trigger by mannual 手动触发
                Integer taskId = parseTaskId(instanceParams.getRight());
                svc.appendLog(LogLevel.INFO, taskId, Optional.empty()
                        , "start to execute data synchronize pipeline:" + JsonUtil.toString(instanceParams, false));
            }
        } finally {
            context.finallyCommit();
        }
    }

    public static class InitializeNodeCfg {
        public static InitializeNodeCfg parse(ITaskExecutorContext context) {
            JSONObject jobParams = (context.getJobParams());
            InitializeNodeCfg initializeNodeCfg = new InitializeNodeCfg(jobParams.getString(StoreResourceType.DATAX_NAME),
                    StoreResourceType.parse(jobParams.getString(StoreResourceType.KEY_STORE_RESOURCE_TYPE)));
            if (initializeNodeCfg.isTisDataflowType()) {
                Integer workflowId = Objects.requireNonNull(jobParams.getInteger(KEY_WORKFLOW_ID),
                        "key:" + KEY_WORKFLOW_ID + " must be present");
                initializeNodeCfg.setWorkflowId(workflowId);

            }
            return initializeNodeCfg;
        }

        private final String dataXName;
        public final StoreResourceType resourceType;
        private Integer workflowId;


        public InitializeNodeCfg(String dataXName, StoreResourceType resourceType) {
            this.dataXName = dataXName;
            this.resourceType = Objects.requireNonNull(resourceType);
        }

        public Integer getWorkflowId() {
            return workflowId;
        }

        public void setWorkflowId(Integer workflowId) {
            this.workflowId = workflowId;
        }

        public boolean isTisDataflowType() {
            return resourceType == StoreResourceType.DataFlow;
        }

        public String getDataXName() {
            return dataXName;
        }

        @Override
        public String toString() {
            return "{" +
                    "dataXName='" + dataXName + '\'' +
                    ", resourceType=" + resourceType +
                    ", workflowId=" + workflowId +
                    '}';
        }
    }
}
