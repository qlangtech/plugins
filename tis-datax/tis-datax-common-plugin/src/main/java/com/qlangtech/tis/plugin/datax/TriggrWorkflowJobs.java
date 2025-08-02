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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/24
 */
public class TriggrWorkflowJobs {

    private static final String KEY_WORKFLOW_INSTANCE_ID = "workflow_instance_id";
    private static final String KEY_WORKFLOW_BUILD_HISTORY_FACTORY = "workflow_build_factory";

    private static final String KEY_TIS_TASK_ID = "tis_task_id";
    private static final String KEY_EXEC_STATUS = "exec_result";
    final BlockingQueue<WorkFlowBuildHistoryPayload> triggrWorkflowJobs = new ArrayBlockingQueue<>(200);
    private final File submitWorkflowJobs;

    private TriggrWorkflowJobs(File submitWorkflowJobs) throws Exception {
        this.submitWorkflowJobs = submitWorkflowJobs;
    }

    public static TriggrWorkflowJobs create(ICommonDAOContext daoContext) throws Exception {
        return create(daoContext, (workflowBuildHistoryFactory) -> {
            try {
                Class<?> clazz = TIS.get().getPluginManager().uberClassLoader.loadClass(workflowBuildHistoryFactory);
                return clazz;
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }


    public static TriggrWorkflowJobs create(ICommonDAOContext daoContext, Function<String, Class<?>> workflowBuildHistoryFactoryLoader) throws Exception {

        WorkFlowBuildHistoryPayload history = null;
        File submitWorkflowJobs = createSubmitWorkflowJobsFile();
        TriggrWorkflowJobs triggrWorkflowJobs = new TriggrWorkflowJobs(submitWorkflowJobs);
        Map<Long /**powerjobWorkflowInstanceId*/, WorkFlowBuildHistoryPayload.SubmitLog> submitLogs = Maps.newHashMap();
        if (submitWorkflowJobs.exists()) {
            LineIterator lineIt = FileUtils.lineIterator(submitWorkflowJobs, TisUTF8.getName());


            WorkFlowBuildHistoryPayload.SubmitLog submitLog = null;
            WorkFlowBuildHistoryPayload.SubmitLog preSubmitLog = null;
            Integer execResult = null;
            while (lineIt.hasNext()) {

                JSONObject info = JSONObject.parseObject(lineIt.nextLine());
                String workflowBuildHistoryFactory = info.getString(KEY_WORKFLOW_BUILD_HISTORY_FACTORY);
                Class<?> clazz = workflowBuildHistoryFactoryLoader.apply(workflowBuildHistoryFactory);
                if (clazz == null) {
                    throw new IllegalStateException("workflow build historyFactory:" + workflowBuildHistoryFactory + " can not find relevant class instance");
                }

                submitLog = new WorkFlowBuildHistoryPayload.SubmitLog(
                        () -> {
                            return DataxProcessor.load(null
                                    , StoreResourceType.parse(info.getString(StoreResourceType.KEY_STORE_RESOURCE_TYPE))
                                    , info.getString(IFullBuildContext.KEY_APP_NAME));
                        },
                        info.getLong(KEY_WORKFLOW_INSTANCE_ID)
                        , info.getInteger(KEY_TIS_TASK_ID)
                        , (WorkFlowBuildHistoryPayloadFactory) clazz.newInstance());

                execResult = info.getInteger(KEY_EXEC_STATUS);
                if (execResult != null) {
                    submitLog.setExecResult(ExecResult.parse(execResult));
                }
                if ((preSubmitLog = submitLogs.get(submitLog.spiWorkflowInstanceId)) != null) {
                    preSubmitLog.overwrite(submitLog);
                } else {
                    submitLogs.put(submitLog.spiWorkflowInstanceId, submitLog);
                }
            }
        }


        for (WorkFlowBuildHistoryPayload.SubmitLog log : submitLogs.values()) {
            if (log.getExecResult() == null) {
                history = log.restore(daoContext);// new WorkFlowBuildHistoryPayload(log.tisTaskId, daoContext);
                //  history.restorePowerJobWorkflowInstanceId(log.powerjobWorkflowInstanceId);
                triggrWorkflowJobs.triggrWorkflowJobs.offer(history);
            }
        }

        return triggrWorkflowJobs;
    }


    public static final String FILE_NAME_SUBMIT_WORKFLOW_JOBS = "submit-workflow-jobs.json";

    public static File createSubmitWorkflowJobsFile() {
        File submitWorkflowJobs = new File(Config.getMetaCfgDir(), FILE_NAME_SUBMIT_WORKFLOW_JOBS);
        return submitWorkflowJobs;
    }

    public void offer(WorkFlowBuildHistoryPayload workFlowBuildHistoryPayload) {
        if (triggrWorkflowJobs.offer(workFlowBuildHistoryPayload)) {
            //  return new File(Config.getMetaCfgDir(), "df-logs/" + taskid + "/" + phase.getName());
            appendLog(workFlowBuildHistoryPayload, Optional.empty());
        }
    }


    private void appendLog(WorkFlowBuildHistoryPayload workFlowBuildHistoryPayload, Optional<ExecResult> execResult) {
//        JSONObject wfHistory =  new JSONObject();
        IDataxProcessor dataxProcessor = Objects.requireNonNull(workFlowBuildHistoryPayload.dataxProcessor, "dataxProcessor can not be null");
        JSONObject wfHistory = dataxProcessor.createNode();
        wfHistory.put(KEY_WORKFLOW_INSTANCE_ID, workFlowBuildHistoryPayload.getSPIWorkflowInstanceId());
        wfHistory.put(KEY_TIS_TASK_ID, workFlowBuildHistoryPayload.getTisTaskId());
        wfHistory.put(KEY_WORKFLOW_BUILD_HISTORY_FACTORY, workFlowBuildHistoryPayload.getFactory().getName());
//        wfHistory.put(StoreResourceType.KEY_STORE_RESOURCE_TYPE, dataxProcessor.getResType().getType());
//        wfHistory.put(IFullBuildContext.KEY_APP_NAME, dataxProcessor.identityValue());

        // DataxProcessor.load()
        execResult.ifPresent((er) -> {
            wfHistory.put(KEY_EXEC_STATUS, er.getValue());
        });

        try {
            FileUtils.write(submitWorkflowJobs, JsonUtil.toString(wfHistory, false) + "\n", TisUTF8.get(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public WorkFlowBuildHistoryPayload poll() {
        return triggrWorkflowJobs.poll();
    }

    public void taskFinal(WorkFlowBuildHistoryPayload workFlowBuildHistoryPayload, ExecResult execResult) {
        appendLog(workFlowBuildHistoryPayload, Optional.of(execResult));
    }
}
