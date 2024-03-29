package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/24
 */
public class TriggrWorkflowJobs {

    private static final String KEY_WORKFLOW_INSTANCE_ID = "workflow_instance_id";
    private static final String KEY_TIS_TASK_ID = "tis_task_id";
    private static final String KEY_EXEC_STATUS = "exec_result";
    private final BlockingQueue<WorkFlowBuildHistoryPayload> triggrWorkflowJobs = new ArrayBlockingQueue<>(200);
    private final File submitWorkflowJobs;

    private TriggrWorkflowJobs(File submitWorkflowJobs) throws Exception {
        this.submitWorkflowJobs = submitWorkflowJobs;
    }


    public static TriggrWorkflowJobs create(ICommonDAOContext daoContext) throws Exception {

        WorkFlowBuildHistoryPayload history = null;
        File submitWorkflowJobs = new File(Config.getMetaCfgDir(), "submit-workflow-jobs.json");
        TriggrWorkflowJobs triggrWorkflowJobs = new TriggrWorkflowJobs(submitWorkflowJobs);
        Map<Long /**powerjobWorkflowInstanceId*/, WorkFlowBuildHistoryPayload.SubmitLog> submitLogs = Maps.newHashMap();
        if (submitWorkflowJobs.exists()) {
            LineIterator lineIt = FileUtils.lineIterator(submitWorkflowJobs, TisUTF8.getName());
            JSONObject info = null;


            WorkFlowBuildHistoryPayload.SubmitLog submitLog = null;
            WorkFlowBuildHistoryPayload.SubmitLog preSubmitLog = null;
            Integer execResult = null;
            while (lineIt.hasNext()) {

                info = JSONObject.parseObject(lineIt.nextLine());
                submitLog = new WorkFlowBuildHistoryPayload.SubmitLog(info.getLong(KEY_WORKFLOW_INSTANCE_ID), info.getInteger(KEY_TIS_TASK_ID));
                execResult = info.getInteger(KEY_EXEC_STATUS);
                if (execResult != null) {
                    submitLog.setExecResult(ExecResult.parse(execResult));
                }
                if ((preSubmitLog = submitLogs.get(submitLog.powerjobWorkflowInstanceId)) != null) {
                    preSubmitLog.overwrite(submitLog);
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

    public void offer(WorkFlowBuildHistoryPayload workFlowBuildHistoryPayload) {
        if (triggrWorkflowJobs.offer(workFlowBuildHistoryPayload)) {
            //  return new File(Config.getMetaCfgDir(), "df-logs/" + taskid + "/" + phase.getName());
            appendLog(workFlowBuildHistoryPayload, Optional.empty());
        }
    }

    private void appendLog(WorkFlowBuildHistoryPayload workFlowBuildHistoryPayload, Optional<ExecResult> execResult) {
        JSONObject wfHistory = new JSONObject();
        wfHistory.put(KEY_WORKFLOW_INSTANCE_ID, workFlowBuildHistoryPayload.getPowerJobWorkflowInstanceId());
        wfHistory.put(KEY_TIS_TASK_ID, workFlowBuildHistoryPayload.getTisTaskId());


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
