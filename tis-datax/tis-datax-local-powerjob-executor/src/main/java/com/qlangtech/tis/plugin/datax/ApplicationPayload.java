package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.biz.dal.pojo.ApplicationCriteria;
import com.qlangtech.tis.trigger.util.JsonUtil;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.response.WorkflowInfoDTO;

import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/10
 */
public class ApplicationPayload {

    private static final String EXEC_RANGE = "execRange";
    private final Application application;
    private final IApplicationDAO applicationDAO;

    public ApplicationPayload(String appName, IApplicationDAO applicationDAO) {
        this.applicationDAO = Objects.requireNonNull(applicationDAO, "applicationDAO can not be null");
        this.application = Objects.requireNonNull(applicationDAO.selectByName(appName), "appName:" + appName + " relevant app can not be null");
    }

    public PowerJobWorkflow getPowerJobWorkflowId(boolean validateWorkflowId) {
        // Application application = applicationDAO.selectByName(appName);
        JSONObject payload = getAppPayload();
        Long workflowId = payload.getLong(IFullBuildContext.KEY_WORKFLOW_ID);
        if (validateWorkflowId) {
            Objects.requireNonNull(workflowId
                    , "param " + IFullBuildContext.KEY_WORKFLOW_ID + " can not be null");
        }

        if (workflowId == null) {
            return null;
        }

        JSONArray execRange = Objects.requireNonNull(payload.getJSONArray(EXEC_RANGE)
                , "key:" + EXEC_RANGE + " relevant props can not be null");

        if (execRange.size() != 2) {
            throw new IllegalStateException("execRange.size() must be 2 ,but now is " + JsonUtil.toString(execRange));
        }

        return new PowerJobWorkflow(workflowId
                , new ExecutePhaseRange(FullbuildPhase.parse(execRange.getString(0)), FullbuildPhase.parse(execRange.getString(1))));
    }

    public static class PowerJobWorkflow {
        private final Long workflowId;
        private final ExecutePhaseRange executePhaseRange;

        public PowerJobWorkflow(Long workflowId, ExecutePhaseRange executePhaseRange) {
            this.workflowId = workflowId;
            this.executePhaseRange = executePhaseRange;
        }

        public Long getWorkflowId() {
            return workflowId;
        }

        public boolean isDisbaled(PowerJobClient powerClient) {
            try {
                WorkflowInfoDTO wfDTO = DistributedPowerJobDataXJobSubmit.result(powerClient.fetchWorkflow(workflowId));
                return wfDTO == null || !wfDTO.getEnable();
            } catch (Exception e) {
                return false;
            }
        }

        public ExecutePhaseRange getExecutePhaseRange() {
            return executePhaseRange;
        }
    }


    private JSONObject getAppPayload() {
        JSONObject payload = JSONObject.parseObject(Objects.requireNonNull(application, "application can not be null").getFullBuildCronTime());
        return payload == null ? new JSONObject() : payload;
    }


    public void setPowerJobWorkflowId(Long workflowId, ExecutePhaseRange executePhaseRange) {

        JSONObject appPayload = getAppPayload();
        appPayload.put(IFullBuildContext.KEY_WORKFLOW_ID, workflowId);
        appPayload.put(EXEC_RANGE, new String[]{Objects.requireNonNull(executePhaseRange, "param executePhaseRange can not be null").getStart().getName()
                , executePhaseRange.getEnd().getName()});

        Application app = new Application();
        app.setFullBuildCronTime(JsonUtil.toString(appPayload));
        this.application.setFullBuildCronTime(app.getFullBuildCronTime());
        ApplicationCriteria appCriteria = new ApplicationCriteria();
        appCriteria.createCriteria().andAppIdEqualTo(application.getAppId());
        if (applicationDAO.updateByExampleSelective(app, appCriteria) < 1) {
            throw new IllegalStateException("app:" + application.getProjectName() + " update workflowId in payload faild");
        }
    }
}
