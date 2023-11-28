package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.manage.biz.dal.dao.IApplicationDAO;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.biz.dal.pojo.ApplicationCriteria;
import com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.response.WorkflowInfoDTO;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.vistWorkflowNodes;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/10
 */
public class ApplicationPayload {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationPayload.class);
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

        return new PowerJobWorkflow(application, workflowId
                , new ExecutePhaseRange(FullbuildPhase.parse(execRange.getString(0)), FullbuildPhase.parse(execRange.getString(1))));
    }

    public static class PowerJobWorkflow {
        private final Long workflowId;
        private final ExecutePhaseRange executePhaseRange;
        private final Application application;

        public PowerJobWorkflow(Application application, Long workflowId, ExecutePhaseRange executePhaseRange) {
            this.workflowId = workflowId;
            this.executePhaseRange = executePhaseRange;
            this.application = application;
        }

        public Long getWorkflowId() {
            return workflowId;
        }

        /**
         * 是否已经失效
         *
         * @param powerClient
         * @param selectedTabTriggers
         * @return
         */
        public WorkflowUnEffectiveJudge isUnEffective(PowerJobClient powerClient, Map<ISelectedTab, SelectedTabTriggers> selectedTabTriggers) {
            WorkflowUnEffectiveJudge unEffectiveJudge = new WorkflowUnEffectiveJudge();
            try {
                WorkflowInfoDTO wfDTO = TISPowerJobClient.result(powerClient.fetchWorkflow(workflowId));
                if (wfDTO == null || !wfDTO.getEnable()) {
                    return new WorkflowUnEffectiveJudge(true);
                }
                //PEWorkflowDAG dag = wfDTO.getPEWorkflowDAG();
//                if (dag.getNodes().size() != selectedTabTriggers.size()) {
//                    return new WorkflowUnEffectiveJudge(true);
//                }
                Map<String /**tableName*/, SelectedTabTriggers> tabTriggers
                        = selectedTabTriggers.entrySet().stream().collect(Collectors.toMap((e) -> e.getKey().getName(), (e) -> e.getValue()));
                vistWorkflowNodes(this.application.getProjectName(), wfDTO, new DistributedPowerJobDataXJobSubmit.WorkflowVisit() {
                    @Override
                    public void vistStartInitNode(PEWorkflowDAG.Node node) {
                        unEffectiveJudge.setStatInitNode(node);
                    }

                    @Override
                    public void vistWorkerNode(PEWorkflowDAG.Node wfNode) {
                        SelectedTabTriggers tabTrigger = tabTriggers.get(wfNode.getNodeName());
                        if (tabTrigger == null) {
                            // 该表同步已经被删除
                            unEffectiveJudge.addDeletedWfNode(wfNode);
                        } else {
                            unEffectiveJudge.addExistWfNode(tabTrigger, wfNode);
                            if (!wfNode.getEnable() || !StringUtils.equals(wfNode.getNodeParams(), JsonUtil.toString(tabTrigger.createMRParams()))) {
                                // 触发条件更改了
                                unEffectiveJudge.setUnEffective();
                            }
                        }
                    }
                });


//                 tabTrigger = null;
//
//                for (PEWorkflowDAG.Node wfNode : dag.getNodes()) {
//                    tabTrigger = tabTriggers.get(wfNode.getNodeName());
//                    if (tabTrigger == null) {
//                        // 该表同步已经被删除
//                        unEffectiveJudge.addDeletedWfNode(wfNode);
//                    } else {
//                        unEffectiveJudge.addExistWfNode(tabTrigger, wfNode);
//                        if (!wfNode.getEnable() || !StringUtils.equals(wfNode.getNodeParams(), JsonUtil.toString(tabTrigger.createMRParams()))) {
//                            // 触发条件更改了
//                            unEffectiveJudge.setUnEffective();
//                        }
//                    }
//
//                    // return unEffectiveJudge;
//
////                    Objects.requireNonNull()
////                            , "table name:" + wfNode.getNodeName() + " can not get relevant tabTrigger ");
//
//                }

            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                return unEffectiveJudge.setUnEffective();
            }

            return unEffectiveJudge;
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
