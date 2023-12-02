package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.dao.ICommonDAOContext;
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
import com.qlangtech.tis.workflow.dao.IWorkFlowDAO;
import com.qlangtech.tis.workflow.pojo.WorkFlow;
import com.qlangtech.tis.workflow.pojo.WorkFlowCriteria;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.model.PEWorkflowDAG;
import tech.powerjob.common.response.WorkflowInfoDTO;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.datax.DistributedPowerJobDataXJobSubmit.vistWorkflowNodes;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/2
 */
public abstract class PowerWorkflowPayload {

    private static final Logger logger = LoggerFactory.getLogger(PowerWorkflowPayload.class);

    private static final String EXEC_RANGE = "execRange";

    public static PowerWorkflowPayload createApplicationPayload(String appName, ICommonDAOContext daoContext) {
        return new ApplicationPayload(appName, daoContext.getApplicationDAO());
    }

    public static PowerWorkflowPayload createTISWorkflowPayload(String name, ICommonDAOContext commonDAOContext) {
        return new TISWorkflowPayload(name, commonDAOContext.getWorkFlowDAO());
    }


    public PowerJobWorkflow getPowerJobWorkflowId(boolean validateWorkflowId) {
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

        return new PowerJobWorkflow(this.getTargetEntityName(), workflowId
                , new ExecutePhaseRange( //
                FullbuildPhase.parse(execRange.getString(0)) //
                , FullbuildPhase.parse(execRange.getString(1))));
    }

    /**
     * application.getProjectName()
     *
     * @return
     */
    protected abstract String getTargetEntityName();

    public static class PowerJobWorkflow {
        private final Long powerjobWorkflowId;
        private final ExecutePhaseRange executePhaseRange;
        private final String appName;

        public PowerJobWorkflow(String appName, Long powerjobWorkflowId, ExecutePhaseRange executePhaseRange) {
            if (StringUtils.isEmpty(appName)) {
                throw new IllegalArgumentException("param appName can not be null");
            }
            this.powerjobWorkflowId = powerjobWorkflowId;
            this.executePhaseRange = executePhaseRange;
            this.appName = appName;
        }

        public Long getPowerjobWorkflowId() {
            return powerjobWorkflowId;
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
                WorkflowInfoDTO wfDTO = TISPowerJobClient.result(powerClient.fetchWorkflow(powerjobWorkflowId));
                if (wfDTO == null || !wfDTO.getEnable()) {
                    return new WorkflowUnEffectiveJudge(true);
                }
                //PEWorkflowDAG dag = wfDTO.getPEWorkflowDAG();
//                if (dag.getNodes().size() != selectedTabTriggers.size()) {
//                    return new WorkflowUnEffectiveJudge(true);
//                }
                Map<String /**tableName*/, SelectedTabTriggers> tabTriggers
                        = selectedTabTriggers.entrySet().stream().collect(Collectors.toMap((e) -> e.getKey().getName(), (e) -> e.getValue()));
                vistWorkflowNodes(this.appName, wfDTO, new DistributedPowerJobDataXJobSubmit.WorkflowVisit() {
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

                            if (!wfNode.getEnable() || !JsonUtil.objEquals(JSONObject.parseObject(wfNode.getNodeParams())
                                    , tabTrigger.createMRParams()
                                    , Sets.newHashSet("/exec/taskSerializeNum", "/exec/jobInfo[]/taskSerializeNum"))) {
                                // 触发条件更改了
                                unEffectiveJudge.setUnEffective();
                            }
                        }
                    }
                });

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


    protected final JSONObject getAppPayload() {
        JSONObject payload = JSONObject.parseObject(getPayloadContent());
        return payload == null ? new JSONObject() : payload;
    }

    protected abstract String getPayloadContent();


    public void setPowerJobWorkflowId(Long workflowId, ExecutePhaseRange executePhaseRange) {

        JSONObject appPayload = getAppPayload();
        appPayload.put(IFullBuildContext.KEY_WORKFLOW_ID, workflowId);
        appPayload.put(EXEC_RANGE, new String[]{Objects.requireNonNull(
                executePhaseRange, "param executePhaseRange can not be null").getStart().getName()
                , executePhaseRange.getEnd().getName()});
        setPowerJobWorkflowPayload(appPayload);

    }


    protected abstract void setPowerJobWorkflowPayload(JSONObject appPayload);


    private static class TISWorkflowPayload extends PowerWorkflowPayload {

        private final String tisWorkflowName;
        private final IWorkFlowDAO workFlowDAO;

        private WorkFlow tisWorkflow;

        private TISWorkflowPayload(String tisWorkflowName, IWorkFlowDAO workFlowDAO) {
            if (StringUtils.isEmpty(tisWorkflowName)) {
                throw new IllegalArgumentException("param  tisWorkflowName can not be empty");
            }
            this.tisWorkflowName = tisWorkflowName;
            this.workFlowDAO = Objects.requireNonNull(workFlowDAO
                    , "param workFlowDAO can not be null");
        }

        @Override
        protected String getTargetEntityName() {
            return this.tisWorkflowName;
        }

        private WorkFlow load() {
            if (this.tisWorkflow == null) {
                WorkFlowCriteria criteria = new WorkFlowCriteria();
                criteria.createCriteria().andNameEqualTo(this.tisWorkflowName);
                List<WorkFlow> workFlows = workFlowDAO.selectByExample(criteria);
                for (WorkFlow wf : workFlows) {
                    this.tisWorkflow = wf;
                    break;
                }
                Objects.requireNonNull(this.tisWorkflow
                        , "workflowName:" + this.tisWorkflowName + " relevant tisWorkflow can not be null");
            }
            return this.tisWorkflow;
        }

        @Override
        protected String getPayloadContent() {
            return this.load().getGitPath();
        }

        @Override
        protected void setPowerJobWorkflowPayload(JSONObject appPayload) {

            WorkFlow wf = new WorkFlow();
            wf.setOpTime(new Date());
            wf.setGitPath(JsonUtil.toString(Objects.requireNonNull(appPayload, "appPayload can not be null")));
            WorkFlow beUpdate = this.load();
            WorkFlowCriteria criteria = new WorkFlowCriteria();
            criteria.createCriteria().andIdEqualTo(beUpdate.getId());

            if (this.workFlowDAO.updateByExampleSelective(wf, criteria) < 1) {
                throw new IllegalStateException("app:" + beUpdate.getName() + " update workflowId in payload faild");
            }
        }
    }

    /**
     * @author 百岁 (baisui@qlangtech.com)
     * @date 2023/11/10
     */
    private static class ApplicationPayload extends PowerWorkflowPayload {

        private final Application application;
        private final IApplicationDAO applicationDAO;

        private ApplicationPayload(String appName, IApplicationDAO applicationDAO) {
            this.applicationDAO = Objects.requireNonNull(applicationDAO, "applicationDAO can not be null");
            this.application = Objects.requireNonNull(//
                    applicationDAO.selectByName(appName), "appName:" + appName + " relevant app can not be null");
        }

        @Override
        protected String getTargetEntityName() {
            return this.application.getProjectName();
        }

        @Override
        protected String getPayloadContent() {
            return Objects.requireNonNull(application, "application can not be null").getFullBuildCronTime();
        }

        @Override
        protected void setPowerJobWorkflowPayload(JSONObject appPayload) {
            Application app = new Application();
            app.setFullBuildCronTime(JsonUtil.toString(Objects.requireNonNull(appPayload, "appPayload can not be null")));
            this.application.setFullBuildCronTime(app.getFullBuildCronTime());
            ApplicationCriteria appCriteria = new ApplicationCriteria();
            appCriteria.createCriteria().andAppIdEqualTo(application.getAppId());
            if (applicationDAO.updateByExampleSelective(app, appCriteria) < 1) {
                throw new IllegalStateException("app:" + application.getProjectName() + " update workflowId in payload faild");
            }
        }
    }
}
