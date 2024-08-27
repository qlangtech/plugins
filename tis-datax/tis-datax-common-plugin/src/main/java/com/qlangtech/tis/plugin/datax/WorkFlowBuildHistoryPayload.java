package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.dao.ICommonDAOContext;
import com.qlangtech.tis.datax.job.ITISPowerJob;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistoryCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/13
 */
public abstract class WorkFlowBuildHistoryPayload {
    private static final Logger logger = LoggerFactory.getLogger(WorkFlowBuildHistoryPayload.class);
    private final Integer tisTaskId;
    private final ICommonDAOContext daoContext;
    // private final WorkflowInfoDTO wfInfo;
    private Long spiWorkflowInstanceId;

    public WorkFlowBuildHistoryPayload(Integer tisTaskId, ICommonDAOContext daoContext) {
        this.tisTaskId = Objects.requireNonNull(tisTaskId, "param tisTaskId can not be null");
        this.daoContext = Objects.requireNonNull(daoContext, "daoContent can not be null");
        // this.wfInfo = Objects.requireNonNull(wfInfo, "wfInfo can not be null");
    }

    public Integer getTisTaskId() {
        return this.tisTaskId;
    }

    public Long getSPIWorkflowInstanceId() {

        if (this.spiWorkflowInstanceId == null) {
            WorkFlowBuildHistory wfBuildHistory
                    = daoContext.getTaskBuildHistoryDAO().selectByPrimaryKey(tisTaskId);
            this.spiWorkflowInstanceId = ITISPowerJob.getPowerJobWorkflowInstanceId(wfBuildHistory, true);
        }
        return this.spiWorkflowInstanceId;
    }

    public void setSPIWorkflowInstanceId(Long workflowInstanceId) {
        // logger.info("create workflow instanceId:{}", workflowInstanceId);
        // 需要将task执行历史记录更新，将instanceId 绑定到历史记录上去，以便后续最终
        WorkFlowBuildHistory record = new WorkFlowBuildHistory();
        JSONObject wfHistory = new JSONObject();
        wfHistory.put(ITISPowerJob.KEY_POWERJOB_WORKFLOW_INSTANCE_ID, workflowInstanceId);
        record.setAsynSubTaskStatus(JsonUtil.toString(wfHistory));
        WorkFlowBuildHistoryCriteria taskHistoryCriteria = new WorkFlowBuildHistoryCriteria();
        taskHistoryCriteria.createCriteria().andIdEqualTo(tisTaskId);
        if (daoContext.getTaskBuildHistoryDAO().updateByExampleSelective(record, taskHistoryCriteria) < 1) {
            throw new IllegalStateException("update taskBuildHistory faild,taskId:" + tisTaskId
                    + ",powerJob workflowInstanceId:" + workflowInstanceId);
        }
        this.spiWorkflowInstanceId = workflowInstanceId;
    }

    public abstract ExecResult processExecHistoryRecord();


    public void updateFinalStatus(ExecResult execResult) {
        WorkFlowBuildHistory record = new WorkFlowBuildHistory();
        record.setState((byte) execResult.getValue());
        record.setEndTime(new Date());
        record.setOpTime(new Date());
        WorkFlowBuildHistoryCriteria criteria = new WorkFlowBuildHistoryCriteria();
        criteria.createCriteria().andIdEqualTo(tisTaskId);
        if (daoContext.getTaskBuildHistoryDAO().updateByExampleSelective(record, criteria) < 1) {
            throw new IllegalStateException("tisTaskId:"
                    + tisTaskId + " update to new exec state:" + execResult + " falid");
        }
    }

    public abstract <T extends WorkFlowBuildHistoryPayloadFactory> Class<T> getFactory();

    static class SubmitLog {
        public final Long powerjobWorkflowInstanceId;
        private final Integer tisTaskId;
        private final WorkFlowBuildHistoryPayloadFactory workFlowBuildHistoryPayloadFactory;

        private ExecResult execResult;

        public SubmitLog(Long powerjobWorkflowInstanceId, Integer tisTaskId
                , WorkFlowBuildHistoryPayloadFactory workFlowBuildHistoryPayloadFactory) {
            this.powerjobWorkflowInstanceId = Objects.requireNonNull(powerjobWorkflowInstanceId, "powerjobWorkflowInstanceId can not be null");
            this.tisTaskId = Objects.requireNonNull(tisTaskId, "tisTaskId can not be null");
            this.workFlowBuildHistoryPayloadFactory
                    = Objects.requireNonNull(workFlowBuildHistoryPayloadFactory, "workFlowBuildHistoryPayloadFactory can not be null");
        }

        public ExecResult getExecResult() {
            return execResult;
        }

        public WorkFlowBuildHistoryPayload restore(ICommonDAOContext daoContext) {
            WorkFlowBuildHistoryPayload history = workFlowBuildHistoryPayloadFactory.create(this.tisTaskId, daoContext);// new WorkFlowBuildHistoryPayload(this.tisTaskId, daoContext);
            history.spiWorkflowInstanceId = (this.powerjobWorkflowInstanceId);
            return history;
        }


        public void setExecResult(ExecResult execResult) {
            this.execResult = execResult;
        }

        public void overwrite(SubmitLog submitLog) {
            if (this.execResult == null && submitLog.execResult != null) {
                this.execResult = submitLog.execResult;
            }
        }
    }
}
