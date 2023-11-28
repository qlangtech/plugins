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
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.enums.WorkflowInstanceStatus;
import tech.powerjob.common.response.WorkflowInstanceInfoDTO;

import java.util.Date;
import java.util.Objects;

import static com.qlangtech.tis.plugin.datax.powerjob.TISPowerJobClient.result;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/13
 */
public class WorkFlowBuildHistoryPayload {
    private static final Logger logger = LoggerFactory.getLogger(WorkFlowBuildHistoryPayload.class);
    private final Integer tisTaskId;
    private final ICommonDAOContext daoContext;
    // private final WorkflowInfoDTO wfInfo;
    private Long powerJobWorkflowInstanceId;

    public WorkFlowBuildHistoryPayload(Integer tisTaskId, ICommonDAOContext daoContext) {
        this.tisTaskId = Objects.requireNonNull(tisTaskId, "param tisTaskId can not be null");
        this.daoContext = Objects.requireNonNull(daoContext, "daoContent can not be null");
        // this.wfInfo = Objects.requireNonNull(wfInfo, "wfInfo can not be null");
    }

    public Integer getTisTaskId() {
        return this.tisTaskId;
    }

    public Long getPowerJobWorkflowInstanceId() {

        if (this.powerJobWorkflowInstanceId == null) {
            WorkFlowBuildHistory wfBuildHistory
                    = daoContext.getTaskBuildHistoryDAO().selectByPrimaryKey(tisTaskId);
            this.powerJobWorkflowInstanceId = ITISPowerJob.getPowerJobWorkflowInstanceId(wfBuildHistory, true);
        }
        return this.powerJobWorkflowInstanceId;
    }

    public void setPowerJobWorkflowInstanceId(Long workflowInstanceId) {
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
        this.powerJobWorkflowInstanceId = workflowInstanceId;
    }

//    /**
//     * 从日志中恢复
//     *
//     * @param workflowInstanceId
//     */
//    public void restorePowerJobWorkflowInstanceId(Long workflowInstanceId) {
//        this.powerJobWorkflowInstanceId = workflowInstanceId;
//    }

    public ExecResult processExecHistoryRecord(PowerJobClient powerJobClient) {

        Long powerJobWorkflowInstanceId = this.getPowerJobWorkflowInstanceId();

        WorkflowInstanceInfoDTO workflowInstanceInfo = result(powerJobClient.fetchWorkflowInstanceInfo(powerJobWorkflowInstanceId));

        WorkflowInstanceStatus wfStatus = WorkflowInstanceStatus.of(workflowInstanceInfo.getStatus());
        if (WorkflowInstanceStatus.FINISHED_STATUS.contains(wfStatus.getV())) {
            ExecResult execResult = null;
            switch (wfStatus) {
                case SUCCEED:
                    execResult = (ExecResult.SUCCESS);
                    break;
                case FAILED: {
//                    for (PEWorkflowDAG.Node wfNode : this.wfInfo.getPEWorkflowDAG().getNodes()) {
//
//
//                        powerJobClient.fetchInstanceInfo(wfNode.getInstanceId());
//                    }
                    execResult = (ExecResult.FAILD);
                    break;
                }
                case STOPPED:
                    execResult = (ExecResult.CANCEL);
                    break;
                default:
                    throw new IllegalStateException("illegal status :" + wfStatus);
            }
            this.updateFinalStatus(execResult);
            return execResult;
        }

        return null;
    }

    public void updateFinalStatus(ExecResult execResult) {
        WorkFlowBuildHistory record = new WorkFlowBuildHistory();
        record.setState((byte) execResult.getValue());
        record.setEndTime(new Date());
        record.setOpTime(new Date());
        WorkFlowBuildHistoryCriteria criteria = new WorkFlowBuildHistoryCriteria();
        criteria.createCriteria().andIdEqualTo(tisTaskId);
        if (daoContext.getTaskBuildHistoryDAO().updateByExampleSelective(record, criteria) < 1) {
            throw new IllegalStateException("tisTaskId:" + tisTaskId + " update to new exec state:" + execResult + " falid");
        }
    }

    static class SubmitLog {
        public final Long powerjobWorkflowInstanceId;
        private final Integer tisTaskId;

        private ExecResult execResult;

        public SubmitLog(Long powerjobWorkflowInstanceId, Integer tisTaskId) {
            this.powerjobWorkflowInstanceId = Objects.requireNonNull(powerjobWorkflowInstanceId, "powerjobWorkflowInstanceId can not be null");
            this.tisTaskId = Objects.requireNonNull(tisTaskId, "tisTaskId can not be null");
        }

        public ExecResult getExecResult() {
            return execResult;
        }

        public WorkFlowBuildHistoryPayload restore(ICommonDAOContext daoContext) {
            WorkFlowBuildHistoryPayload history = new WorkFlowBuildHistoryPayload(this.tisTaskId, daoContext);
            history.powerJobWorkflowInstanceId = (this.powerjobWorkflowInstanceId);
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
