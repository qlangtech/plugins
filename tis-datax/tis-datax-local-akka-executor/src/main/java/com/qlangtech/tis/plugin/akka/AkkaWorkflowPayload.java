package com.qlangtech.tis.plugin.akka;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.coredefine.module.action.DistributeJobTriggerBuildResult;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.plugin.datax.BasicWorkflowInstance;
import com.qlangtech.tis.plugin.datax.BasicWorkflowPayload;
import com.qlangtech.tis.plugin.datax.SPIExecContext;
import com.qlangtech.tis.plugin.datax.WorkFlowBuildHistoryPayload;
import com.qlangtech.tis.plugin.datax.powerjob.WorkflowUnEffectiveJudge;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/31
 */
public class AkkaWorkflowPayload extends BasicWorkflowPayload<AkkaWorkflow> {

    public AkkaWorkflowPayload(IDataxProcessor dataxProcessor, DistributedAKKAJobDataXJobSubmit submit) {
        super(dataxProcessor, submit);
    }

    @Override
    protected AkkaWorkflow createWorkflowInstance(Long spiWorkflowId, ExecutePhaseRange execRange) {
        return null;
    }

    @Override
    public DistributeJobTriggerBuildResult triggerWorkflow(IExecChainContext execChainContext,
                                                           RpcServiceReference feedback) {
        JSONObject instanceParams = new JSONObject();
        DistributeJobTriggerBuildResult buildResult = new DistributeJobTriggerBuildResult(true, instanceParams);
        return buildResult;
    }

    @Override
    protected String getPayloadContent() {
        return "";
    }

    @Override
    public void innerCreatePowerjobWorkflow(boolean updateProcess, Optional<Pair<Map<ISelectedTab, SelectedTabTriggers>, Map<String, ISqlTask>>> selectedTabTriggers, Optional<WorkflowUnEffectiveJudge> unEffectiveOpt) {

    }

    @Override
    protected SPIExecContext createSPIExecContext() {
        return null;
    }

    @Override
    protected WorkFlowBuildHistoryPayload createBuildHistoryPayload(Integer tisTaskId) {
        return null;
    }

    @Override
    protected Long runSPIWorkflow(BasicWorkflowInstance spiWorkflowId, JSONObject instanceParams) {
        return 0L;
    }

    @Override
    protected void setSPIWorkflowPayload(JSONObject appPayload) {

    }
}
