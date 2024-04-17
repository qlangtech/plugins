package com.qlangtech.tis.datax.powerjob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.coredefine.module.action.PowerjobTriggerBuildResult;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.rpc.grpc.log.appender.LoggingEvent.Level;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.lang3.tuple.Pair;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.WorkflowContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;
import tech.powerjob.worker.log.OmsLogger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.qlangtech.tis.fullbuild.IFullBuildContext.KEY_WORKFLOW_ID;

/**
 * 所有子节点都要依赖该节点，如果发现是定时任务启动，则需要初始化TaskId等信息，提供给下游节点使用
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/26
 */
public class TISInitializeProcessor implements BasicProcessor {
    @Override
    public ProcessResult process(TaskContext context) throws Exception {
        OmsLogger omsLogger = context.getOmsLogger();
        Pair<Boolean, JSONObject> instanceParams = TISTableDumpProcessor.getInstanceParams(context);
        RpcServiceReference rpcSvcRef = TISTableDumpProcessor.getRpcServiceReference();
        StatusRpcClientFactory.AssembleSvcCompsite svc = rpcSvcRef.get();
        if (!instanceParams.getLeft()) {
            // 说明是定时任务触发
            omsLogger.info("trigger by crontab,now shall create taskId");
            InitializeNodeCfg initNodeCfg = InitializeNodeCfg.parse(context);

            IExecChainContext.TriggerNewTaskParam triggerParams =
                    new IExecChainContext.TriggerNewTaskParam(context.getWorkflowContext().getWfInstanceId(),
                            initNodeCfg.getDataXName(), initNodeCfg.isTisDataflowType()) {
                        @Override
                        public List<HttpUtils.PostParam> params() {
                            List<HttpUtils.PostParam> params = super.params();
                            if (initNodeCfg.isTisDataflowType()) {
                                // 为了满足 OfflineDatasourceAction.doExecuteWorkflow 执行
                                params.add(new HttpUtils.PostParam("id", initNodeCfg.getWorkflowId()));
                                params.add(new HttpUtils.PostParam(IFullBuildContext.DRY_RUN, false));
                            }
                            return params;
                        }
                    };
            /**=======================================================================
             *TriggerNewTask
             =======================================================================*/
            PowerjobTriggerBuildResult triggerResult = IExecChainContext.triggerNewTask(triggerParams);
            svc.appendLog(Level.INFO, triggerResult.getTaskid(), Optional.empty(), "start to execute data synchronize pipeline:" + String.valueOf(initNodeCfg));

            WorkflowContext wfContext = context.getWorkflowContext();
            omsLogger.info("create task context,taskId:{},name:{}", triggerResult.getTaskid(), initNodeCfg.getDataXName());
            JSONObject iparams = IExecChainContext.createInstanceParams(triggerResult.getTaskid(), () -> initNodeCfg.getDataXName(),
                    false, Optional.of(triggerResult.getPluginCfgsMetas()));
            for (Map.Entry<String, Object> e : iparams.entrySet()) {
                wfContext.appendData2WfContext(e.getKey(), e.getValue());
            }
            wfContext.appendData2WfContext(JobParams.KEY_JAVA_MEMORY_SPEC, triggerResult.getJavaMemorySpec());
            if (triggerResult.getPreviousTaskId() != null) {
                wfContext.appendData2WfContext(JobParams.KEY_PREVIOUS_TASK_ID, triggerResult.getPreviousTaskId());
            }
        } else {
            // trigger by mannual 手动触发
            Integer taskId = TISTableDumpProcessor.parseTaskId(instanceParams.getRight());
            svc.appendLog(Level.INFO, taskId, Optional.empty()
                    , "start to execute data synchronize pipeline:" + JsonUtil.toString(instanceParams, false));
        }

        return new ProcessResult(true);
    }


    public static class InitializeNodeCfg {
        public static InitializeNodeCfg parse(TaskContext context) {
            JSONObject jobParams = JSON.parseObject(context.getJobParams());
            InitializeNodeCfg initializeNodeCfg = new InitializeNodeCfg(jobParams.getString(DataxUtils.DATAX_NAME),
                    jobParams.getBooleanValue(DataxUtils.TIS_WORK_FLOW_CHANNEL));
            if (initializeNodeCfg.isTisDataflowType()) {
                Integer workflowId = Objects.requireNonNull(jobParams.getInteger(KEY_WORKFLOW_ID),
                        "key:" + KEY_WORKFLOW_ID + " must be present");
                initializeNodeCfg.setWorkflowId(workflowId);

            }
            return initializeNodeCfg;
        }

        private final String dataXName;
        private final boolean tisDataflowType;
        private Integer workflowId;


        public InitializeNodeCfg(String dataXName, boolean tisDataflowType) {
            this.dataXName = dataXName;
            this.tisDataflowType = tisDataflowType;
        }

        public Integer getWorkflowId() {
            return workflowId;
        }

        public void setWorkflowId(Integer workflowId) {
            this.workflowId = workflowId;
        }

        public boolean isTisDataflowType() {
            return tisDataflowType;
        }

        public String getDataXName() {
            return dataXName;
        }

        @Override
        public String toString() {
            return "{" +
                    "dataXName='" + dataXName + '\'' +
                    ", tisDataflowType=" + tisDataflowType +
                    ", workflowId=" + workflowId +
                    '}';
        }
    }
}
