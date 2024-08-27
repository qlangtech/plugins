package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.datax.executor.BasicTISInitializeProcessor;
import com.qlangtech.tis.datax.powerjob.impl.PowerJobTaskContext;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;

/**
 * 所有子节点都要依赖该节点，如果发现是定时任务启动，则需要初始化TaskId等信息，提供给下游节点使用
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/26
 */
public class TISInitializeProcessor extends BasicTISInitializeProcessor implements BasicProcessor {
    @Override
    public ProcessResult process(TaskContext context) throws Exception {

        this.initializeProcess(new PowerJobTaskContext(context));
        return new ProcessResult(true);
//        OmsLogger omsLogger = context.getOmsLogger();
//        Pair<Boolean, JSONObject> instanceParams = TISTableDumpProcessor.getInstanceParams(context);
//        RpcServiceReference rpcSvcRef = TISTableDumpProcessor.getRpcServiceReference();
//        StatusRpcClientFactory.AssembleSvcCompsite svc = rpcSvcRef.get();
//        if (!instanceParams.getLeft()) {
//            // 说明是定时任务触发
//            omsLogger.info("trigger by crontab,now shall create taskId");
//            InitializeNodeCfg initNodeCfg = InitializeNodeCfg.parse(context);
//
//            IExecChainContext.TriggerNewTaskParam triggerParams =
//                    new IExecChainContext.TriggerNewTaskParam(context.getWorkflowContext().getWfInstanceId(),
//                            initNodeCfg.getDataXName(), initNodeCfg.isTisDataflowType()) {
//                        @Override
//                        public List<HttpUtils.PostParam> params() {
//                            List<HttpUtils.PostParam> params = super.params();
//                            if (initNodeCfg.isTisDataflowType()) {
//                                // 为了满足 OfflineDatasourceAction.doExecuteWorkflow 执行
//                                params.add(new HttpUtils.PostParam("id", initNodeCfg.getWorkflowId()));
//                                params.add(new HttpUtils.PostParam(IFullBuildContext.DRY_RUN, false));
//                            }
//                            return params;
//                        }
//                    };
//            /**=======================================================================
//             *TriggerNewTask
//             =======================================================================*/
//            PowerjobTriggerBuildResult triggerResult = IExecChainContext.triggerNewTask(triggerParams);
//            svc.appendLog(Level.INFO, triggerResult.getTaskid(), Optional.empty(), "start to execute data synchronize pipeline:" + String.valueOf(initNodeCfg));
//
//            WorkflowContext wfContext = context.getWorkflowContext();
//            omsLogger.info("create task context,taskId:{},name:{}", triggerResult.getTaskid(), initNodeCfg.getDataXName());
//            JSONObject iparams = IExecChainContext.createInstanceParams(triggerResult.getTaskid(), () -> initNodeCfg.getDataXName(),
//                    false, Optional.of(triggerResult.getPluginCfgsMetas()));
//            for (Map.Entry<String, Object> e : iparams.entrySet()) {
//                wfContext.appendData2WfContext(e.getKey(), e.getValue());
//            }
//            wfContext.appendData2WfContext(JobParams.KEY_JAVA_MEMORY_SPEC, triggerResult.getJavaMemorySpec());
//            if (triggerResult.getPreviousTaskId() != null) {
//                wfContext.appendData2WfContext(JobParams.KEY_PREVIOUS_TASK_ID, triggerResult.getPreviousTaskId());
//            }
//        } else {
//            // trigger by mannual 手动触发
//            Integer taskId = TISTableDumpProcessor.parseTaskId(instanceParams.getRight());
//            svc.appendLog(Level.INFO, taskId, Optional.empty()
//                    , "start to execute data synchronize pipeline:" + JsonUtil.toString(instanceParams, false));
//        }


    }


}
