package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.datax.executor.BasicTISTableJoinProcessor;
import com.qlangtech.tis.datax.powerjob.impl.PowerJobTaskContext;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/11
 */
public class TISTableJoinProcessor extends BasicTISTableJoinProcessor implements BasicProcessor {

    // public static final String KEY_instanceParams = "instanceParams";
//    private static final Pattern PATTERN_PARTITION_PARAMS =
//            Pattern.compile("^" + ExecChainContextUtils.PARTITION_DATA_PARAMS + "_(.+?)$");
//    transient RpcServiceReference statusRpc;
//
//    private static DataXJoinProcessConsumer createTableJoinConsumer() {
//        DataXJoinProcessConsumer joinProcessConsumer = new DataXJoinProcessConsumer(BasicTISTableDumpProcessor.createSysPramsSuppiler());
//        return joinProcessConsumer;
//    }
//
//    private RpcServiceReference createRpcServiceReference() {
//        if (this.statusRpc != null) {
//            return this.statusRpc;
//        }
//        try {
//            this.statusRpc = StatusRpcClientFactory.getService(ITISCoordinator.create());
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        return this.statusRpc;
//    }


    @Override
    public ProcessResult process(TaskContext context) throws Exception {

        this.process(new PowerJobTaskContext(context));
        return new ProcessResult(true);
//        RpcServiceReference rpcRef = createRpcServiceReference();
//        final OmsLogger logger = context.getOmsLogger();
//        StatusRpcClientFactory.AssembleSvcCompsite feedback = rpcRef.get();
//        //        JSONObject instanceParams = JSONObject.parseObject(context.getInstanceParams());
//        //        DefaultExecContext execContext = IExecChainContext.deserializeInstanceParams(instanceParams);
//        //        execContext.setResType(StoreResourceType.DataFlow);
//        //        execContext.setWorkflowName(execContext.getIndexName());
//        SqlTaskNodeMeta sqlTask =
//                SqlTaskNodeMeta.deserializeTaskNode(ISqlTask.toCfg(JSON.parseObject(context.getJobParams())));
//        DefaultExecContext execContext = createDftExecContent(context);
//        try {
//
//
//            //            IDataxProcessor dataxProc = execContext.getProcessor();
//            //            IPartionableWarehouse partionableWarehouse =
//            //                    IDataxWriter.getPartionableWarehouse(dataxProc.getWriter(null));
//            //
//            //            Matcher matcher = null;
//            //
//            //            for (Map.Entry<String, Object> entry : instanceParams.entrySet()) {
//            //                Object val = entry.getValue();
//            //                try {
//            //                    matcher = PATTERN_PARTITION_PARAMS.matcher(entry.getKey());
//            //                    if (matcher.matches()) {
//            //                        execContext.putTablePt(EntityName.parse(matcher.group(1)),
//            //                                () -> partionableWarehouse.getPsFormat().format(Long.parseLong(String
//            //                                .valueOf(val))));
//            //                    }
//            //                } catch (Exception e) {
//            //                    throw new RuntimeException("key:" + entry.getKey() + ",val:" + entry.getValue(), e);
//            //                }
//            //            }
//            if (TisAppLaunch.isTestMock()) {
//                DataXJoinProcessExecutor.executeJoin(feedback, execContext, sqlTask);
//            } else {
//                // 另外单独启一个进程来执行
//                DataXJoinProcessConsumer tableJoinConsumer = createTableJoinConsumer();
//
//                WorkflowHookMsg wfHookMsg = WorkflowHookMsg.create(sqlTask, execContext, sqlTask.getExportName());
//
//                tableJoinConsumer.consumeMessage(wfHookMsg);
//            }
//            TISTableDumpProcessor.addSuccessPartition(context, execContext, sqlTask.getExportName());
//            return new ProcessResult(true);
//
//        } catch (StatusRuntimeException e) {
//            rpcRef.reConnect();
//            throw e;
//        } catch (Exception e) {
//            Throwable rootCause = ExceptionUtils.getRootCause(e);
//
//            RpcUtils.setJoinStatus(execContext.getTaskId(), true, true, feedback, sqlTask.getExportName());
//
//            feedback.appendLog(LoggingEvent.Level.ERROR, execContext.getTaskId(), Optional.empty(),
//                    rootCause != null ? ExceptionUtils.getStackTrace(rootCause) : ExceptionUtils.getStackTrace(e));
//            throw new RuntimeException(e);
//        }

    }

//    private DefaultExecContext createDftExecContent(TaskContext context) {
//        JSONObject instanceParams = JSONObject.parseObject(context.getInstanceParams());
//        final CfgsSnapshotConsumer snapshotConsumer = new CfgsSnapshotConsumer();
//        DefaultExecContext execContext = IExecChainContext.deserializeInstanceParams(instanceParams, (ctx) -> {
//            ctx.setResType(StoreResourceType.DataFlow);
//            ctx.setWorkflowName(ctx.getIndexName());
//            ctx.setExecutePhaseRange(new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN));
//        }, snapshotConsumer);
//
//
//        snapshotConsumer.synchronizTpisAndConfs(execContext, TISTableDumpProcessor.cacheSnaphsot);
//
//        IDataxProcessor dataxProc = execContext.getProcessor();
//        IPartionableWarehouse partionableWarehouse = IDataxWriter.getPartionableWarehouse(dataxProc.getWriter(null));
//
//        Matcher matcher = null;
//
//        for (Map.Entry<String, Object> entry : instanceParams.entrySet()) {
//            Object val = entry.getValue();
//            try {
//                matcher = PATTERN_PARTITION_PARAMS.matcher(entry.getKey());
//                if (matcher.matches()) {
//                    execContext.putTablePt(EntityName.parse(matcher.group(1)),
//                            () -> partionableWarehouse.getPsFormat().format(Long.parseLong(String.valueOf(val))));
//                }
//            } catch (Exception e) {
//                throw new RuntimeException("key:" + entry.getKey() + ",val:" + entry.getValue(), e);
//            }
//        }
//
//        return execContext;
//    }


}
