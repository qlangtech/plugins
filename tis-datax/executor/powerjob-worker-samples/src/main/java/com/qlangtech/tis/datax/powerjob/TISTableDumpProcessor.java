package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.IDataXTaskRelevant;
import com.qlangtech.tis.datax.executor.BasicTISTableDumpProcessor;
import com.qlangtech.tis.datax.powerjob.impl.PowerJobTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.TaskResult;
import tech.powerjob.worker.core.processor.sdk.MapReduceProcessor;

import java.io.File;
import java.util.List;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/11
 */
public class TISTableDumpProcessor extends BasicTISTableDumpProcessor implements MapReduceProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TISTableDumpProcessor.class);

    // public static final String KEY_instanceParams = "instanceParams";
    static {
        setDataXExecutorDir();
    }

    static void setDataXExecutorDir() {
        DataXJobInfo.dataXExecutorDir.set(new File("/opt/tis/" + IDataXTaskRelevant.KEY_TIS_DATAX_EXECUTOR));
    }


    @Override
    public ProcessResult reduce(TaskContext context, List<TaskResult> taskResults) {

        this.processPostTask(new PowerJobTaskContext(context));
        return new ProcessResult(true);
//        final OmsLogger omsLogger = context.getOmsLogger();
//        if (CollectionUtils.isEmpty(taskResults)) {
//            return new ProcessResult(false, "taskResults is empty,terminate");
//        }
//        for (TaskResult childResult : taskResults) {
//            if (!childResult.isSuccess()) {
//                return new ProcessResult(false,
//                        "childResult faild:" + childResult.getResult() + ",taskid:" + childResult.getTaskId() + "  " + "skip reduce phase");
//            }
//        }
//
//
//        RpcServiceReference statusRpc = getRpcServiceReference();
//        StatusRpcClientFactory.AssembleSvcCompsite svc = statusRpc.get();
//        Triple<DefaultExecContext, CfgsSnapshotConsumer, SelectedTabTriggers.SelectedTabTriggersConfig> pair = createExecContext(context, ExecPhase.Reduce);
//
//        DefaultExecContext execContext = Objects.requireNonNull(pair.getLeft(), "execContext can not be null");
//        SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg = pair.getRight();
//        // execContext.putTablePt( );
//        //  IDataxProcessor processor = execContext.getProcessor(); // DataxProcessor.load(null, triggerCfg
//        // .getDataXName());
//        ISelectedTab tab = new DefaultTab(triggerCfg.getTabName());
//        String postTrigger = null;
//        Integer taskId = execContext.getTaskId();
//        if (StringUtils.isNotEmpty(postTrigger = triggerCfg.getPostTrigger())) {
//
//            try {
//                RpcUtils.setJoinStatus(taskId, false, false, svc, postTrigger);
//                omsLogger.info("exec postTrigger:{}", postTrigger);
//
//                IRemoteTaskTrigger postTask = createDataXJob(execContext, Pair.of(postTrigger,
//                        IDataXBatchPost.LifeCycleHook.Post), tab.getName());
//                postTask.run();
//                RpcUtils.setJoinStatus(taskId, true, false, svc, postTrigger);
//            } catch (Exception e) {
//                RpcUtils.setJoinStatus(taskId, true, true, svc, postTrigger);
//                //  markFaildToken(context);
//                omsLogger.error("postTrigger:" + postTrigger + " falid", e);
//                //throw new RuntimeException(e);
//                return new ProcessResult(false, e.getMessage());
//            }
//        }
//
//        addSuccessPartition(context, execContext, tab.getName());
//
//        return new ProcessResult(true);
    }


    @Override
    public ProcessResult process(TaskContext context) throws Exception {

        // PowerJobTaskContext powerJobTaskContext = ;

        // final OmsLogger logger = context.getOmsLogger();
        //  ExecPhase execPhase = ;

        processSync(new PowerJobTaskContext(context), PowerJobTaskContext.parse(this, context));
        return new ProcessResult(true, "map success");
//        RpcServiceReference statusRpc = getRpcServiceReference();
//        /**
//         *
//         * 同步远端resource 资源
//         */
//        Triple<DefaultExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig> pair = createExecContext(context, execPhase);
//
//        StatusRpcClientFactory.AssembleSvcCompsite svc = null;
////        if (pair.getMiddle().getCfgsSnapshotWhenSuccessSync() != null) {
////            this.cacheSnaphsot = pair.getMiddle().getCfgsSnapshotWhenSuccessSync();
////        }
//
//        try {
//            svc = statusRpc.get();
//
//
//            final DefaultExecContext execChainContext = Objects.requireNonNull(pair.getLeft(),
//                    "execChainContext can " + "not be null");
//            final SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg = pair.getRight();
//
//            ISelectedTab tab = new DefaultTab(triggerCfg.getTabName());
//
//            // IDataxProcessor processors = DataxProcessor.load(null, triggerCfg.getResType(), triggerCfg.getDataXName());
//
//            if (isRootTask()) {
//
//                // L1. 执行根任务
//                String preTrigger = triggerCfg.getPreTrigger();
//                if (StringUtils.isNotEmpty(preTrigger)) {
//                    try {
//
//                        svc.reportDumpJobStatus(false, false, false, execChainContext.getTaskId(), preTrigger, -1, -1);
//                        logger.info("exec preTrigger:{}", preTrigger);
//
//
//                        IRemoteTaskTrigger previousTrigger = createDataXJob(execChainContext, Pair.of(preTrigger,
//                                IDataXBatchPost.LifeCycleHook.Prep), tab.getName());
//
//                        previousTrigger.run();
//                        svc.reportDumpJobStatus(false, true, false, execChainContext.getTaskId(), preTrigger, -1, -1);
//                    } catch (Exception e) {
//                        logger.error("pretrigger:" + preTrigger + " faild", e);
//                        svc.reportDumpJobStatus(true, true, false, execChainContext.getTaskId(), preTrigger, -1, -1);
//
//                        reportError(e, execChainContext, svc);
//
//                        return new ProcessResult(false, e.getMessage());
//                    }
//                }
//                try {
//                    // List<SplitTabSync> splitTabsSync = Lists.newArrayList();
//                    for (CuratorDataXTaskMessage tskMsg : triggerCfg.getSplitTabsCfg()) {
//                        //  splitTabsSync.add();
//
//                        ProcessResult result
//                                = executeSplitTabSync(logger, statusRpc, svc, execChainContext, new SplitTabSync(tskMsg));
//                        if (!result.isSuccess()) {
//                            return result;
//                        }
//                    }
//
//                    // map(splitTabsSync, triggerCfg.getTabName() + "Mapper");
//                    /**
//                     * 由于powerjob 的map任务执行有问题，先把 map阶段执行的任务，都在初始化阶段执行了
//                     */
//                    map(Collections.emptyList(), triggerCfg.getTabName() + "Mapper");
//
//                    return new ProcessResult(true, "map success");
//                } catch (Exception e) {
//                    reportError(e, execChainContext, svc);
//                    return new ProcessResult(false, e.getMessage());
//                }
//            } else if (context.getSubTask() instanceof SplitTabSync) {
//                SplitTabSync tabSync = (SplitTabSync) context.getSubTask();
//                return executeSplitTabSync(logger, statusRpc, svc, execChainContext, tabSync);
//            }
//
//
//            return new ProcessResult(false, "UNKNOWN_TYPE_OF_SUB_TASK");
//        } finally {
//        }
    }


}
