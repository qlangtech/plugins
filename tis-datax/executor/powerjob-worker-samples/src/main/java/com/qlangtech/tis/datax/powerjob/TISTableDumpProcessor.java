package com.qlangtech.tis.datax.powerjob;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobRunEnvironmentParamsSetter;
import com.qlangtech.tis.datax.DataXLifecycleHookMsg;
import com.qlangtech.tis.datax.DataxPrePostConsumer;
import com.qlangtech.tis.datax.IDataXBatchPost;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.RpcUtils;
import com.qlangtech.tis.datax.join.DataXJoinProcessConsumer;
import com.qlangtech.tis.exec.DefaultExecContext;
import com.qlangtech.tis.exec.ExecChainContextUtils;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.PluginAndCfgSnapshotLocalCache;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.ds.DefaultTab;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.powerjob.SelectedTabTriggers.SelectedTabTriggersConfig;
import com.qlangtech.tis.rpc.grpc.log.appender.LoggingEvent;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.web.start.TisAppLaunch;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import com.tis.hadoop.rpc.StatusRpcClientFactory.AssembleSvcCompsite;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.TaskResult;
import tech.powerjob.worker.core.processor.sdk.MapReduceProcessor;
import tech.powerjob.worker.log.OmsLogger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/11
 */
public class TISTableDumpProcessor implements MapReduceProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TISTableDumpProcessor.class);
    // public static final String KEY_instanceParams = "instanceParams";
    public transient static final PluginAndCfgSnapshotLocalCache cacheSnaphsot = new PluginAndCfgSnapshotLocalCache();


    @Override
    public ProcessResult reduce(TaskContext context, List<TaskResult> taskResults) {
        final OmsLogger omsLogger = context.getOmsLogger();
        if (CollectionUtils.isEmpty(taskResults)) {
            return new ProcessResult(false, "taskResults is empty,terminate");
        }
        for (TaskResult childResult : taskResults) {
            if (!childResult.isSuccess()) {
                return new ProcessResult(false,
                        "childResult faild:" + childResult.getResult() + ",taskid:" + childResult.getTaskId() + "  " + "skip reduce phase");
            }
        }


        RpcServiceReference statusRpc = getRpcServiceReference();
        StatusRpcClientFactory.AssembleSvcCompsite svc = statusRpc.get();
        Triple<DefaultExecContext, CfgsSnapshotConsumer, SelectedTabTriggers.SelectedTabTriggersConfig> pair = createExecContext(context, ExecPhase.Reduce);

        DefaultExecContext execContext = Objects.requireNonNull(pair.getLeft(), "execContext can not be null");
        SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg = pair.getRight();
        // execContext.putTablePt( );
        //  IDataxProcessor processor = execContext.getProcessor(); // DataxProcessor.load(null, triggerCfg
        // .getDataXName());
        ISelectedTab tab = new DefaultTab(triggerCfg.getTabName());
        String postTrigger = null;
        Integer taskId = execContext.getTaskId();
        if (StringUtils.isNotEmpty(postTrigger = triggerCfg.getPostTrigger())) {

            try {
                RpcUtils.setJoinStatus(taskId, false, false, svc, postTrigger);
                omsLogger.info("exec postTrigger:{}", postTrigger);

                IRemoteTaskTrigger postTask = createDataXJob(execContext, Pair.of(postTrigger,
                        IDataXBatchPost.LifeCycleHook.Post), tab.getName());
                postTask.run();
                RpcUtils.setJoinStatus(taskId, true, false, svc, postTrigger);
            } catch (Exception e) {
                RpcUtils.setJoinStatus(taskId, true, true, svc, postTrigger);
                //  markFaildToken(context);
                omsLogger.error("postTrigger:" + postTrigger + " falid", e);
                //throw new RuntimeException(e);
                return new ProcessResult(false, e.getMessage());
            }
        }

        addSuccessPartition(context, execContext, tab.getName());

        return new ProcessResult(true);
    }

    public static void addSuccessPartition(TaskContext context, DefaultExecContext execContext, String entityName) {
        Objects.requireNonNull(context.getWorkflowContext(), "workflowContext can not be null") //
                .appendData2WfContext( //
                        ExecChainContextUtils.PARTITION_DATA_PARAMS + "_" + entityName,
                        execContext.getPartitionTimestampWithMillis());
    }


    @Override
    public ProcessResult process(TaskContext context) throws Exception {


        final OmsLogger logger = context.getOmsLogger();
        ExecPhase execPhase = ExecPhase.parse(this, context);
        RpcServiceReference statusRpc = getRpcServiceReference();
        /**
         *
         * 同步远端resource 资源
         */
        Triple<DefaultExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig> pair = createExecContext(context, execPhase);

        StatusRpcClientFactory.AssembleSvcCompsite svc = null;
//        if (pair.getMiddle().getCfgsSnapshotWhenSuccessSync() != null) {
//            this.cacheSnaphsot = pair.getMiddle().getCfgsSnapshotWhenSuccessSync();
//        }

        try {
            svc = statusRpc.get();


            final DefaultExecContext execChainContext = Objects.requireNonNull(pair.getLeft(),
                    "execChainContext can " + "not be null");
            final SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg = pair.getRight();

            ISelectedTab tab = new DefaultTab(triggerCfg.getTabName());

            // IDataxProcessor processors = DataxProcessor.load(null, triggerCfg.getResType(), triggerCfg.getDataXName());

            if (isRootTask()) {

                // L1. 执行根任务
                String preTrigger = triggerCfg.getPreTrigger();
                if (StringUtils.isNotEmpty(preTrigger)) {
                    try {

                        svc.reportDumpJobStatus(false, false, false, execChainContext.getTaskId(), preTrigger, -1, -1);
                        logger.info("exec preTrigger:{}", preTrigger);


                        IRemoteTaskTrigger previousTrigger = createDataXJob(execChainContext, Pair.of(preTrigger,
                                IDataXBatchPost.LifeCycleHook.Prep), tab.getName());

                        previousTrigger.run();
                        svc.reportDumpJobStatus(false, true, false, execChainContext.getTaskId(), preTrigger, -1, -1);
                    } catch (Exception e) {
                        logger.error("pretrigger:" + preTrigger + " faild", e);
                        svc.reportDumpJobStatus(true, true, false, execChainContext.getTaskId(), preTrigger, -1, -1);

                        reportError(e, execChainContext, svc);

                        return new ProcessResult(false, e.getMessage());
                    }
                }
                try {
                    // List<SplitTabSync> splitTabsSync = Lists.newArrayList();
                    for (CuratorDataXTaskMessage tskMsg : triggerCfg.getSplitTabsCfg()) {
                        //  splitTabsSync.add();

                        ProcessResult result
                                = executeSplitTabSync(logger, statusRpc, svc, execChainContext, new SplitTabSync(tskMsg));
                        if (!result.isSuccess()) {
                            return result;
                        }
                    }

                    // map(splitTabsSync, triggerCfg.getTabName() + "Mapper");
                    /**
                     * 由于powerjob 的map任务执行有问题，先把 map阶段执行的任务，都在初始化阶段执行了
                     */
                    map(Collections.emptyList(), triggerCfg.getTabName() + "Mapper");

                    return new ProcessResult(true, "map success");
                } catch (Exception e) {
                    reportError(e, execChainContext, svc);
                    return new ProcessResult(false, e.getMessage());
                }
            } else if (context.getSubTask() instanceof SplitTabSync) {
                SplitTabSync tabSync = (SplitTabSync) context.getSubTask();
                return executeSplitTabSync(logger, statusRpc, svc, execChainContext, tabSync);
            }


            return new ProcessResult(false, "UNKNOWN_TYPE_OF_SUB_TASK");
        } finally {
        }
    }

    private ProcessResult executeSplitTabSync(OmsLogger logger, RpcServiceReference statusRpc
            , AssembleSvcCompsite svc, DefaultExecContext execChainContext, SplitTabSync tabSync) {
        try {

            tabSync.execSync(execChainContext, statusRpc);
            return new ProcessResult(true, "table split sync:" + tabSync.tskMsg.getJobName() + ",task " +
                    "serial:" + tabSync.tskMsg.getTaskSerializeNum());
        } catch (Exception e) {
            logger.error("spilt table sync job:" + tabSync.tskMsg.getJobName() + " faild", e);
            reportError(e, execChainContext, svc);
            return new ProcessResult(false, e.getMessage());
        }
    }

    private static void reportError(Exception e, DefaultExecContext execChainContext, StatusRpcClientFactory.AssembleSvcCompsite svc) {

        Throwable rootCause = ExceptionUtils.getRootCause(e);
        svc.appendLog(LoggingEvent.Level.ERROR, execChainContext.getTaskId(), Optional.empty(),
                rootCause != null ? ExceptionUtils.getStackTrace(rootCause) : ExceptionUtils.getStackTrace(e));
    }

    private IRemoteTaskTrigger createDataXJob(DefaultExecContext execContext, Pair<String,
            IDataXBatchPost.LifeCycleHook> lifeCycleHookInfo, String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("param tableName can not be null");
        }
        IDataxProcessor processor = execContext.getProcessor();

        if (TisAppLaunch.isTestMock()) {
            IDataXBatchPost dataXBatchPost = getDataXBatchPost(execContext.getProcessor());
            IDataXBatchPost.LifeCycleHook cycleHook = lifeCycleHookInfo.getRight();
            if (cycleHook == IDataXBatchPost.LifeCycleHook.Post) {
                //IExecChainContext execContext, ISelectedTab tab, DataXCfgGenerator.GenerateCfgs cfgFileNames
                return dataXBatchPost.createPostTask(execContext, new DefaultTab(tableName),
                        processor.getDataxCfgFileNames(null,Optional.empty()));
            } else if (cycleHook == IDataXBatchPost.LifeCycleHook.Prep) {
                return dataXBatchPost.createPreExecuteTask(execContext, new DefaultTab(tableName));
            } else {
                throw new IllegalArgumentException("cycleHook:" + cycleHook);
            }
        }


        DataxPrePostConsumer prePostConsumer = createPrePostConsumer();

        DataXLifecycleHookMsg lifecycleHookMsg = DataXLifecycleHookMsg.createDataXLifecycleHookMsg(processor,
                tableName, execContext.getTaskId(), lifeCycleHookInfo.getKey(),
                execContext.getPartitionTimestampWithMillis(), lifeCycleHookInfo.getValue(), execContext.isDryRun());

        return new IRemoteTaskTrigger() {
            @Override
            public String getTaskName() {
                return Objects.requireNonNull(lifeCycleHookInfo.getKey(), "task name can not be null");
            }

            @Override
            public void run() {
                try {
                    prePostConsumer.consumeMessage(lifecycleHookMsg);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static DataxPrePostConsumer createPrePostConsumer() {
        DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler systemPramsSuppiler = createSysPramsSuppiler();
        DataxPrePostConsumer prePostConsumer = new DataxPrePostConsumer(systemPramsSuppiler);
        return prePostConsumer;
    }

    public static DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler createSysPramsSuppiler() {
        return DataXJobRunEnvironmentParamsSetter.createSysPramsSuppiler();
    }

    public static DataXJoinProcessConsumer createTableJoinConsumer() {
        DataXJoinProcessConsumer joinProcessConsumer = new DataXJoinProcessConsumer(createSysPramsSuppiler());
        return joinProcessConsumer;
    }


    /**
     * Pair<Boolean: 是否获取到必要的taskId 参数？没有则说明是定时任务触发, JSONObject: InstanceParams>
     *
     * @param context
     * @return
     */
    public static Pair<Boolean, JSONObject> getInstanceParams(TaskContext context) {
        JSONObject instanceParams = JSONObject.parseObject(context.getInstanceParams());
        return Pair.of((MapUtils.isNotEmpty(instanceParams) && instanceParams.getInteger(JobParams.KEY_TASK_ID) != null), instanceParams);
    }

    /**
     * initialize 节点之后执行的任务节点
     *
     * @param context
     * @return
     * @throws InstanceParamsException
     */
    public static Triple<DefaultExecContext, CfgsSnapshotConsumer, SelectedTabTriggers.SelectedTabTriggersConfig>
    createExecContext(TaskContext context, ExecPhase execPhase) {
        JSONObject instanceParams = null;
        Pair<Boolean, JSONObject> instanceParamsGetter = getInstanceParams(context);
        instanceParams = instanceParamsGetter.getRight();
        if (!instanceParamsGetter.getLeft()) {
            //            Map<String, String> appendedWfData = context.getWorkflowContext().getAppendedContextData();
            //            instanceParams = JSONObject.parseObject(appendedWfData.get(KEY_instanceParams));
            throw new IllegalStateException("instanceParams is illegal:" + JsonUtil.toString(instanceParams, true));
        }

        Integer taskId = parseTaskId(instanceParams);
        Triple<DefaultExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig> pair
                = createExecContext(context, taskId, instanceParams);

        SelectedTabTriggersConfig triggerCfg = pair.getRight();
        logger.info("tabName:" + triggerCfg.getTabName() + ",phase:" + execPhase
                + ",splitTabsCfgs:"
                + triggerCfg.getSplitTabsCfg().stream().map((msg) -> msg.getJobName()).collect(Collectors.joining(",")));

        return pair;
    }

    public static Integer parseTaskId(JSONObject instanceParams) {
        Integer taskId = Objects.requireNonNull(instanceParams.getInteger(JobParams.KEY_TASK_ID),
                JobParams.KEY_TASK_ID + " can not be null," + JsonUtil.toString(instanceParams));
        return taskId;
    }


    private static Triple<DefaultExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig>  //
    createExecContext(TaskContext context, Integer taskId, JSONObject instanceParams) {
        if (taskId == null) {
            throw new IllegalArgumentException("param taskId can not be null");
        }

        SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg = getTriggerCfg(context);

        final CfgsSnapshotConsumer snapshotConsumer = new CfgsSnapshotConsumer();
        DefaultExecContext execContext = IExecChainContext.deserializeInstanceParams(instanceParams, (ctx) -> {


            ctx.setLatestPhaseStatusCollection(cacheSnaphsot.getPreviousStatus(ctx.getTaskId(), () -> {
                Integer prevTaskId = instanceParams.getInteger(JobParams.KEY_PREVIOUS_TASK_ID);
                if (prevTaskId == null) {
                    return null;
                }
                AssembleSvcCompsite svc = getRpcServiceReference().get();
                return svc.statReceiveSvc.loadPhaseStatusFromLatest(prevTaskId);
            }));

        }, snapshotConsumer);
        execContext.setResType(Objects.requireNonNull(triggerCfg.getResType()));
        if (triggerCfg.getResType() == StoreResourceType.DataFlow) {
            execContext.setWorkflowName(triggerCfg.getDataXName());
        }


        snapshotConsumer.synchronizTpisAndConfs(execContext, cacheSnaphsot);

        Long triggerTimestamp = execContext.getPartitionTimestampWithMillis();// instanceParams.getLong(DataxUtils
        // .EXEC_TIMESTAMP);
        System.setProperty(DataxUtils.EXEC_TIMESTAMP, String.valueOf(triggerTimestamp));

        //        Integer taskId = instanceParams.getInteger(JobParams.KEY_TASK_ID);
        //        if (taskId == null) {
        //            // 说明是定时任务触发
        //            throw new InstanceParamsException(JobParams.KEY_TASK_ID + " can not be null," + JsonUtil
        //            .toString(instanceParams));
        //        }
        //boolean dryRun = instanceParams.getBooleanValue(IFullBuildContext.DRY_RUN);
        for (CuratorDataXTaskMessage tskMsg : triggerCfg.getSplitTabsCfg()) {
            tskMsg.setExecTimeStamp(triggerTimestamp);
        }
        // String dataXName = triggerCfg.getDataXName();
        // DefaultExecContext execChainContext = new DefaultExecContext(dataXName, triggerTimestamp);
        //        execChainContext.setCoordinator(ITISCoordinator.create());
        //        execChainContext.setDryRun(dryRun);
        //        execChainContext.setAttribute(JobCommon.KEY_TASK_ID, taskId);
        triggerCfg.getSplitTabsCfg().forEach((tskMsg) -> {
            tskMsg.setJobId(taskId);
        });


        return Triple.of(execContext, snapshotConsumer, triggerCfg);
    }


    private static transient RpcServiceReference statusRpc;

    public static RpcServiceReference getRpcServiceReference() {

        if (statusRpc != null) {
            return statusRpc;
        }
        try {
            statusRpc = StatusRpcClientFactory.getService(ITISCoordinator.create());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return statusRpc;
    }


    private static IDataXBatchPost getDataXBatchPost(IDataxProcessor processor) {
        IDataxWriter writer = processor.getWriter(null);
        return IDataxWriter.castBatchPost(writer);
    }

    private static SelectedTabTriggers.SelectedTabTriggersConfig getTriggerCfg(TaskContext context) {
        SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg =
                SelectedTabTriggers.deserialize(JSONObject.parseObject(context.getJobParams()));
        return triggerCfg;
    }


}
