package com.qlangtech.tis.datax.powerjob;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobRunEnvironmentParamsSetter;
import com.qlangtech.tis.datax.DataXLifecycleHookMsg;
import com.qlangtech.tis.datax.DataxPrePostConsumer;
import com.qlangtech.tis.datax.IDataXBatchPost;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.RpcUtils;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.join.DataXJoinProcessConsumer;
import com.qlangtech.tis.exec.DefaultExecContext;
import com.qlangtech.tis.exec.ExecChainContextUtils;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.ds.DefaultTab;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.pubhook.common.RunEnvironment;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.web.start.TisAppLaunch;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.TaskResult;
import tech.powerjob.worker.core.processor.sdk.MapReduceProcessor;
import tech.powerjob.worker.log.OmsLogger;

import java.util.List;
import java.util.Objects;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/11
 */
public class TISTableDumpProcessor implements MapReduceProcessor {

    // public static final String KEY_instanceParams = "instanceParams";


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
        Pair<DefaultExecContext, SelectedTabTriggers.SelectedTabTriggersConfig> pair = createExecContext(context);
        RpcServiceReference statusRpc = createRpcServiceReference();
        StatusRpcClientFactory.AssembleSvcCompsite svc = statusRpc.get();

        DefaultExecContext execContext = Objects.requireNonNull(pair.getKey(), "execContext can not be null");
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

        Pair<DefaultExecContext, SelectedTabTriggers.SelectedTabTriggersConfig> pair = createExecContext(context);
        RpcServiceReference statusRpc = createRpcServiceReference();
        StatusRpcClientFactory.AssembleSvcCompsite svc = null;

        try {
            svc = statusRpc.get();

            final DefaultExecContext execChainContext = Objects.requireNonNull(pair.getKey(),
                    "execChainContext can " + "not be null");
            final SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg = pair.getRight();

            ISelectedTab tab = new DefaultTab(triggerCfg.getTabName());

            IDataxProcessor processor = DataxProcessor.load(null, triggerCfg.getResType(), triggerCfg.getDataXName());

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
                        return new ProcessResult(false, e.getMessage());
                    }
                }

                List<SplitTabSync> splitTabsSync = Lists.newArrayList();
                for (CuratorDataXTaskMessage tskMsg : triggerCfg.getSplitTabsCfg()) {
                    splitTabsSync.add(new SplitTabSync(tskMsg));
                }
                try {
                    map(splitTabsSync, "LEVEL1_TASK_A");
                    return new ProcessResult(true, "map success");
                } catch (Exception e) {
                    return new ProcessResult(false, e.getMessage());
                }
            }


            if (context.getSubTask() instanceof SplitTabSync) {
                SplitTabSync tabSync = (SplitTabSync) context.getSubTask();
                try {

                    tabSync.execSync(execChainContext, statusRpc, processor);
                    return new ProcessResult(true, "table split sync:" + tabSync.tskMsg.getJobName() + ",task " +
                            "serial:" + tabSync.tskMsg.getTaskSerializeNum());
                } catch (Exception e) {
                    logger.error("spilt table sync job:" + tabSync.tskMsg.getJobName() + " faild", e);
                    return new ProcessResult(false, e.getMessage());
                }
            }


            return new ProcessResult(false, "UNKNOWN_TYPE_OF_SUB_TASK");
        } finally {
        }
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
                        processor.getDataxCfgFileNames(null));
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
//        DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler systemPramsSuppiler =
//                new DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler() {
//            @Override
//            public List<String> get() {
//                List<String> params = Lists.newArrayList(super.get());
//                params.add("-D" + Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS + "=true");
//                params.add("-D" + Config.KEY_ASSEMBLE_HOST + "=" + Config.getAssembleHost());
//                params.add("-D" + Config.KEY_TIS_HOST + "=" + Config.getTisHost());
//                params.add("-D" + Config.KEY_RUNTIME + "=" + RunEnvironment.getSysRuntime().getKeyName());
//                //  params.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=50005");
//                return params;
//            }
//        };
//        return systemPramsSuppiler;
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
    public static Pair<DefaultExecContext, SelectedTabTriggers.SelectedTabTriggersConfig> createExecContext(TaskContext context) {
        JSONObject instanceParams = null;
        Pair<Boolean, JSONObject> instanceParamsGetter = getInstanceParams(context);
        instanceParams = instanceParamsGetter.getRight();
        if (!instanceParamsGetter.getLeft()) {
            //            Map<String, String> appendedWfData = context.getWorkflowContext().getAppendedContextData();
            //            instanceParams = JSONObject.parseObject(appendedWfData.get(KEY_instanceParams));
            throw new IllegalStateException("instanceParams is illegal:" + JsonUtil.toString(instanceParams, true));
        }

        Integer taskId = Objects.requireNonNull(instanceParams.getInteger(JobParams.KEY_TASK_ID),
                JobParams.KEY_TASK_ID + " can not be null," + JsonUtil.toString(instanceParams));
        return createExecContext(context, taskId, instanceParams);
    }


    private static Pair<DefaultExecContext, SelectedTabTriggers.SelectedTabTriggersConfig>  //
    createExecContext(TaskContext context, Integer taskId, JSONObject instanceParams) {
        if (taskId == null) {
            throw new IllegalArgumentException("param taskId can not be null");
        }
        SelectedTabTriggers.SelectedTabTriggersConfig triggerCfg = getTriggerCfg(context);

        //        JSONObject instanceParamss = JSONObject.parseObject(context.getInstanceParams());
        //        if (instanceParams == null) {
        //            throw new InstanceParamsException("instanceParams can not be null");
        //        }
        final CfgsSnapshotConsumer snapshotConsumer = new CfgsSnapshotConsumer();
        DefaultExecContext execContext = IExecChainContext.deserializeInstanceParams(instanceParams, snapshotConsumer);
        execContext.setResType(Objects.requireNonNull(triggerCfg.getResType()));
        if (triggerCfg.getResType() == StoreResourceType.DataFlow) {
            execContext.setWorkflowName(triggerCfg.getDataXName());
        }

        snapshotConsumer.synchronizTpisAndConfs(execContext);

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


        return Pair.of(execContext, triggerCfg);
    }


    transient RpcServiceReference statusRpc;

    private RpcServiceReference createRpcServiceReference() {
        if (this.statusRpc != null) {
            return this.statusRpc;
        }
        try {
            this.statusRpc = StatusRpcClientFactory.getService(ITISCoordinator.create());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this.statusRpc;
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
