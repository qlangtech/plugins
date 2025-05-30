/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.datax.executor;

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
import com.qlangtech.tis.datax.powerjob.CfgsSnapshotConsumer;
import com.qlangtech.tis.datax.powerjob.ExecPhase;
import com.qlangtech.tis.datax.powerjob.SplitTabSync;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.exec.ExecChainContextUtils;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.PluginAndCfgSnapshotLocalCache;
import com.qlangtech.tis.plugin.ds.DefaultTab;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.powerjob.SelectedTabTriggersConfig;
import com.qlangtech.tis.rpc.grpc.log.ILoggerAppenderClient.LogLevel;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.web.start.TisAppLaunch;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-18 19:22
 * // @see com.qlangtech.tis.datax.powerjob.TISTableDumpProcessor
 **/
public class BasicTISTableDumpProcessor {

    private static final Logger logger = LoggerFactory.getLogger(BasicTISTableDumpProcessor.class);
    public transient static final PluginAndCfgSnapshotLocalCache cacheSnaphsot = new PluginAndCfgSnapshotLocalCache();

    /**
     * Pair<Boolean: 是否获取到必要的taskId 参数？没有则说明是定时任务触发, JSONObject: InstanceParams>
     *
     * @param context
     * @return
     */
    public static Pair<Boolean, JSONObject> getInstanceParams(ITaskExecutorContext context) {
        JSONObject instanceParams = (context.getInstanceParams());
        return Pair.of((MapUtils.isNotEmpty(instanceParams) && instanceParams.getInteger(JobParams.KEY_TASK_ID) != null), instanceParams);
    }


    public void processPostTask(ITaskExecutorContext context) {

        RpcServiceReference svc = getRpcServiceReference();
        // StatusRpcClientFactory.AssembleSvcCompsite svc = statusRpc.get();
        Triple<AbstractExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig> pair = createExecContext(context, ExecPhase.Reduce);

        AbstractExecContext execContext = Objects.requireNonNull(pair.getLeft(), "execContext can not be null");
        SelectedTabTriggersConfig triggerCfg = pair.getRight();

        ISelectedTab tab = new DefaultTab(triggerCfg.getTabName());
        String postTrigger = null;
        Integer taskId = execContext.getTaskId();
        if (StringUtils.isNotEmpty(postTrigger = triggerCfg.getPostTrigger())) {

            try {
                RpcUtils.setJoinStatus(taskId, false, false, svc, postTrigger);
                context.infoLog("exec postTrigger:{}", postTrigger);

                IRemoteTaskTrigger postTask = createDataXJob(execContext, Pair.of(postTrigger,
                        IDataXBatchPost.LifeCycleHook.Post), tab.getName());
                postTask.run();
                RpcUtils.setJoinStatus(taskId, true, false, svc, postTrigger);
            } catch (Exception e) {
                RpcUtils.setJoinStatus(taskId, true, true, svc, postTrigger);
                context.errorLog("postTrigger:" + postTrigger + " falid", e);
                throw new RuntimeException("postTrigger:" + postTrigger + " falid", e);
            }
        }

        addSuccessPartition(context, execContext, tab.getName());
    }


    /**
     * initialize 节点之后执行的任务节点
     *
     * @param context
     * @return //@throws com.qlangtech.tis.datax.powerjob.InstanceParamsException
     */
    public static Triple<AbstractExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig>
    createExecContext(ITaskExecutorContext context, ExecPhase execPhase) {
        JSONObject instanceParams = null;
        Pair<Boolean, JSONObject> instanceParamsGetter = getInstanceParams(context);
        instanceParams = instanceParamsGetter.getRight();
        if (!instanceParamsGetter.getLeft()) {
            throw new IllegalStateException("instanceParams is illegal:" + JsonUtil.toString(instanceParams, true));
        }

        Integer taskId = BasicTISInitializeProcessor.parseTaskId(instanceParams);
        Triple<AbstractExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig> pair
                = createExecContext(context, taskId, instanceParams);

        SelectedTabTriggersConfig triggerCfg = pair.getRight();
        logger.info("tabName:" + triggerCfg.getTabName() + ",phase:" + execPhase
                + ",splitTabsCfgs:"
                + triggerCfg.getSplitTabsCfg().stream().map((msg) -> msg.getJobName()).collect(Collectors.joining(",")));

        return pair;
    }

    private static Triple<AbstractExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig>  //
    createExecContext(ITaskExecutorContext context, Integer taskId, JSONObject instanceParams) {
        if (taskId == null) {
            throw new IllegalArgumentException("param taskId can not be null");
        }

        SelectedTabTriggersConfig triggerCfg = getTriggerCfg(context);

        final CfgsSnapshotConsumer snapshotConsumer = new CfgsSnapshotConsumer();
        AbstractExecContext execContext = IExecChainContext.deserializeInstanceParams(triggerCfg, instanceParams, (ctx) -> {
            ctx.setLatestPhaseStatusCollection(cacheSnaphsot.getPreviousStatus(ctx.getTaskId(), () -> {
                Integer prevTaskId = instanceParams.getInteger(JobParams.KEY_PREVIOUS_TASK_ID);
                if (prevTaskId == null) {
                    return null;
                }
                return getRpcServiceReference().loadPhaseStatusFromLatest(prevTaskId);
            }));

        }, snapshotConsumer);

        execContext.setSpecifiedLocalLoggerPath(context.getSpecifiedLocalLoggerPath());
        execContext.setDisableGrpcRemoteServerConnect(context.isDisableGrpcRemoteServerConnect());
        /**
         * 同步必要的配置及tpi资源到本地
         */
        snapshotConsumer.synchronizTpisAndConfs(execContext, cacheSnaphsot);

        Long triggerTimestamp = execContext.getPartitionTimestampWithMillis();// instanceParams.getLong(DataxUtils
        System.setProperty(DataxUtils.EXEC_TIMESTAMP, String.valueOf(triggerTimestamp));
        for (CuratorDataXTaskMessage tskMsg : triggerCfg.getSplitTabsCfg()) {
            tskMsg.setExecTimeStamp(triggerTimestamp);
        }
        triggerCfg.getSplitTabsCfg().forEach((tskMsg) -> {
            tskMsg.setJobId(taskId);
        });


        return Triple.of(execContext, snapshotConsumer, triggerCfg);
    }

    public static DataxPrePostConsumer createPrePostConsumer() {
        DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler systemPramsSuppiler = createSysPramsSuppiler();
        DataxPrePostConsumer prePostConsumer = new DataxPrePostConsumer(systemPramsSuppiler);
        return prePostConsumer;
    }

    public static DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler createSysPramsSuppiler() {
        return DataXJobRunEnvironmentParamsSetter.createSysPramsSuppiler();
    }

    public static void addSuccessPartition(ITaskExecutorContext context, AbstractExecContext execContext, String entityName) {

        Objects.requireNonNull(context, "workflowContext can not be null")
                .appendData2WfContext(ExecChainContextUtils.PARTITION_DATA_PARAMS + "_" + entityName, execContext.getPartitionTimestampWithMillis());
    }

    protected static IDataXBatchPost getDataXBatchPost(IDataxProcessor processor) {
        IDataxWriter writer = processor.getWriter(null);
        return IDataxWriter.castBatchPost(writer);
    }

    public void processSync(ITaskExecutorContext context, ExecPhase execPhase) throws Exception {

        RpcServiceReference svc = getRpcServiceReference();
        /**
         * 同步远端resource 资源
         */
        Triple<AbstractExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig> pair = createExecContext(context, execPhase);

        final AbstractExecContext execChainContext = Objects.requireNonNull(pair.getLeft(),
                "execChainContext can " + "not be null");
        final SelectedTabTriggersConfig triggerCfg = pair.getRight();

        ISelectedTab tab = new DefaultTab(triggerCfg.getTabName());

        // L1. 执行根任务
        try {
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

                    throw new RuntimeException("pretrigger:" + preTrigger + " faild", e);
                }
            }

            for (CuratorDataXTaskMessage tskMsg : triggerCfg.getSplitTabsCfg()) {
                executeSplitTabSync(context, statusRpc, svc, execChainContext, new SplitTabSync(tskMsg));
            }
        } finally {
            context.finallyCommit();
        }

    }

    protected void executeSplitTabSync(ITaskExecutorContext context, RpcServiceReference statusRpc
            , RpcServiceReference svc, AbstractExecContext execChainContext, SplitTabSync tabSync) {
        try {

            tabSync.execSync(execChainContext, statusRpc);
        } catch (Exception e) {
            context.errorLog("spilt table sync job:" + tabSync.tskMsg.getJobName() + " faild", e);
            reportError(e, execChainContext, svc);
            // return new ProcessResult(false, e.getMessage());
            throw new RuntimeException("spilt table sync job:" + tabSync.tskMsg.getJobName() + " faild", e);
        }
    }

    private static SelectedTabTriggersConfig getTriggerCfg(ITaskExecutorContext context) {
        SelectedTabTriggersConfig triggerCfg = SelectedTabTriggers.deserialize((context.getJobParams()));
        return triggerCfg;
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

    protected IRemoteTaskTrigger createDataXJob(AbstractExecContext execContext, Pair<String,
            IDataXBatchPost.LifeCycleHook> lifeCycleHookInfo, String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("param tableName can not be null");
        }
        IDataxProcessor processor = execContext.getProcessor();

        if (TisAppLaunch.isTestMock()) {
            IDataXBatchPost dataXBatchPost = getDataXBatchPost(execContext.getProcessor());
            DefaultTab tab = new DefaultTab(tableName);
            EntityName entityName = dataXBatchPost.parseEntity(tab);
            IDataXBatchPost.LifeCycleHook cycleHook = lifeCycleHookInfo.getRight();
            if (cycleHook == IDataXBatchPost.LifeCycleHook.Post) {
                return dataXBatchPost.createPostTask(execContext, entityName, tab,
                        processor.getDataxCfgFileNames(null, Optional.empty()));
            } else if (cycleHook == IDataXBatchPost.LifeCycleHook.Prep) {
                return dataXBatchPost.createPreExecuteTask(execContext, entityName, tab);
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

    private static void reportError(Exception e, AbstractExecContext execChainContext, RpcServiceReference svc) {

        Throwable rootCause = ExceptionUtils.getRootCause(e);
        svc.appendLog(LogLevel.ERROR, execChainContext.getTaskId(), Optional.empty(),
                rootCause != null ? ExceptionUtils.getStackTrace(rootCause) : ExceptionUtils.getStackTrace(e));
    }

}
