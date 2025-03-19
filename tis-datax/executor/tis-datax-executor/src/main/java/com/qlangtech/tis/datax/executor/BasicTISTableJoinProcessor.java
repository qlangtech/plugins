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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.RpcUtils;
import com.qlangtech.tis.datax.join.DataXJoinProcessConsumer;
import com.qlangtech.tis.datax.join.DataXJoinProcessExecutor;
import com.qlangtech.tis.datax.join.WorkflowHookMsg;
import com.qlangtech.tis.datax.powerjob.CfgsSnapshotConsumer;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.exec.ExecChainContextUtils;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IPartionableWarehouse;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.powerjob.TriggersConfig;
import com.qlangtech.tis.rpc.grpc.log.ILoggerAppenderClient.LogLevel;
import com.qlangtech.tis.rpc.grpc.log.appender.LoggingEvent;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.web.start.TisAppLaunch;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import io.grpc.StatusRuntimeException;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-19 09:19
 **/
public class BasicTISTableJoinProcessor {
    private static final Pattern PATTERN_PARTITION_PARAMS =
            Pattern.compile("^" + ExecChainContextUtils.PARTITION_DATA_PARAMS + "_(.+?)$");
    transient RpcServiceReference statusRpc;

    private static DataXJoinProcessConsumer createTableJoinConsumer() {
        DataXJoinProcessConsumer joinProcessConsumer = new DataXJoinProcessConsumer(BasicTISTableDumpProcessor.createSysPramsSuppiler());
        return joinProcessConsumer;
    }

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


    protected void process(ITaskExecutorContext context) throws Exception {
        RpcServiceReference feedback = createRpcServiceReference();
        // StatusRpcClientFactory.AssembleSvcCompsite feedback = rpcRef.get();
        SqlTaskNodeMeta sqlTask =
                SqlTaskNodeMeta.deserializeTaskNode(ISqlTask.toCfg((context.getJobParams())));
        AbstractExecContext execContext = createDftExecContent(context);
        try {

            if (TisAppLaunch.isTestMock()) {
                DataXJoinProcessExecutor.executeJoin(feedback, execContext, sqlTask);
            } else {
                // 另外单独启一个进程来执行
                DataXJoinProcessConsumer tableJoinConsumer = createTableJoinConsumer();

                WorkflowHookMsg wfHookMsg = WorkflowHookMsg.create(sqlTask, execContext, sqlTask.getExportName());

                tableJoinConsumer.consumeMessage(wfHookMsg);
            }
            BasicTISTableDumpProcessor.addSuccessPartition(context, execContext, sqlTask.getExportName());

        } catch (StatusRuntimeException e) {
            // rpcRef.reConnect();
            throw e;
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);

            RpcUtils.setJoinStatus(execContext.getTaskId(), true, true, feedback, sqlTask.getExportName());

            feedback.appendLog(LogLevel.ERROR, execContext.getTaskId(), Optional.empty(),
                    rootCause != null ? ExceptionUtils.getStackTrace(rootCause) : ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    private AbstractExecContext createDftExecContent(ITaskExecutorContext context) {
        JSONObject instanceParams = (context.getInstanceParams());
        final CfgsSnapshotConsumer snapshotConsumer = new CfgsSnapshotConsumer();
        TriggersConfig triggerCfg = new TriggersConfig(instanceParams.getString(JobParams.KEY_COLLECTION), StoreResourceType.DataFlow);
        AbstractExecContext execContext = IExecChainContext.deserializeInstanceParams(triggerCfg, instanceParams, (ctx) -> {
//            ctx.setResType(StoreResourceType.DataFlow);
//            ctx.setWorkflowName(ctx.getIndexName());
//            ctx.setExecutePhaseRange(new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN));
            ctx.setExecutePhaseRange(new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN));
        }, snapshotConsumer);


        snapshotConsumer.synchronizTpisAndConfs(execContext, BasicTISTableDumpProcessor.cacheSnaphsot);

        IDataxProcessor dataxProc = execContext.getProcessor();
        IPartionableWarehouse partionableWarehouse = IDataxWriter.getPartionableWarehouse(dataxProc.getWriter(null));

        Matcher matcher = null;

        for (Map.Entry<String, Object> entry : instanceParams.entrySet()) {
            Object val = entry.getValue();
            try {
                matcher = PATTERN_PARTITION_PARAMS.matcher(entry.getKey());
                if (matcher.matches()) {
                    execContext.putTablePt(EntityName.parse(matcher.group(1)),
                            () -> partionableWarehouse.getPsFormat().format(Long.parseLong(String.valueOf(val))));
                }
            } catch (Exception e) {
                throw new RuntimeException("key:" + entry.getKey() + ",val:" + entry.getValue(), e);
            }
        }

        return execContext;
    }
}
