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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.core.util.container.JarLoader;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataxExecutor;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.TISJarLoader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.workflow.pojo.IWorkflow;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.qlangtech.tis.order.center.IParamContext;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 测试用让实例与assemble节点在同一个VM中跑
 * 需要在 tis-assemble工程中添加
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-04-22 20:29
 **/
@TISExtension()
@Public
public class EmbeddedDataXJobSubmit extends DataXJobSubmit {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedDataXJobSubmit.class);


    private transient JarLoader uberClassLoader;

    @Override
    public InstanceType getType() {
        return InstanceType.EMBEDDED;
    }

    @Override
    public boolean cancelTask(IControlMsgHandler module, Context context, IBuildHistory buildHistory) {
        return LocalDataXJobSubmit.terminateWorkingTask(module, context, buildHistory);
    }

    @Override
    public TriggerBuildResult triggerJob(IControlMsgHandler module, Context context
            , String appName, Optional<Long> powerJobWorkflowInstanceIdOpt) {
        if (StringUtils.isEmpty(appName)) {
            throw new IllegalArgumentException("param appName can not be empty");
        }
        if (powerJobWorkflowInstanceIdOpt.isPresent()) {
            throw new IllegalStateException("powerJobWorkflowInstanceIdOpt contain is not support,workflowId:"
                    + powerJobWorkflowInstanceIdOpt.get());
        }
        try {
            List<HttpUtils.PostParam> params = Lists.newArrayList();
            params.add(new HttpUtils.PostParam(TriggerBuildResult.KEY_APPNAME, appName));
            return TriggerBuildResult.triggerBuild(module, context, params);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TriggerBuildResult triggerWorkflowJob(IControlMsgHandler module, Context context
            , IWorkflow workflow, Boolean dryRun, Optional<Long> powerJobWorkflowInstanceIdOpt) {
        return getTriggerWorkflowBuildResult(module, context, workflow, dryRun, powerJobWorkflowInstanceIdOpt);

//        if (!TriggerBuildResult.triggerBuild(module, context, params).success) {
//            // throw new IllegalStateException("dataflowid:" + id + " trigger faild");
//        }


    }

    public static TriggerBuildResult getTriggerWorkflowBuildResult(IControlMsgHandler module, Context context
            , IWorkflow workflow, Boolean dryRun, Optional<Long> powerJobWorkflowInstanceIdOpt) {
        if (powerJobWorkflowInstanceIdOpt.isPresent()) {
            throw new IllegalStateException("powerJobWorkflowInstanceIdOpt is not support ");
        }
        List<HttpUtils.PostParam> params = Lists.newArrayList();
        Objects.requireNonNull(workflow, " workflow can not be null");
        params.add(new HttpUtils.PostParam(IFullBuildContext.DRY_RUN, dryRun));
        params.add(new HttpUtils.PostParam(IFullBuildContext.KEY_WORKFLOW_NAME, workflow.getName()));
        params.add(new HttpUtils.PostParam(IFullBuildContext.KEY_WORKFLOW_ID, String.valueOf(workflow.getId())));
        // TODO 单独触发的DF执行后期要保证该流程最后的执行的结果数据不能用于索引build
        params.add(new HttpUtils.PostParam(IFullBuildContext.KEY_APP_SHARD_COUNT, IFullBuildContext.KEY_APP_SHARD_COUNT_SINGLE));
        params.add(new HttpUtils.PostParam(IParamContext.COMPONENT_START, FullbuildPhase.FullDump.getName()));
        params.add(new HttpUtils.PostParam(IParamContext.COMPONENT_END, FullbuildPhase.JOIN.getName()));

        try {
            return TriggerBuildResult.triggerBuild(module, context, params);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    //    @Override
//    public void createJob(IControlMsgHandler module, Context context, DataxProcessor dataxProcessor) {
//
//    }
//
//    @Override
//    public <WorkflowInfoDTO> WorkflowInfoDTO saveJob(IControlMsgHandler module, Context context, DataxProcessor dataxProcessor) {
//        return null;
//    }

    @Override
    public IRemoteTaskTrigger createDataXJob(IDataXJobContext taskContext, RpcServiceReference statusRpc
            , DataXJobInfo jobName, IDataxProcessor processor, CuratorDataXTaskMessage jobDTO) {

        Integer jobId = jobDTO.getTaskId();

        String dataXName = jobDTO.getDataXName();
        DataxExecutor.statusRpc = statusRpc;
        final DataxExecutor dataxExecutor
                = new DataxExecutor(InstanceType.EMBEDDED, jobDTO.getAllRowsApproximately());

        if (uberClassLoader == null) {
            uberClassLoader = new TISJarLoader(TIS.get().getPluginManager());
        }

        return new IRemoteTaskTrigger() {
            @Override
            public String getTaskName() {
                return jobName.jobFileName;
            }


            @Override
            public void run() {
                try {

                    if (!taskContext.getTaskContext().isDryRun()) {
                        dataxExecutor.reportDataXJobStatus(false, false, false, jobId, jobName);

                        DataxExecutor.DataXJobArgs jobArgs
                                = DataxExecutor.DataXJobArgs.createJobArgs(processor, jobId, jobName, jobDTO.getTaskSerializeNum(), jobDTO.getExecEpochMilli());

                        dataxExecutor.exec(uberClassLoader, jobName, processor, jobArgs);
                    }

                    dataxExecutor.reportDataXJobStatus(false, jobId, jobName);
                } catch (Throwable e) {
                    dataxExecutor.reportDataXJobStatus(true, jobId, jobName);
                    //logger.error(e.getMessage(), e);
                    try {
                        //确保日志向远端写入了
                        Thread.sleep(3000);
                    } catch (InterruptedException ex) {

                    }
                    throw new RuntimeException(e);
                }

            }
        };
    }

    @Override
    public IDataXJobContext createJobContext(IJoinTaskContext parentContext) {
        return DataXJobSubmit.IDataXJobContext.create(parentContext);
    }
}
