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
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataXJobUtils;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.DataxExecutor;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.TISJarLoader;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.trigger.JobTrigger;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.workflow.pojo.IWorkflow;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;
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
        return DataXJobUtils.terminateWorkingTask(module, context, buildHistory);
    }


    @Override
    public TriggerBuildResult triggerJob(IControlMsgHandler module, Context context
            , DataXName appName, Optional<Long> powerJobWorkflowInstanceIdOpt, Optional<WorkFlowBuildHistory> latestWorkflowHistory) {
        if (appName == null) {
            throw new IllegalArgumentException("param appName can not be empty");
        }
        if (powerJobWorkflowInstanceIdOpt.isPresent()) {
            throw new IllegalStateException("powerJobWorkflowInstanceIdOpt contain is not support,workflowId:"
                    + powerJobWorkflowInstanceIdOpt.get());
        }
        try {
            List<HttpUtils.PostParam> params = Lists.newArrayList();
            params.add(new HttpUtils.PostParam(TriggerBuildResult.KEY_APPNAME, appName.getPipelineName()));

            Optional<JobTrigger> partialTrigger = JobTrigger.getPartialTriggerFromContext(context);
            partialTrigger.ifPresent((partial) -> {
                params.add(partial.getHttpPostSelectedTabsAsParam());
            });
            JobTrigger.addLatestWorkflowHistoryAsParam(params, latestWorkflowHistory);
            return TriggerBuildResult.triggerBuild(module, context, params);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TriggerBuildResult triggerWorkflowJob(IControlMsgHandler module, Context context
            , IWorkflow workflow, Boolean dryRun
            , Optional<Long> powerJobWorkflowInstanceIdOpt, Optional<WorkFlowBuildHistory> latestSuccessWorkflowHistory) {
        return DataXJobUtils.getTriggerWorkflowBuildResult(
                module, context, workflow, dryRun, powerJobWorkflowInstanceIdOpt, latestSuccessWorkflowHistory);
    }


    @Override
    public IRemoteTaskTrigger createDataXJob(IDataXJobContext taskContext, RpcServiceReference statusRpc
            , DataXJobInfo jobName, IDataxProcessor processor, CuratorDataXTaskMessage jobDTO) {

        Integer jobId = jobDTO.getTaskId();

        String dataXName = jobDTO.getDataXName();
        // DataxExecutor.statusRpc = statusRpc;
        final DataxExecutor dataxExecutor
                = new DataxExecutor(statusRpc, InstanceType.EMBEDDED, jobDTO.getAllRowsApproximately());

        ;

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
                                = DataxExecutor.DataXJobArgs.createJobArgs(
                                processor, jobId, jobName, jobDTO.getTaskSerializeNum(), jobDTO.getExecEpochMilli());

                        dataxExecutor.exec(getUberClassLoader(), jobName, processor, jobArgs);
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

    private JarLoader getUberClassLoader() {
        if (uberClassLoader == null) {
            uberClassLoader = new TISJarLoader(TIS.get().getPluginManager());
        }
        return uberClassLoader;
    }

    @Override
    public IDataXJobContext createJobContext(IExecChainContext parentContext) {
        return DataXJobSubmit.IDataXJobContext.create(parentContext);
    }
}
