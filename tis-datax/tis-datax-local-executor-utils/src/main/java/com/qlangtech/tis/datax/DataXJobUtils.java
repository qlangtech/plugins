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

package com.qlangtech.tis.datax;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.assemble.ExecResult;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.manage.common.ConfigFileContext;
import com.qlangtech.tis.manage.common.ConfigFileContext.Header;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.order.center.IAppSourcePipelineController;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.workflow.pojo.IWorkflow;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DataXJobUtils {
    public static boolean terminateWorkingTask(IControlMsgHandler module, Context context, IBuildHistory buildHistory) {
        ExecResult processState = ExecResult.parse(buildHistory.getState());
        List<Header> headers = Lists.newArrayList();
        headers.add(new Header(JobCommon.KEY_TASK_ID, String.valueOf(buildHistory.getTaskId())));
        headers.add(new Header(IParamContext.KEY_ASYN_JOB_NAME, String.valueOf(processState == ExecResult.ASYN_DOING)));
        headers.add(new Header(IFullBuildContext.KEY_APP_NAME, IAppSourcePipelineController.DATAX_FULL_PIPELINE + buildHistory.getAppName()));

        TriggerBuildResult triggerResult = null;
        try {
            triggerResult = TriggerBuildResult.triggerBuild(module, context, ConfigFileContext.HTTPMethod.DELETE, Collections.emptyList(), headers);
            return triggerResult.success;
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
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
}
