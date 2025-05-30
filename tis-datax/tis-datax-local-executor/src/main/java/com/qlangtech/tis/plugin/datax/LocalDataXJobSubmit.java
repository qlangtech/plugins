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
import com.alibaba.datax.common.element.QueryCriteria;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.build.task.IBuildHistory;
import com.qlangtech.tis.config.flink.IFlinkCluster;
import com.qlangtech.tis.coredefine.module.action.TriggerBuildResult;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobRunEnvironmentParamsSetter;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataXJobUtils;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.DataxExecutor;
import com.qlangtech.tis.datax.DataxPrePostConsumer;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.preview.PreviewRowsData;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskTrigger;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.plugin.datax.DataXPipelinePreviewProcessorExecutor.PreviewLaunchParam;
import com.qlangtech.tis.plugin.trigger.JobTrigger;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.web.start.TisAppLaunch;
import com.qlangtech.tis.web.start.TisSubModule;
import com.qlangtech.tis.workflow.pojo.IWorkflow;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.qlangtech.tis.fullbuild.IFullBuildContext.KEY_LASTEST_WORKFLOW_HISTORY_ID;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 17:28
 **/
@TISExtension()
@Public
public class LocalDataXJobSubmit extends DataXJobSubmit implements DataXJobRunEnvironmentParamsSetter {

    private String mainClassName = DataxExecutor.class.getName();
    private File workingDirectory = new File(".");
    private String classpath;
    private ExtraJavaSystemPramsSuppiler extraJavaSystemPramsSuppiler = new ExtraJavaSystemPramsSuppiler(true);

    private final static Logger logger = LoggerFactory.getLogger(LocalDataXJobSubmit.class);

    @Override
    public InstanceType getType() {
        return InstanceType.LOCAL;
    }

    @Override
    public TriggerBuildResult triggerWorkflowJob(IControlMsgHandler module
            , Context context, IWorkflow workflow, Boolean dryRun
            , Optional<Long> powerJobWorkflowInstanceIdOpt, Optional<WorkFlowBuildHistory> latestSuccessWorkflowHistory) {
        return DataXJobUtils.getTriggerWorkflowBuildResult(
                module, context, workflow, dryRun, powerJobWorkflowInstanceIdOpt, latestSuccessWorkflowHistory);
    }

    /**
     * 由Console节点调用
     *
     * @param module
     * @param context
     * @param appName
     * @return
     */
    @Override
    public TriggerBuildResult triggerJob(IControlMsgHandler module, Context context
            , DataXName appName, Optional<Long> powerjobWorkflowInstanceIdOpt, Optional<WorkFlowBuildHistory> latestWorkflowHistory) {
        if ((appName) == null) {
            throw new IllegalArgumentException("param appName can not be empty");
        }
        if (powerjobWorkflowInstanceIdOpt.isPresent()) {
            throw new UnsupportedOperationException("must processed by pwoerJob");
        }
        try {
            List<HttpUtils.PostParam> params = Lists.newArrayList();
            params.add(new HttpUtils.PostParam(TriggerBuildResult.KEY_APPNAME, appName));
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

    private final ConcurrentMap<String, DataXPipelinePreviewProcessorExecutor> tabSynchronizeCache = Maps.newConcurrentMap();

    @Override
    public PreviewRowsData previewRowsData(DataXName dataXName, String tableName, QueryCriteria queryCriteria) {
        DataXPipelinePreviewProcessorExecutor previewProcessorExecutor = getPipelinePreviewProcessor(dataXName.getPipelineName());
        return previewProcessorExecutor.previewRowsData(dataXName, tableName, queryCriteria);
    }

    private DataXPipelinePreviewProcessorExecutor getPipelinePreviewProcessor(String dataXName) {
        DataXPipelinePreviewProcessorExecutor previewProcessorExecutor = tabSynchronizeCache.get(dataXName);
        if (previewProcessorExecutor == null) {
            previewProcessorExecutor = tabSynchronizeCache.computeIfAbsent(dataXName, (name) -> {
                try {
                    final int freeSocketPort = NetUtils.getFreeSocketPort();
                    final DataXPipelinePreviewProcessorExecutor previewLaunchProcessorExecutor = new DataXPipelinePreviewProcessorExecutor(freeSocketPort);
                    previewLaunchProcessorExecutor.setClasspath(getJAVAClasspath());
                    PreviewLaunchParam launchParam = new PreviewLaunchParam(dataXName);
                    // 启动服务
                    previewLaunchProcessorExecutor.consumeMessage(launchParam);


                    PreviewProgressorExpireTracker commitTracker = new PreviewProgressorExpireTracker(dataXName, TimeUnit.SECONDS.toMillis(60)) {
                        @Override
                        protected void executeExpirEvent() {
                            previewLaunchProcessorExecutor.runningTask.values().forEach((task) -> {
                                task.destroyProcess();
                            });
                            tabSynchronizeCache.remove(dataXName);
                        }
                    };
                    previewLaunchProcessorExecutor.setCommitTracker(commitTracker);
                    return previewLaunchProcessorExecutor;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return previewProcessorExecutor;
    }



    @Override
    public boolean cancelTask(IControlMsgHandler module, Context context, IBuildHistory buildHistory) {
        return DataXJobUtils.terminateWorkingTask(module, context, buildHistory);
    }

    @Override
    public IRemoteTaskTrigger createDataXJob(
            IDataXJobContext taskContext, RpcServiceReference statusRpc
            , DataXJobInfo jobName, IDataxProcessor processor, CuratorDataXTaskMessage dataXJobDTO) {
        getJAVAClasspath();
        logger.info("dataX Job:{},classpath:{},workingDir:{}", jobName.jobFileName, this.classpath, workingDirectory.getPath());
        Objects.requireNonNull(statusRpc, "statusRpc can not be null");
        // IDataxReader dataxReader = dataxProcessor.getReader(null);
        //Optional<List<String>> ptabs = null;
//        if (dataxReader instanceof IDataSourceFactoryGetter) {
//            DataSourceFactory dsFactory = ((IDataSourceFactoryGetter) dataxReader).getDataSourceFactory();
////            tabDataXEntity.getDbIdenetity();
////            tabDataXEntity.getSourceTableName();
//            ptabs = Optional.of(dsFactory.getAllPhysicsTabs(tabDataXEntity));
//        }

        //  TableInDB tablesInDB = dataxReader.getTablesInDB();

        return TaskExec.getRemoteJobTrigger(taskContext, this, jobName, processor);
    }


    private String getJAVAClasspath() {
        if (StringUtils.isEmpty(this.classpath)) {
            if (TisAppLaunch.isTestMock()) {
                this.setClasspath(DataxPrePostConsumer.DEFAULT_CLASSPATH);
            } else {
                File tisHomeDir = Config.getTisHome();
                File assebleDir = new File(tisHomeDir, TisSubModule.TIS_ASSEMBLE.moduleName);
                File localExecutorLibDir = new File(Config.getLibDir(), "plugins/" + IFlinkCluster.PLUGIN_TIS_DATAX_LOCAL_EXECOTOR + "/WEB-INF/lib");
                File webStartDir = new File(tisHomeDir, TisSubModule.WEB_START.moduleName);

                if (!localExecutorLibDir.exists()) {
                    throw new IllegalStateException("target localExecutorLibDir dir is not exist:" + localExecutorLibDir.getAbsolutePath());
                }
                if (!assebleDir.exists()) {
                    throw new IllegalStateException("target asseble dir is not exist:" + assebleDir.getAbsolutePath());
                }
                if (!webStartDir.exists()) {
                    throw new IllegalStateException("target " + TisSubModule.WEB_START.moduleName + "/lib dir is not exist:" + webStartDir.getAbsolutePath());
                }
                this.classpath = assebleDir.getPath() + "/lib/*:" + localExecutorLibDir.getPath()
                        + "/*:" + webStartDir.getPath() + "/conf:" + new File(webStartDir, "/lib/*").getPath();
            }
        }
        return this.classpath;
    }


    @Override
    public DataXJobSubmit.IDataXJobContext createJobContext(final IExecChainContext parentContext) {
        return DataXJobSubmit.IDataXJobContext.create(parentContext);
    }

    public void setMainClassName(String mainClassName) {
        this.mainClassName = mainClassName;
    }


    @Override
    public void setWorkingDirectory(File workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public String getMainClassName() {
        return mainClassName;
    }

    public File getWorkingDirectory() {
        return workingDirectory;
    }

    public String getClasspath() {
        if (StringUtils.isEmpty(this.classpath)) {
            throw new IllegalStateException("param classpath can not be null");
        }
        return classpath;
    }

    @Override
    public void setExtraJavaSystemPramsSuppiler(ExtraJavaSystemPramsSuppiler extraJavaSystemPramsSuppiler) {
        this.extraJavaSystemPramsSuppiler = extraJavaSystemPramsSuppiler;
    }

    @Override
    public void setClasspath(String classpath) {
        this.classpath = classpath;
    }

    public String[] getExtraJavaSystemPrams() {
        List<String> params = Objects.requireNonNull(extraJavaSystemPramsSuppiler.get(), "extraJavaSystemPramsSuppiler can not be null");
        return params.toArray(new String[params.size()]);
        // return new String[]{"-D" + CenterResource.KEY_notFetchFromCenterRepository + "=true"};
    }

//    @Override
//    public CuratorDataXTaskMessage getDataXJobDTO(IJoinTaskContext taskContext, DataXJobInfo dataXJobInfo) {
//        return super.getDataXJobDTO(taskContext, dataXJobInfo);
//    }


}
