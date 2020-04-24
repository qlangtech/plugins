/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.fullbuild.indexbuild.impl;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.build.task.TaskMapper;
import com.qlangtech.tis.common.utils.TSearcherConfigFetcher;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.yarn.IYarnConfig;
import com.qlangtech.tis.fullbuild.indexbuild.*;
import com.qlangtech.tis.manage.common.ConfigFileReader;
import com.qlangtech.tis.manage.common.IndexBuildParam;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.pubhook.common.RunEnvironment;
import com.qlangtech.tis.trigger.jst.ImportDataProcessInfo;
import com.qlangtech.tis.util.XStream2;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2015年11月3日 上午10:39:41
 */
public class Hadoop020RemoteJobTriggerFactory implements IRemoteJobTriggerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(Hadoop020RemoteJobTriggerFactory.class);
    private static final Logger logger = LOG;

    private static final String DEPENDENCIES_LIB_DIR_SUFFIX = "-wf";

    private IYarnConfig yarnConfig;

    private FileSystemFactory fsFactory;
    private final IContainerPodSpec podSpec;

    // 构建索引过程中最大索引出错条数，超过了这个阀值就终止构建索引了
    private int maxDocMakeFaild;

    public Hadoop020RemoteJobTriggerFactory(IYarnConfig yarnConfig, FileSystemFactory fsFactory, IContainerPodSpec podSpec) {
        super();
        this.yarnConfig = yarnConfig;
        this.fsFactory = fsFactory;
        this.podSpec = podSpec;
    }

    /**
     * 执行索引构建
     *
     * @param timePoint
     * @param indexName
     * @param
     * @param groupNum
     * @param state
     * @param context
     * @return
     * @throws Exception
     */
    @Override
    public IRemoteJobTrigger createBuildJob(String timePoint, String indexName
            , String groupNum, IIndexBuildParam state, TaskContext context) throws Exception {
        final String coreName = indexName + "-" + groupNum;
        return getRemoteJobTrigger(coreName
                , createIndexBuildLauncherParam(state, Integer.parseInt(groupNum), podSpec.getName()));
    }

    @Override
    public void startTask(TaskMapper taskMapper, TaskContext taskContext) {

    }

    @Override
    public IRemoteJobTrigger createSingleTableDumpJob(IDumpTable table, String startTime, TaskContext context) {

        JobConfParams tabDumpParams = JobConfParams.createTabDumpParams(table, startTime, podSpec.getName());
        final String jobName = table.getDbName() + "." + table.getTableName();
        try {
            return getRemoteJobTrigger(jobName, tabDumpParams);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private IRemoteJobTrigger getRemoteJobTrigger(String name, JobConfParams launcherParam) throws IOException, YarnException {

        TSearcherConfigFetcher config = TSearcherConfigFetcher.get();
        RunEnvironment runtime = config.getRuntime();
        ParamsConfig pConfig = (ParamsConfig) this.yarnConfig;

        YarnConfiguration yarnConfig = pConfig.createConfigInstance();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfig);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();
        ApplicationSubmissionContext submissionContext = app.getApplicationSubmissionContext();
        submissionContext.setApplicationType(name);
        submissionContext.setMaxAppAttempts(2);
        submissionContext.setKeepContainersAcrossApplicationAttempts(false);
        final ApplicationId appid = submissionContext.getApplicationId();
        submissionContext.setApplicationName(name);
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        // 可以设置javaHome 留给以后扩展
        final String JAVA_HOME = "";// "/usr/lib/java/jdk1.8.0_91";

        String javaCommand = StringUtils.isEmpty(JAVA_HOME) ? "java" : (JAVA_HOME + "/bin/java ");

        final int memoryConsume = podSpec.getMaxHeapMemory();
        amContainer.setCommands(
                Collections.singletonList(javaCommand + getMemorySpec(memoryConsume) + getRemoteDebugParam()
                        + " -Druntime=" + runtime.getKeyName() + " com.qlangtech.tis.build.yarn.NodeMaster "
                        + launcherParam.paramSerialize() + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + " 2>"
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

        /* CLASSPATH 运行依賴的環境變量 */
        Map<String, String> environment = new HashMap<String, String>();
        setEnvironment(environment, amContainer, true);
        submissionContext.setAMContainerSpec(amContainer);
        // 使用4核10G的节点，原则上越大越好
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memoryConsume);
        capability.setVirtualCores(podSpec.getMaxCPUCores());
        // submissionContext.setNodeLabelExpression(nodeLabelExpression);
        submissionContext.setResource(capability);
        submissionContext.setQueue("default");
        Priority p = Records.newRecord(Priority.class);
        p.setPriority(2);
        submissionContext.setPriority(p);

        return new IRemoteJobTrigger() {
            @Override
            public void submitJob() {
                try {
                    yarnClient.submitApplication(submissionContext);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public RunningStatus getRunningStatus() {
                try {
                    ApplicationReport appReport = yarnClient.getApplicationReport(appid);
                    YarnApplicationState appState = appReport.getYarnApplicationState();
                    FinalApplicationStatus finalStatus = appReport.getFinalApplicationStatus();

                    if (appState != YarnApplicationState.RUNNING && appState != YarnApplicationState.KILLED
                            && appState != YarnApplicationState.FAILED && appState != YarnApplicationState.FINISHED) {
                        logger.info("waitting:" + name + " ,build task wait launch,current:" + appState);
                        // 等待任务开始
                        return new RunningStatus(0, false, false);
                    }

                    if (appState == YarnApplicationState.RUNNING) {
                        // 正在运行
                        return new RunningStatus(appReport.getProgress(), false, false);
                    }

                    if (appState == YarnApplicationState.KILLED || appState == YarnApplicationState.FAILED
                            || finalStatus != FinalApplicationStatus.SUCCEEDED) {
                        logger.error("slice:" + name + " ,build result:" + appState + "\n finalStatus:" + finalStatus
                                + "\ndiagnostics:" + appReport.getDiagnostics());
                        // 完成了，但是失败了
                        return new RunningStatus(appReport.getProgress(), true, false);
                    } else {
                        logger.info("core:" + name + " app (" + appid + ") is " + appState);
                        return new RunningStatus(appReport.getProgress(), true, true);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public int getMaxDocMakeFaild() {
        return maxDocMakeFaild;
    }

    public void setMaxDocMakeFaild(int maxDocMakeFaild) {
        this.maxDocMakeFaild = maxDocMakeFaild;
    }

    private JobConfParams createIndexBuildLauncherParam(
            IIndexBuildParam state, int groupNum, String indexBuilderTriggerFactoryName) {
        if (StringUtils.isEmpty(indexBuilderTriggerFactoryName)) {
            throw new IllegalArgumentException("param 'indexBuilderTriggerFactoryName' can not be empty");
        }
        final String coreName = state.getIndexName() + '-' + groupNum;
        // TSearcherConfigFetcher config = TSearcherConfigFetcher.get();
        JobConfParams jobConf = new JobConfParams();
        // 设置记录条数
        if (state.getDumpCount() != null) {
            jobConf.set(IndexBuildParam.INDEXING_ROW_COUNT, String.valueOf(state.getDumpCount()));
        }
        jobConf.set(IndexBuildParam.INDEXING_BUILDER_TRIGGER_FACTORY, indexBuilderTriggerFactoryName);
        jobConf.set(IndexBuildParam.INDEXING_BUILD_TABLE_TITLE_ITEMS, state.getBuildTableTitleItems());

        String outPath = ImportDataProcessInfo.createIndexDir( //
                this.fsFactory, state.getTimepoint() //
                , String.valueOf(groupNum) //
                , state.getIndexName() //
                , false);

        jobConf.set(IndexBuildParam.INDEXING_OUTPUT_PATH, outPath);

        String hdfsSourcePath = state.getHdfsSourcePath() == null //
                ? ImportDataProcessInfo.createIndexDir(this.fsFactory, state.getTimepoint(), String.valueOf(groupNum),
                state.getIndexName(), true) //
                : state.getHdfsSourcePath().build(String.valueOf(groupNum));

        try {
            jobConf.set(IndexBuildParam.INDEXING_SOURCE_PATH, URLEncoder.encode(hdfsSourcePath, "utf8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        final String schemaPath = this.fsFactory.getRootDir() + "/" + coreName + "/config/"
                + ConfigFileReader.FILE_SCHEMA.getFileName();
        final String solrConifgPath = this.fsFactory.getRootDir() + "/" + coreName + "/config/"
                + ConfigFileReader.FILE_SOLOR.getFileName();
        jobConf.set(IndexBuildParam.INDEXING_SCHEMA_PATH, schemaPath);
        jobConf.set(IndexBuildParam.INDEXING_SOLRCONFIG_PATH, solrConifgPath);

        jobConf.set(IndexBuildParam.INDEXING_SERVICE_NAME, state.getIndexName());

        jobConf.set(IndexBuildParam.INDEXING_CORE_NAME, coreName);
        jobConf.set(IndexBuildParam.INDEXING_MAX_NUM_SEGMENTS, String.valueOf(1));
        // jobConf.set(IndexBuildParam.INDEXING_USER_NAME, username);
        jobConf.set(IndexBuildParam.INDEXING_INCR_TIME, state.getTimepoint());
        jobConf.set(IndexBuildParam.INDEXING_GROUP_NUM, String.valueOf(groupNum));
        if (StringUtils.isNotBlank(state.getHdfsdelimiter())) {
            jobConf.set(IndexBuildParam.INDEXING_DELIMITER, state.getHdfsdelimiter());
        }
        jobConf.set(IndexBuildParam.JOB_TYPE, IndexBuildParam.JOB_TYPE_INDEX_BUILD);
        jobConf.set(IndexBuildParam.INDEXING_MAX_DOC_FAILD_LIMIT, String.valueOf(this.getMaxDocMakeFaild()));
        return jobConf;
    }

    private static void setEnvironment(Map<String, String> environment, ContainerLaunchContext ctx,
                                       boolean includeHadoopJars) throws IOException {

        Apps.addToEnvironment(environment, ApplicationConstants.Environment.CLASSPATH.name(), "/opt/data/tis/sharelib/indexbuild7.6/*",
                File.pathSeparator);
        Apps.addToEnvironment(environment, ApplicationConstants.Environment.CLASSPATH.name(), "/opt/data/tis/conf", File.pathSeparator);

        for (String c : YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH) {
            Apps.addToEnvironment(environment, ApplicationConstants.Environment.CLASSPATH.name(), c.trim(), File.pathSeparator);
        }
        Apps.addToEnvironment(environment //
                , ApplicationConstants.Environment.CLASSPATH.name() //
                , ApplicationConstants.Environment.HADOOP_COMMON_HOME.$() + "/share/hadoop/mapreduce/*",
                File.pathSeparator);
        ctx.setEnvironment(environment);
        logger.info("classpath:" + environment.get(ApplicationConstants.Environment.CLASSPATH.name()));
    }


    /**
     * 开启端口可用于调试之用
     */
    protected String getRemoteDebugParam() {
        return this.podSpec.getRunjdwpPort() > 0
                ? " -Xrunjdwp:transport=dt_socket,address=" + this.podSpec.getRunjdwpPort() + ",suspend=y,server=y " : StringUtils.EMPTY;
    }

    protected String getMemorySpec(int memoryConsume) {
        final int javaMemory = (int) (memoryConsume * 0.8);
        return " -Xms" + javaMemory + "m -Xmx" + javaMemory + "m";
    }


}
