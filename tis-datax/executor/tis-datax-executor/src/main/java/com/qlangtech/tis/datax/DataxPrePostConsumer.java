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

import com.google.common.collect.Lists;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.solrj.util.ZkUtils;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;
import java.util.Objects;

/**
 * DataX 执行器
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-06 14:57
 **/
public class DataxPrePostConsumer extends DataXJobSingleProcessorExecutor<DataXLifecycleHookMsg> {

    private final DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler extraJavaSystemPramsSuppiler;

    public DataxPrePostConsumer() {
        this(new DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler(false) {
            @Override
            public List<String> get() {
                List<String> params = Lists.newArrayList(super.get());
                params.add("-D" + Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS + "=true");
                return params;
            }
        });
    }

    public DataxPrePostConsumer(DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler extraJavaSystemPramsSuppiler) {
        //  this.dataxExecutor = dataxExecutor;
        //  this.curatorClient = curatorClient;
        this.extraJavaSystemPramsSuppiler = extraJavaSystemPramsSuppiler;
    }

    //    =
    //            ;

    public DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler getExtraJavaSystemPramsSuppiler() {
        return Objects.requireNonNull(extraJavaSystemPramsSuppiler, "extraJavaSystemPramsSuppiler can not be null");
    }


    @Override
    public void consumeMessage(DataXLifecycleHookMsg msg) throws Exception {
        super.consumeMessage(msg);
    }

    @Override
    protected void addMainClassParams(DataXLifecycleHookMsg msg, Integer taskId, String jobName, String dataxName,
                                      CommandLine cmdLine) {
        //        Integer jobId = Integer.parseInt(args[0]);
        //        String dataXName = args[1];
        //        String incrStateCollectAddress = args[2];
        //        if (StringUtils.isEmpty(incrStateCollectAddress)) {
        //            throw new IllegalArgumentException("arg 'incrStateCollectAddress' can not be null");
        //        }
        //        StatusRpcClient.AssembleSvcCompsite statusRpc =
        //                StatusRpcClient.connect2RemoteIncrStatusServer(incrStateCollectAddress);
        //        DataxExecutor.rpcRef.set(statusRpc);
        //
        //        final String lifecycleHookName = args[3];
        //        final ISelectedTab tab = new DefaultTab(args[4]);
        //        if (StringUtils.isEmpty(tab.getName())) {
        //            throw new IllegalStateException("param table name can not be empty");
        //        }
        //
        //        StoreResourceType resType = StoreResourceType.parse(args[5]);
        //        final long execEpochMilli = Long.parseLong(args[6]);
        //        if (StringUtils.isEmpty(msg.getTableName())) {
        //            throw new IllegalArgumentException("param table name can not be empty");
        //        }
        if (StringUtils.isEmpty(dataxName)) {
            throw new IllegalArgumentException("param dataxName can not be empty");
        }
        //        if (StringUtils.isEmpty(msg.getTableName())) {
        //            throw new IllegalArgumentException("param getTableName can not be empty");
        //        }
        if (StringUtils.isEmpty(msg.getJobName())) {
            throw new IllegalArgumentException("param getJobName can not be empty");
        }

        cmdLine.addArgument(String.valueOf(Objects.requireNonNull(taskId, "taskId can not be null")));
        cmdLine.addArgument(dataxName);
        cmdLine.addArgument(getIncrStateCollectAddress());
        // lifecycleHookName
        cmdLine.addArgument(msg.getLifeCycleHook().getToken());
        // tabName: ISelectedTab
        cmdLine.addArgument(msg.getTableName());
        // StoreResourceType
        cmdLine.addArgument(msg.getResType().getType());
        cmdLine.addArgument(msg.getJobName());
        cmdLine.addArgument(String.valueOf(msg.getExecEpochMilli()));
    }

    @Override
    protected String[] getExtraJavaSystemPrams() {
        List<String> params = getExtraJavaSystemPramsSuppiler().get();
        return params.toArray(new String[params.size()]);
    }

    @Override
    protected boolean isCurrentJobProcessing(Integer jobId) {
        //        WorkFlowBuildHistory wfStatus = DagTaskUtils.getWFStatus(jobId);
        //        ExecResult execStat = ExecResult.parse(wfStatus.getState());
        //        return execStat.isProcessing();
        return true;
    }

    @Override
    protected DataXJobSubmit.InstanceType getExecMode() {
        return DataXJobSubmit.InstanceType.DISTRIBUTE;
    }

    protected String getIncrStateCollectAddress() {
        // return ZkUtils.getFirstChildValue(this.coordinator, ZkUtils.ZK_ASSEMBLE_LOG_COLLECT_PATH);
        // TODO: 如果是分布式环境的话这个getAssembleHost 需要可以支持HA，目前是写死的
        return Config.getAssembleHost() + ":" + ZkUtils.ZK_ASSEMBLE_LOG_COLLECT_PORT;
    }

    protected String getMainClassName() {
        return DataxPrePostExecutor.class.getName();
    }

    public File getWorkingDirectory() {
        return getDataXExecutorDir();
    }

    public static File getDataXExecutorDir() {
        File workDir = new File("/opt/tis/tis-datax-executor");
        if (!workDir.exists()) {
            throw new IllegalStateException("workDir is not exist:" + workDir.getAbsolutePath());
        }
        return workDir;
    }

    public static final String DEFAULT_CLASSPATH = "./lib/*:./tis-datax-executor.jar:./conf/";

    public String getClasspath() {
        return DEFAULT_CLASSPATH;
    }

}
