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

package com.qlangtech.plugins.incr.flink.launch;


import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.launch.ckpt.CKOn;
import com.qlangtech.plugins.incr.flink.launch.clustertype.ClusterType;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.IDeploymentDetail;
import com.qlangtech.tis.coredefine.module.action.IFlinkIncrJobStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.manage.common.incr.UberJarUtil;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.trigger.jst.ILogListener;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-16 09:16
 **/
@Public
public class TISFlinkCDCStreamFactory extends IncrStreamFactory {
    public static final String NAME_FLINK_CDC = "Flink";
    private static final String KEY_FIELD_CHECKPOINT = "checkpoint";
    private static final String KEY_FIELD_STATEBACKEND = "stateBackend";

    @FormField(ordinal = 1, validate = {Validator.require})
    public ClusterType cluster;

    @Override
    public FlinkClusterType getClusterType() {
        return cluster.getClusterType();
    }


    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.integer, Validator.require})
    public Integer parallelism;


    @FormField(ordinal = 4, validate = {Validator.require})
    public RestartStrategyFactory restartStrategy;
    /**
     * 支持任务恢复，当Flink节点因为服务器意外宕机导致当前运行的flink job意外终止，需要支持Flink Job恢复执行，需要Flink配置，配置支持
     * 1.持久化stateBackend
     * 2.开启checkpoint
     */
    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean enableRestore;
    @FormField(ordinal = 6, validate = {Validator.require})
    public CheckpointFactory checkpoint;


    @FormField(ordinal = 7, validate = {Validator.require})
    public StateBackendFactory stateBackend;

    @Override
    public Optional<ISavePointSupport> restorable() {
        if (isCheckpointEnable()) {
            return StateBackendFactory.getSavePointSupport(stateBackend);
        }
        return Optional.empty();
    }

    private boolean isCheckpointEnable() {
        return checkpoint instanceof CKOn;
    }

    @Override
    public IFlinkIncrJobStatus getIncrJobStatus(TargetResName collection) {
        return stateBackend.getIncrJobStatus(collection);
    }

    public static List<Option> allRestartStrategy() {
        return Arrays.stream(FlinkJobRestartStrategy.values())
                .map((v) -> new Option(v.val))
                .collect(Collectors.toList());
    }

    public ClusterClient getFlinkCluster() {
        ClusterType item = getClusterCfg();
        return item.createRestClusterClient();
    }


    @Override
    public StreamExecutionEnvironment createStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(this.parallelism);
        Objects.requireNonNull(this.restartStrategy, "restartStrategy can not be null");
        env.setRestartStrategy(this.restartStrategy.parseRestartStrategy());

        Objects.requireNonNull(this.checkpoint, "checkpoint can not be null");
        this.checkpoint.setProps(env);

        Objects.requireNonNull(this.stateBackend, "stateBackend can not be null");
        stateBackend.setProps(env);
        return env;
    }

    public ClusterType getClusterCfg() {
        return this.cluster;
    }

    // @Override
    private FlinkTaskNodeController getIncrSync() {
        FlinkTaskNodeController flinkTaskNodeController = new FlinkTaskNodeController(this);
        return flinkTaskNodeController;
    }

    /**
     * ==========================================================================
     * implement: IRCController Start
     * ==========================================================================
     */
    @Override
    public void checkUseable(TargetResName collection) {
        this.getClusterCfg().checkUseable(collection);
    }

    @Override
    public void deploy(TargetResName collection, ReplicasSpec incrSpec, long timestamp) throws Exception {

        File streamUberJar = UberJarUtil.createStreamUberJar(collection, timestamp);
        this.deploy(collection, streamUberJar
                , (request) -> {
                }, (jobId) -> {
                    IFlinkIncrJobStatus incrJob = getIncrJobStatus(collection);
                    incrJob.createNewJob(jobId);
                });
    }


    private void deploy(TargetResName collection, File streamUberJar
            , Consumer<JarSubmitFlinkRequest> requestSetter, Consumer<JobID> afterSuccess) throws Exception {
        this.getClusterCfg().deploy(this, collection, streamUberJar, requestSetter, afterSuccess);
    }

    @Override
    public void removeInstance(TargetResName collection) throws Exception {
        this.getClusterCfg().removeInstance(this, collection);
    }

    @Override
    public void stopInstance(TargetResName indexName) {
        this.getIncrSync().stopInstance(indexName);
    }

    @Override
    public SupportTriggerSavePointResult supportTriggerSavePoint(TargetResName collection) {
        return this.getIncrSync().supportTriggerSavePoint(collection);
    }

    @Override
    public void restoreFromCheckpoint(TargetResName resName, Integer checkpointId) {
        this.getIncrSync().restoreFromCheckpoint(resName, checkpointId);
    }

    @Override
    public void triggerSavePoint(TargetResName collection) {
        this.getIncrSync().triggerSavePoint(collection);
    }

    @Override
    public void relaunch(TargetResName collection, String... targetPod) {
        this.getIncrSync().relaunch(collection, targetPod);
    }

    @Override
    public IDeploymentDetail getRCDeployment(TargetResName collection) {
        return this.getIncrSync().getRCDeployment(collection);
    }

    @Override
    public WatchPodLog listPodAndWatchLog(TargetResName collection, String podName, ILogListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void discardSavepoint(TargetResName resName, String savepointPath) {
        this.getIncrSync().discardSavepoint(resName, savepointPath);
    }

    /**
     * ==========================================================================
     * implement: IRCController END
     * ==========================================================================
     */


    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<IncrStreamFactory> {

        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return NAME_FLINK_CDC;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            TISFlinkCDCStreamFactory plugin = postFormVals.newInstance();

            if (plugin.enableRestore) {
                if (!plugin.restorable().isPresent()) {
                    if (!plugin.isCheckpointEnable()) {
                        msgHandler.addFieldError(context, KEY_FIELD_CHECKPOINT, "请确认是否开启");
                    }
                    if (!StateBackendFactory.getSavePointSupport(plugin.stateBackend).isPresent()) {
                        msgHandler.addFieldError(context, KEY_FIELD_STATEBACKEND, "请使用持久化stateBackend");
                    }
                    msgHandler.addErrorMessage(context, "尚未满足可恢复任务配置要求");
                    return false;
                }
            }


            return super.validateAll(msgHandler, context, postFormVals);
        }

        /**
         * 校验并行度
         *
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param value
         * @return
         */
        public boolean validateParallelism(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Integer parallelism = Integer.parseInt(value);
            if (parallelism < 1) {
                msgHandler.addFieldError(context, fieldName, "执行并行度不能小于1");
                return false;
            }
            if (parallelism > 16) {
                msgHandler.addFieldError(context, fieldName, "执行并行度不能大于16");
                return false;
            }
            return true;
        }


    }
}
