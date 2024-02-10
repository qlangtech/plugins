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

package com.qlangtech.plugins.incr.flink.launch.clustertype;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.TISFlinkCDCStart;
import com.qlangtech.plugins.incr.flink.cluster.BasicFlinkK8SClusterCfg;
import com.qlangtech.plugins.incr.flink.cluster.FlinkK8SClusterManager;
import com.qlangtech.plugins.incr.flink.cluster.KubernetesApplicationClusterConfig;
import com.qlangtech.plugins.incr.flink.common.FlinkK8SImage;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.Options;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.config.flink.JobManagerAddress;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterTokenManager;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.runtime.client.JobStatusMessage;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-07 10:39
 * @see KubernetesClusterDescriptor
 **/
public class KubernetesApplication extends ClusterType {

    public static final String KEY_CLUSTER_CFG = "clusterCfg";

    @FormField(ordinal = 0, identity = false, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String clusterId;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.identity, Validator.require})
    public String clusterCfg;

    @Override
    public FlinkClusterType getClusterType() {
        return FlinkClusterType.K8SApplication;
    }

    @Override
    public void checkUseable() throws TisException {

    }

    @Override
    public RestClusterClient createRestClusterClient() {
        return null;
    }

    @Override
    public JobManagerAddress getJobManagerAddress() {
        return null;
    }

    @Override
    public void deploy(TISFlinkCDCStreamFactory factory
            , TargetResName collection, File streamUberJar
            , Consumer<JarSubmitFlinkRequest> requestSetter, Consumer<JobID> afterSucce) throws Exception {
        //File launchTokenParentDir, TargetResName workerType, boolean launchTokenUseCptType, K8SWorkerCptType workerCptType

        FlinkClusterTokenManager flinkClusterToken = ServerLaunchToken.createFlinkClusterToken();
        ServerLaunchToken launchToken = flinkClusterToken.token(FlinkClusterType.K8SApplication, collection);
        DefaultClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();

        launchToken.writeLaunchToken(() -> {


            // File launchTokenParentDir = new File(k8sApplicationCfs.getTargetFile().getFile().getParentFile().getParentFile(), "");
            final ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();

            try {
                Thread.currentThread().setContextClassLoader(KubernetesApplication.class.getClassLoader());

                KubernetesApplicationClusterConfig k8SClusterManager = getK8SClusterCfg();

                FlinkK8SImage flinkK8SImage = k8SClusterManager.getFlinkK8SImage();

//        =new KubernetesApplicationClusterConfig();
//        k8SClusterManager.k8sImage = "tis_flink_image";
//        // k8SClusterManager.clusterId = "tis-flink-cluster";
//        k8SClusterManager.jmMemory = 1238400;
//        k8SClusterManager.tmMemory = 1169472;
//        k8SClusterManager.tmCPUCores = 150;
//        k8SClusterManager.taskSlot = 1;
//        k8SClusterManager.svcExposedType = "NodePort";
//        k8SClusterManager.svcAccount = "default";


                Configuration flinkConfig = Objects.requireNonNull(k8SClusterManager, "k8SClusterManager can not be null").createFlinkConfig();
                flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, TISFlinkCDCStart.class.getName());

                flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, this.clusterId);
                flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, Lists.newArrayList(collection.getName()));
                flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());

                ClusterClientFactory<String> kubernetesClusterClientFactory
                        = clusterClientServiceLoader.getClusterClientFactory(flinkConfig);

                ClusterSpecification clusterSpecification = kubernetesClusterClientFactory.getClusterSpecification(
                        flinkConfig);

//                FlinkKubeClient client
//                        = FlinkKubeClientFactory.getInstance().fromConfiguration(flinkConfig, "client");
                KubernetesClusterDescriptor kubernetesClusterDescriptor
                        = new KubernetesClusterDescriptor(flinkConfig, FlinkKubeClientFactory.getInstance());

                FlinkK8SClusterManager.getCreateAccompanyConfigMapResource();

                ClusterClientProvider<String> clusterProvider
                        = kubernetesClusterDescriptor.deployApplicationCluster(clusterSpecification
                        , ApplicationConfiguration.fromConfiguration(flinkConfig));
                JSONObject token = null;//new JSONObject();
                try (ClusterClient<String> clusterClient = clusterProvider.getClusterClient()) {
//                    final String entryUrl = clusterClient.getWebInterfaceURL();
//                    System.out.println(clusterClient.getWebInterfaceURL());
//                    token.put(FlinkClusterTokenManager.JSON_KEY_WEB_INTERFACE_URL, entryUrl);

                    token = createClusterMeta(FlinkClusterType.K8SApplication, clusterClient, flinkK8SImage);

                    int tryCount = 0;
                    boolean hasGetJobInfo = false;

                    tryGetJob:
                    while (tryCount++ < 5) {
                        for (JobStatusMessage jobStat : clusterClient.listJobs().get()) {
                            afterSucce.accept(jobStat.getJobId());
                            hasGetJobInfo = true;
                            break tryGetJob;
                        }
                        Thread.sleep(3000);
                    }

                    if (!hasGetJobInfo) {
                        throw TisException.create("has not get jobId");
                    }

                }


//                token.put(FlinkClusterTokenManager.JSON_KEY_CLUSTER_ID, clusterId);

//                token.put(FlinkClusterTokenManager.JSON_KEY_CLUSTER_TYPE, FlinkClusterType.K8SApplication.getToken());
//
//                token.put(FlinkClusterTokenManager.JSON_KEY_K8S_NAMESPACE, flinkK8SImage.getNamespace());
//                token.put(FlinkClusterTokenManager.JSON_KEY_K8S_BASE_PATH, flinkK8SImage.getK8SCfg().getKubeBasePath());
//                token.put(FlinkClusterTokenManager.JSON_KEY_K8S_ID, flinkK8SImage.identityValue());
                token.put(FlinkClusterTokenManager.JSON_KEY_APP_NAME, collection.getName());
                return Optional.of(token);
            } finally {
                Thread.currentThread().setContextClassLoader(originClassLoader);
            }
        });
        // launchToken.writeLaunchToken(Optional.of(token));

    }

    public static JSONObject createClusterMeta(FlinkClusterType clusterType, ClusterClient<String> clusterClient, FlinkK8SImage flinkK8SImage) {
        JSONObject token = new JSONObject();
        final String entryUrl = clusterClient.getWebInterfaceURL();
        System.out.println(clusterClient.getWebInterfaceURL());
        token.put(FlinkClusterTokenManager.JSON_KEY_WEB_INTERFACE_URL, entryUrl);
        token.put(FlinkClusterTokenManager.JSON_KEY_CLUSTER_ID, clusterClient.getClusterId());
        // token.put(FlinkClusterTokenManager.JSON_KEY_APP_NAME, collection.getName());
        token.put(FlinkClusterTokenManager.JSON_KEY_CLUSTER_TYPE, clusterType.getToken());

        token.put(FlinkClusterTokenManager.JSON_KEY_K8S_NAMESPACE, flinkK8SImage.getNamespace());
        token.put(FlinkClusterTokenManager.JSON_KEY_K8S_BASE_PATH, flinkK8SImage.getK8SCfg().getKubeBasePath());
        token.put(FlinkClusterTokenManager.JSON_KEY_K8S_ID, flinkK8SImage.identityValue());
        return token;
    }

    protected KubernetesApplicationClusterConfig getK8SClusterCfg() {
        IPluginStore<DataXJobWorker> k8sApplicationCfs = DataXJobWorker.getFlinkKubernetesApplicationCfgStore();
        return (KubernetesApplicationClusterConfig) k8sApplicationCfs.find(this.clusterCfg);
    }


    @TISExtension
    public static class DftDescriptor extends Descriptor<ClusterType> {
        Options<ClusterType> opts;

        public DftDescriptor() {
            super();
            this.registerSelectOptions(KEY_CLUSTER_CFG, () -> allClusterCfgs());
            opts = FlinkPropAssist.createOpts(this);
            BasicFlinkK8SClusterCfg.addClusterIdOption(opts).overwritePlaceholder("tis-flink-cluster");
        }

        private static List<KubernetesApplicationClusterConfig> allClusterCfgs() {
            // List<Option> opts = Lists.newArrayList();

            IPluginStore<DataXJobWorker> k8sApplicationCfs = DataXJobWorker.getFlinkKubernetesApplicationCfgStore();

            return k8sApplicationCfs.getPlugins().stream()
                    .map((cfg) -> (KubernetesApplicationClusterConfig) cfg).collect(Collectors.toList());
//                .forEach((cfg) -> {
//                    opts.add(new Option(cfg.identityValue()));
//                });


        }

        @Override
        public final String getDisplayName() {
            return KubernetesDeploymentTarget.APPLICATION.getName();
        }
        //        @Override
//        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            super.verify(msgHandler, context, postFormVals);
//            return true;
//        }
    }
}
