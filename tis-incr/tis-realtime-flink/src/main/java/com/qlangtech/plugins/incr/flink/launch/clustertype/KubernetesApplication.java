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

import com.alibaba.citrus.turbine.Context;
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
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.JobResName.OwnerJobExec;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterTokenManager;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8SRCResNameWithFieldSelector;
import com.qlangtech.tis.plugin.datax.powerjob.impl.PowerJobPodLogListener;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8SUtils.WaitReplicaControllerLaunch;
import com.qlangtech.tis.plugin.k8s.NamespacedEventCallCriteria;
import com.qlangtech.tis.plugin.k8s.ResChangeCallback;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.trigger.socket.ExecuteState;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1ReplicaSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-07 10:39
 * @see KubernetesClusterDescriptor
 **/
public class KubernetesApplication extends ClusterType {

    public static final String KEY_CLUSTER_CFG = "clusterCfg";
    private static final Logger logger = LoggerFactory.getLogger(KubernetesApplication.class);

    private static final String KEY_CLUSTER_ID = "clusterId";

    @FormField(ordinal = 0, identity = false, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String clusterId;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.identity, Validator.require})
    public String clusterCfg;

    @Override
    public void checkUseable(TargetResName collection) throws TisException {
        ServerLaunchToken launchToken = this.getLaunchToken(collection);
        if (!launchToken.isLaunchTokenExist()) {
            // 实例还没有创建，直接退出
            return;
        }
        super.checkUseable(collection);
    }

    @Override
    public FlinkClusterType getClusterType() {
        return FlinkClusterType.K8SApplication;
    }

    //  private transient ClusterClient _clusterClient;

    @Override
    public ClusterClient createRestClusterClient() {
//        if (this._clusterClient == null) {
        //  final Pair<Configuration, IK8sContext> flinkConfig = ;
//            this._clusterClient =
//        }

        return this.createClient(getFlinkCfg(), false);
    }

    @Override
    public void removeInstance(TISFlinkCDCStreamFactory factory, TargetResName collection) throws Exception {

        //IFlinkIncrJobStatus<JobID> incrJobStatus = factory.getIncrJobStatus(collection);
//        JobID jobID = incrJobStatus.getLaunchJobID();
//        if (jobID != null) {
//            createRestClusterClient().cancel(jobID);
//        }
        boolean hasDelete = false;
        try {
            createRestClusterClient().shutDownCluster();
            hasDelete = true;
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }


        try {
            FlinkK8SImage flinkK8SImage = getFlinkK8SImage();
            AppsV1Api appsV1Api = flinkK8SImage.createAppsV1Api();
            appsV1Api.deleteNamespacedDeployment(this.clusterId, flinkK8SImage.getNamespace())
                    .pretty(K8SUtils.resultPrettyShow).execute();
        } catch (ApiException e) {
            logger.warn(e.getMessage());
        }


        //
        this.getLaunchToken(collection).deleteLaunchToken();

    }

    private FlinkK8SImage getFlinkK8SImage() {
        KubernetesApplicationClusterConfig k8SClusterManager = getK8SClusterCfg();
        return k8SClusterManager.getFlinkK8SImage();
    }


    protected ClusterClient createClient(Pair<Configuration, IK8sContext> flinkConfig, boolean execDeploy) {
        // File launchTokenParentDir = new File(k8sApplicationCfs.getTargetFile().getFile().getParentFile().getParentFile(), "");
        final ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(KubernetesApplication.class.getClassLoader());

            //    DefaultClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();

//        =new KubernetesApplicationClusterConfig();
//        k8SClusterManager.k8sImage = "tis_flink_image";
//        // k8SClusterManager.clusterId = "tis-flink-cluster";
//        k8SClusterManager.jmMemory = 1238400;
//        k8SClusterManager.tmMemory = 1169472;
//        k8SClusterManager.tmCPUCores = 150;
//        k8SClusterManager.taskSlot = 1;
//        k8SClusterManager.svcExposedType = "NodePort";
//        k8SClusterManager.svcAccount = "default";

            // KubernetesApplicationClusterConfig k8SClusterManager = getK8SClusterCfg();
            //  Configuration flinkConfig = Objects.requireNonNull(k8SClusterManager, "k8SClusterManager can not be null").createFlinkConfig();
//            flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, TISFlinkCDCStart.class.getName());
//
//            flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, this.clusterId);
//            flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, Lists.newArrayList(collection.getName()));
//            flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());

            ClusterClientFactory<String> kubernetesClusterClientFactory
                    = new KubernetesClusterClientFactory();
            //clusterClientServiceLoader.getClusterClientFactory(flinkConfig);

            ClusterSpecification clusterSpecification = kubernetesClusterClientFactory.getClusterSpecification(
                    flinkConfig.getLeft());

//                FlinkKubeClient client
//                        = FlinkKubeClientFactory.getInstance().fromConfiguration(flinkConfig, "client");
            KubernetesClusterDescriptor kubernetesClusterDescriptor
                    = new KubernetesClusterDescriptor(flinkConfig.getKey(), FlinkKubeClientFactory.getInstance());

            FlinkK8SClusterManager.getCreateAccompanyConfigMapResource(flinkConfig.getRight());

            if (execDeploy) {
                ClusterClientProvider<String> clusterProvider
                        = kubernetesClusterDescriptor.deployApplicationCluster(clusterSpecification
                        , ApplicationConfiguration.fromConfiguration(flinkConfig.getLeft()));
                return clusterProvider.getClusterClient();
            } else {
                return kubernetesClusterDescriptor.retrieve(
                        flinkConfig.getLeft().getString(KubernetesConfigOptions.CLUSTER_ID)).getClusterClient();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(originClassLoader);
        }
    }

    @Override
    public JobManagerAddress getJobManagerAddress() {
        return new JobManagerAddress(null, -1) {
            @Override
            public String getURL() {
                return createRestClusterClient().getWebInterfaceURL();
            }
        };
    }

    @Override
    public void deploy(TISFlinkCDCStreamFactory factory
            , TargetResName collection, File streamUberJar
            , Consumer<JarSubmitFlinkRequest> requestSetter, Consumer<JobID> afterSucce) throws Exception {
        final SSERunnable sse = SSERunnable.getLocal();

        //  launchToken.writeLaunchToken(() -> {

        FlinkK8SImage flinkK8SImage = getFlinkK8SImage();

//        =new KubernetesApplicationClusterConfig();
//        k8SClusterManager.k8sImage = "tis_flink_image";
//        // k8SClusterManager.clusterId = "tis-flink-cluster";
//        k8SClusterManager.jmMemory = 1238400;
//        k8SClusterManager.tmMemory = 1169472;
//        k8SClusterManager.tmCPUCores = 150;
//        k8SClusterManager.taskSlot = 1;
//        k8SClusterManager.svcExposedType = "NodePort";
//        k8SClusterManager.svcAccount = "default";


        Pair<Configuration, IK8sContext> pair = getFlinkCfg();
        Configuration flinkConfig = pair.getLeft();
        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, TISFlinkCDCStart.class.getName());


        flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, Lists.newArrayList(collection.getName()));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());

//                ClusterClientFactory<String> kubernetesClusterClientFactory
//                        = clusterClientServiceLoader.getClusterClientFactory(flinkConfig);
//
//                ClusterSpecification clusterSpecification = kubernetesClusterClientFactory.getClusterSpecification(
//                        flinkConfig);

//                FlinkKubeClient client
//                        = FlinkKubeClientFactory.getInstance().fromConfiguration(flinkConfig, "client");
//            KubernetesClusterDescriptor kubernetesClusterDescriptor
//                    = new KubernetesClusterDescriptor(flinkConfig, FlinkKubeClientFactory.getInstance());

        //  FlinkK8SClusterManager.getCreateAccompanyConfigMapResource(pair.getRight());

//                ClusterClientProvider<String> clusterProvider
//                        = kubernetesClusterDescriptor.deployApplicationCluster(clusterSpecification
//                        , ApplicationConfiguration.fromConfiguration(flinkConfig));
        // = null;//new JSONObject();
        WatchPodLog watchPodLog = null;
        try (ClusterClient<String> clusterClient = createClient(pair, true)) {
//                    final String entryUrl = clusterClient.getWebInterfaceURL();
//                    System.out.println(clusterClient.getWebInterfaceURL());
//                    token.put(FlinkClusterTokenManager.JSON_KEY_WEB_INTERFACE_URL, entryUrl);


//                efaultK8SImage powerjobServerImage //
//            , K8SRCResNameWithFieldSelector targetResName, ReplicasSpec powerjobServerSpec, CoreV1Api
//                apiClient, NamespacedEventCallCriteria resVer

            AppsV1Api appsV1Api = flinkK8SImage.createAppsV1Api();
            CoreV1Api coreV1Api = flinkK8SImage.createCoreV1Api();

            V1Deployment deployment = appsV1Api.readNamespacedDeployment(this.clusterId, flinkK8SImage.namespace)
                    .pretty(K8SUtils.resultPrettyShow).execute();
            NamespacedEventCallCriteria evtCallCriteria = null;
            for (V1ReplicaSet rs : appsV1Api.listNamespacedReplicaSet(flinkK8SImage.namespace)
                    .pretty(K8SUtils.resultPrettyShow).execute().getItems()) {
                for (V1OwnerReference oref : rs.getMetadata().getOwnerReferences()) {
                    if (StringUtils.equals(oref.getUid(), deployment.getMetadata().getUid())) {
                        evtCallCriteria = NamespacedEventCallCriteria.createResVersion(rs.getMetadata());
                    }
                }
            }
            Objects.requireNonNull(evtCallCriteria, "evtCallCriteria can not be null");


            K8SRCResNameWithFieldSelector resSelector
                    = new K8SRCResNameWithFieldSelector(this.clusterId
                    , new OwnerJobExec<Object, NamespacedEventCallCriteria>() {
                @Override
                public NamespacedEventCallCriteria accept(Object powerJobServer) throws Exception {
                    throw new UnsupportedOperationException();
                }
            });
            WaitReplicaControllerLaunch controllerLaunch
                    = K8SUtils.waitReplicaControllerLaunch(flinkK8SImage, resSelector
                    , deployment.getSpec().getReplicas(), coreV1Api, evtCallCriteria, new ResChangeCallback() {
                        @Override
                        public void applyDefaultPodPhase(final Map<String, RunningStatus> relevantPodNames, V1Pod pod) {
                            relevantPodNames.put(pod.getMetadata().getName(), RunningStatus.SUCCESS);
                        }

                        @Override
                        public boolean shallGetExistPods() {
                            return true;
                        }
                    });


            watchPodLog = K8SDataXPowerJobServer.watchOneOfPowerJobPodLog(
                    new K8SController(flinkK8SImage, coreV1Api), controllerLaunch, new PowerJobPodLogListener() {
                        @Override
                        protected void consumePodLogMsg(ExecuteState<String> log) {
                            sse.info(clusterId, TimeFormat.getCurrentTimeStamp(), log.getMsg());
                        }
                    });


            int tryCount = 0;
            boolean hasGetJobInfo = false;

            tryGetJob:
            while (tryCount++ < 5) {
                for (JobStatusMessage jobStat : clusterClient.listJobs().get()) {
                    afterSucce.accept(jobStat.getJobId());
                    JSONObject token = createClusterMeta(FlinkClusterType.K8SApplication, clusterClient, flinkK8SImage);
                    token.put(FlinkClusterTokenManager.JSON_KEY_APP_NAME, collection.getName());
                    this.getLaunchToken(collection).appendJobNote(token);
                    hasGetJobInfo = true;
                    break tryGetJob;
                }
                Thread.sleep(3000);
            }

            if (!hasGetJobInfo) {
                throw TisException.create("has not get jobId");
            }

        } finally {
            try {
                watchPodLog.close();
            } catch (Throwable e) {

            }
        }


//                token.put(FlinkClusterTokenManager.JSON_KEY_CLUSTER_ID, clusterId);

//                token.put(FlinkClusterTokenManager.JSON_KEY_CLUSTER_TYPE, FlinkClusterType.K8SApplication.getToken());
//
//                token.put(FlinkClusterTokenManager.JSON_KEY_K8S_NAMESPACE, flinkK8SImage.getNamespace());
//                token.put(FlinkClusterTokenManager.JSON_KEY_K8S_BASE_PATH, flinkK8SImage.getK8SCfg().getKubeBasePath());
//                token.put(FlinkClusterTokenManager.JSON_KEY_K8S_ID, flinkK8SImage.identityValue());

        //  return Optional.of(token);
        //  }
        //finally {
        // Thread.currentThread().setContextClassLoader(originClassLoader);
        //  }
        // });
        // launchToken.writeLaunchToken(Optional.of(token));

    }


    private Pair<Configuration, IK8sContext> getFlinkCfg() {
        try {
            KubernetesApplicationClusterConfig k8SClusterManager = getK8SClusterCfg();
            Pair<Configuration, IK8sContext> flinkConfig
                    = Objects.requireNonNull(k8SClusterManager, "k8SClusterManager can not be null").createFlinkConfig();

            Configuration cfg = flinkConfig.getLeft();
//            cfg.set(KubernetesConfigOptions.KUBE_CONFIG_FILE
//                    , cfg.getString(KubernetesConfigOptions.FLINK_CONF_DIR)
//                            + File.separator + FlinkK8SClusterManager.CONFIG_FILE_KUBE_CONFIG);
            cfg.set(KubernetesConfigOptions.CLUSTER_ID, this.clusterId);
            return flinkConfig;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            BasicFlinkK8SClusterCfg.addClusterIdOption(opts);//.overwritePlaceholder();
        }

        public boolean validateClusterId(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return FlinkK8SClusterManager.DescriptorImpl.checkClusterId(msgHandler, context, fieldName, value);
        }


        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            KubernetesApplication k8sApp = postFormVals.newInstance();
            FlinkK8SImage flinkK8SImage = k8sApp.getFlinkK8SImage();

            try {
                V1Deployment deploy = flinkK8SImage.createAppsV1Api().readNamespacedDeployment(k8sApp.clusterId, flinkK8SImage.namespace).execute();
                if (deploy != null && StringUtils.equals(k8sApp.clusterId, deploy.getMetadata().getName())) {
                    msgHandler.addFieldError(context, KEY_CLUSTER_ID, "K8S集群中存在名称为：'" + k8sApp.clusterId + "'的Deployment资源类型，请先手动删除");
                    return false;
                }


            } catch (ApiException e) {
                logger.warn(e.getResponseBody());
            }

            return true;
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
