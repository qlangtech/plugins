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

package com.qlangtech.plugins.incr.flink.cluster;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.launch.clustertype.KubernetesApplication;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.FlinkClusterPojo;
import com.qlangtech.tis.datax.job.FlinkSessionResName;
import com.qlangtech.tis.datax.job.ILaunchingOrchestrate;
import com.qlangtech.tis.datax.job.JobResName;
import com.qlangtech.tis.datax.job.JobResName.OwnerJobExec;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterTokenManager;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.lang.ErrorValue;
import com.qlangtech.tis.lang.TisException.ErrorCode;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.plugin.k8s.K8SRCResName;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8sExceptionUtils;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.plugin.k8s.NamespacedEventCallCriteria;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.trigger.jst.ILogListener;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentStatus;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Service;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.cli.KubernetesSessionCli;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator.ConfigMapData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;

/**
 * Flink Cluster 管理
 * https://nightlies.apache.org/flink/flink-docs-master/zh/docs/deployment/resource-providers/native_kubernetes/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-04 14:30
 **/
@Public
public class FlinkK8SClusterManager extends BasicFlinkK8SClusterCfg implements ILaunchingOrchestrate, IdentityName {
    private static final Logger logger = LoggerFactory.getLogger(FlinkK8SClusterManager.class);
//    @FormField(ordinal = 0, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
//    public final String name = K8S_FLINK_CLUSTER_NAME.getName();

//    private static final K8SRCResName<FlinkK8SClusterManager> launchFlinkCluster = new K8SRCResName<FlinkK8SClusterManager>(K8SWorkerCptType.FlinkCluster
//            , (flinkManager) -> {
//        JSONObject[] clusterMeta = new JSONObject[1];
//        flinkManager.processFlinkCluster((cli) -> {
//            cli.run(new String[]{}, (clusterClient) -> {
//                clusterMeta[0] = KubernetesApplication.createClusterMeta(FlinkClusterType.K8SSession, clusterClient, flinkManager.getK8SImage());
//            });
//            SSERunnable.getLocal().run();
//        });
//        SSERunnable.getLocal().setContextAttr(JSONObject[].class, clusterMeta);
//    });

    //  public static final String CONFIG_FILE_KUBE_CONFIG = "tis-kube-config";


    @FormField(ordinal = 0, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String clusterId;

    @Override
    public String identityValue() {
        return this.clusterId;
    }

    @Override
    public ServerLaunchToken getProcessTokenFile() {
        return ServerLaunchToken.createFlinkClusterToken().token(FlinkClusterType.K8SSession, new TargetResName(this.clusterId));
    }

    @Override
    public Map<String, Object> getPayloadInfo() {
        FlinkClusterTokenManager clusterToken = ServerLaunchToken.createFlinkClusterToken();
        FlinkClusterPojo cluster = clusterToken.find(FlinkClusterType.K8SSession, this.clusterId);
        Map<String, Object> payloads = Maps.newHashMap();
        if (cluster != null) {
            payloads = Collections.singletonMap(CLUSTER_ENTRYPOINT_HOST, cluster.getWebInterfaceURL());
        }
        // try {
        // Map<String, Object> payloads = Maps.newHashMap();

//
//            ServiceExposedType serviceExposedType = ServiceExposedType.valueOf(svcExposedType);
//            switch (serviceExposedType) {
//                case NodePort:
//                    return payloads;
//                case ClusterIP:
//                    CoreV1Api coreApi = new CoreV1Api(getK8SApi());
//                    V1Service svc = coreApi.readNamespacedService(this.clusterId + "-rest" //
//                            , this.getK8SImage().getNamespace(), K8SUtils.resultPrettyShow, null, null);
//                    V1ServiceSpec spec = svc.getSpec();
//                    for (V1ServicePort port : spec.getPorts()) {
//                        payloads.put(CLUSTER_ENTRYPOINT_HOST, ) ;
//                        return payloads;
//                    }
//                case LoadBalancer:
//
//                default:
//                    throw new IllegalStateException("illegal serviceExposedType:" + serviceExposedType);
//            }
//
//            // http://192.168.64.3:31000/#/welcome
//            payloads.put(CLUSTER_ENTRYPOINT_HOST, "http://" + this.serverPortExport.getPowerjobHost() + "/#/welcome");
        return payloads;
//        } catch (ApiException e) {
//            throw K8sExceptionUtils.convert(this.clusterId, e);
//        }
    }

    @Override
    public List<ExecuteStep> getExecuteSteps() {


        K8SRCResName<FlinkK8SClusterManager> launchFlinkCluster
                = new K8SRCResName<>(K8SWorkerCptType.FlinkCluster
                , new CreateFlinkSessionJobExec());

        return Lists.newArrayList(new ExecuteStep(launchFlinkCluster, "launch Flink Cluster"));
    }

    private static class CreateFlinkSessionJobExec implements OwnerJobExec<FlinkK8SClusterManager, NamespacedEventCallCriteria> {
        @Override
        public NamespacedEventCallCriteria accept(FlinkK8SClusterManager flinkManager) throws Exception {
            JSONObject[] clusterMeta = new JSONObject[1];
            final String clusterId = flinkManager.clusterId;
            final CoreV1Api coreApi = new CoreV1Api(flinkManager.getK8SApi());
            final ServerPortExport serverPortExport = flinkManager.serverPortExport;
            flinkManager.processFlinkCluster((cli) -> {
                cli.run(false, new String[]{}, (clusterClient, externalService) -> {
                    if (externalService.isPresent()) {
                        throw new IllegalStateException(" this is create flink-session process can not get externalService ahead,clusterId:" + clusterId);
                    }
                    /**
                     * 说明刚刚新建了flink-session集群
                     * 创建TIS配套的额外的服务
                     */
                    String targetPortName = ExternalServiceDecorator.getExternalServiceName(clusterId + "-tis");

                    Pair<ServiceResName, TargetResName> serviceResAndOwner
                            = Pair.of(new ServiceResName(targetPortName, null), new TargetResName(clusterId));

                    try {
                        V1Service svc
                                = coreApi.readNamespacedService(
                                ExternalServiceDecorator.getExternalServiceName(clusterId)
                                , flinkManager.getK8SImage().getNamespace())
                                .pretty(K8SUtils.resultPrettyShow).execute();
                        V1OwnerReference ownerReference = null;
                        for (V1OwnerReference ref : svc.getMetadata().getOwnerReferences()) {
                            ownerReference = ref;
                        }
                        Objects.requireNonNull(ownerReference, "ownerReference can not be null");
                        serverPortExport.exportPort(flinkManager.getK8SImage().getNamespace()
                                , coreApi, targetPortName, serviceResAndOwner, Optional.of(ownerReference));
                    } catch (ApiException e) {
                        throw new RuntimeException(e);
                    }

                    clusterMeta[0] = KubernetesApplication.createClusterMeta(FlinkClusterType.K8SSession
                            , Optional.of("http://" + serverPortExport.getPowerjobClusterHost(
                                    coreApi, flinkManager.getK8SImage().getNamespace(), serviceResAndOwner))
                            , clusterClient, flinkManager.getK8SImage());
                    SSERunnable.getLocal().setContextAttr(JSONObject[].class, clusterMeta);
                });
                SSERunnable.getLocal().run();
            });
            return null;
        }
    }

    @Override
    public Optional<JSONObject> launchService(SSERunnable launchProcess) {

        try {
            for (ExecuteStep execStep : getExecuteSteps()) {

                JobResName subJob = execStep.getSubJob();

                subJob.execSubJob(this);
            }
        } catch (ApiException e) {
            launchProcess.error(null, TimeFormat.getCurrentTimeStamp(), e.getResponseBody());
            logger.error(e.getResponseBody(), e);
            throw K8sExceptionUtils.convert(e);
        } catch (Exception e) {
            launchProcess.error(null, TimeFormat.getCurrentTimeStamp(), e.getMessage());
            throw new RuntimeException(e);
        }

        JSONObject[] clusterMeta = Objects.requireNonNull(
                launchProcess.getContextAttr(JSONObject[].class), "clusterMeta can not be null");
//        // new JSONObject[1];
//        processFlinkCluster((cli) -> {
//            cli.run(new String[]{}, (clusterClient) -> {
//                clusterMeta[0] = KubernetesApplication.createClusterMeta(FlinkClusterType.K8SSession, clusterClient, getK8SImage());
//            });
//            launchProcess.run();
//        });
        ServerLaunchToken.createFlinkClusterToken().cleanCache();
        return Optional.of(clusterMeta[0]);
    }

    private void processFlinkCluster(KubernetesSessionCliProcess process) {

        classLoaderSetter((configuration) -> {
            // "registry.cn-hangzhou.aliyuncs.com/tis/flink-3.1.0:latest"
//            configuration.set(KubernetesConfigOptions.CONTAINER_IMAGE, k8SImageCfg.getImagePath());
//            configuration.set(KubernetesConfigOptions.CLUSTER_ID, clusterId);
//            configuration.set(KubernetesConfigOptions.NAMESPACE, k8SImageCfg.getNamespace());
            // FlinkConfMountDecorator.flinkConfigMapData =
            try {
                getCreateAccompanyConfigMapResource(configuration.getRight());
                //  final String configDir = CliFrontend.getConfigurationDirectoryFromEnv();
                final KubernetesSessionCli cli = new KubernetesSessionCli(configuration.getLeft(), "./conf");

                process.apply(cli);

                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

//        final Thread trd = Thread.currentThread();
//        final ClassLoader currentClassLoader = trd.getContextClassLoader();
//        try {
//            trd.setContextClassLoader(FlinkK8SClusterManager.class.getClassLoader());
//
//
//            final Configuration configuration = createFlinkConfig();
//
//            // "registry.cn-hangzhou.aliyuncs.com/tis/flink-3.1.0:latest"
////            configuration.set(KubernetesConfigOptions.CONTAINER_IMAGE, k8SImageCfg.getImagePath());
////            configuration.set(KubernetesConfigOptions.CLUSTER_ID, clusterId);
////            configuration.set(KubernetesConfigOptions.NAMESPACE, k8SImageCfg.getNamespace());
//            // FlinkConfMountDecorator.flinkConfigMapData =
//            getCreateAccompanyConfigMapResource();
//
//            //  final String configDir = CliFrontend.getConfigurationDirectoryFromEnv();
//
//
//            final KubernetesSessionCli cli = new KubernetesSessionCli(configuration, "./conf");
//
//            process.apply(cli);
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            trd.setContextClassLoader(currentClassLoader);
//        }
    }


    private <T> T classLoaderSetter(Function<Pair<Configuration, IK8sContext>, T> consumer) {
        final Thread trd = Thread.currentThread();
        final ClassLoader currentClassLoader = trd.getContextClassLoader();
        try {
            trd.setContextClassLoader(FlinkK8SClusterManager.class.getClassLoader());

            return consumer.apply(createFlinkConfig());

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            trd.setContextClassLoader(currentClassLoader);
        }
    }

    public ClusterClient<String> createClusterClient() {
        return classLoaderSetter((configuration) -> {
            //  DefaultClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
            try {
                final ClusterClientFactory<String> kubernetesClusterClientFactory = new KubernetesClusterClientFactory();
                // clusterClientServiceLoader.getClusterClientFactory(configuration);

                try (final ClusterDescriptor<String> kubernetesClusterDescriptor =
                             kubernetesClusterClientFactory.createClusterDescriptor(configuration.getLeft())
                ) {
                    ClusterClientProvider<String> clientProvider = kubernetesClusterDescriptor.retrieve(this.clusterId);
                    return clientProvider.getClusterClient();
                }
            } catch (ClusterRetrieveException e) {
                throw new RuntimeException(e);
            }
        });
    }


    interface KubernetesSessionCliProcess {
        void apply(KubernetesSessionCli cli) throws Exception;
    }

    private transient ApiClient apiClient;

    public ApiClient getK8SApi() {
        if (this.apiClient == null) {
            K8sImage k8SImage = this.getK8SImage();
            this.apiClient = k8SImage.createApiClient();
        }

        return this.apiClient;
    }

    @Override
    public void relaunch() {

        //args = new String[]{"--" + JobManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=1600m"};
        // CenterResource.setNotFetchFromCenterRepository();

    }

    @Override
    public void relaunch(String podName) {

    }


    @Override
    public List<RcDeployment> getRCDeployments() {
        RcDeployment deployment = new RcDeployment(DataXJobWorker.K8S_FLINK_CLUSTER_NAME.group().getK8SResName());
        K8sImage k8sImage = this.getK8SImage();
        ApiClient apiClient = k8sImage.createApiClient();

        AppsV1Api appsApi = new AppsV1Api(apiClient);
        CoreV1Api coreApi = new CoreV1Api(apiClient);
        // String name, String namespace, String pretty, Boolean exact, Boolean export
        try {
            V1Deployment deploy
                    = appsApi.readNamespacedDeployment(this.clusterId, k8sImage.getNamespace())
                    .pretty(K8SUtils.resultPrettyShow)
                    .execute();

            K8SController.fillSpecInfo(deployment, deploy.getSpec().getReplicas(), deploy.getSpec().getTemplate());

            V1DeploymentStatus status = deploy.getStatus();
            RcDeployment.ReplicationControllerStatus deploymentStatus = new RcDeployment.ReplicationControllerStatus();
            deploymentStatus.setAvailableReplicas(status.getAvailableReplicas());
            deploymentStatus.setFullyLabeledReplicas(0);
            deploymentStatus.setObservedGeneration(status.getObservedGeneration());
            deploymentStatus.setReadyReplicas(status.getReadyReplicas());
            deploymentStatus.setReplicas(status.getReplicas());
            deployment.setStatus(deploymentStatus);
            K8SController.fillCreateTimestamp(deployment, deploy.getMetadata());
            K8SController.fillPods(coreApi, k8sImage, deployment, new TargetResName(this.clusterId));
        } catch (ApiException e) {

            ErrorValue ev = ErrorValue.create(ErrorCode.FLINK_SESSION_CLUSTER_LOSS_OF_CONTACT
                    , IFullBuildContext.KEY_TARGET_NAME, FlinkSessionResName.group().getName() + "/" + this.clusterId);

            throw K8sExceptionUtils.convert(ev, this.clusterId, e);
        }

        return Collections.singletonList(deployment);
    }

    @Override
    public RcHpaStatus getHpaStatus() {
        return null;
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        K8sImage k8sImage = this.getK8SImage();
        ApiClient apiClient = k8sImage.createApiClient();
        return K8SController.listPodAndWatchLog(apiClient, k8sImage
                , "flink-main-container", new TargetResName(this.clusterId), podName, listener);
    }


    @Override
    public void remove() {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("clusterId can not be null");
        }
        if (!this.inService()) {
            throw new IllegalStateException("job worker is not in service, relevant clusterId:" + clusterId);
        }
        processFlinkCluster((cli) -> {
            cli.killCluster(clusterId);
            this.deleteLaunchToken();
            ServerLaunchToken.createFlinkClusterToken().deleteFlinkSessionCluster(clusterId);
        });
    }


    public static void getCreateAccompanyConfigMapResource(IK8sContext kubeConf) throws IOException {
        Map<String, ConfigMapData> configMap = Maps.newHashMap();
        addResFromCP(configMap, CONFIG_FILE_LOGBACK_NAME);
        addResFromCP(configMap, CONFIG_FILE_LOG4J_NAME);

//        addResFromCP(configMap, CONFIG_FILE_KUBE_CONFIG, kubeConf.getKubeConfigContent());

        FlinkConfMountDecorator.flinkConfigMapData = configMap;

        //Map<String, ConfigMapData> tisConfMap = Maps.newHashMap();
        Config.getInstance().consumeOriginSource((cfgSource) -> {
            try {
                addResFromCP(configMap
                        , StringUtils.replace(Config.bundlePathClasspath, "/", "_"), cfgSource)
                        .setPodPath(Config.bundlePathClasspath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        // FlinkConfMountDecorator.tisConfigMapData = tisConfMap;


    }

    private static void addResFromCP(Map<String, ConfigMapData> configMap, String configFileLogbackName) throws IOException {
        try (InputStream input = FlinkK8SClusterManager.class.getResourceAsStream(configFileLogbackName)) {
            addResFromCP(configMap, configFileLogbackName, input);
        }
    }

    private static ConfigMapData addResFromCP(Map<String, ConfigMapData> configMap
            , String configFileLogbackName, InputStream input) throws IOException {
        return addResFromCP(configMap, configFileLogbackName, IOUtils.toString(input, TisUTF8.get()));
    }

    private static ConfigMapData addResFromCP(Map<String, ConfigMapData> configMap
            , String configFileLogbackName, String inputContent) {
        ConfigMapData cfgMapper = new ConfigMapData(configFileLogbackName, inputContent);
        configMap.put(configFileLogbackName, cfgMapper);
        return cfgMapper;
    }

    @TISExtension()
    public static class DescriptorImpl extends BasicFlinkCfgDescriptor {
        public DescriptorImpl() {
            super();
            addClusterIdOption(opts);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.verify(msgHandler, context, postFormVals);
        }

        private static final Pattern pattern_cluster_id = Pattern.compile("[a-z][\\da-z\\-]{3,44}");

        public boolean validateClusterId(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return checkClusterId(msgHandler, context, fieldName, value);
        }

        public static boolean checkClusterId(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = pattern_cluster_id.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "not match pattern:" + pattern_cluster_id);
                return false;
            }

            return true;
        }

        //

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            FlinkK8SClusterManager clusterManager = postFormVals.newInstance();
            FlinkClusterPojo cluster = ServerLaunchToken.createFlinkClusterToken()
                    .find(FlinkClusterType.K8SSession, clusterManager.clusterId);
            if (cluster != null) {
                msgHandler.addFieldError(context, KEY_FIELD_CLUSTER_ID, IdentityName.MSG_ERROR_NAME_DUPLICATE);
                return false;
            }

            return true;
        }


        @Override
        public K8SWorkerCptType getWorkerCptType() {
            return K8SWorkerCptType.FlinkCluster;
        }
    }
}
