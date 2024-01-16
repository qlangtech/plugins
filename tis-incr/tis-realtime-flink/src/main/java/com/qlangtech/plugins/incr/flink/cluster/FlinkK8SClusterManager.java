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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.ILaunchingOrchestrate;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.plugin.k8s.K8SUtils.K8SRCResName;
import com.qlangtech.tis.plugin.k8s.K8sExceptionUtils;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.plugin.k8s.K8sImage.ImageCategory;
import com.qlangtech.tis.trigger.jst.ILogListener;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentStatus;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.cli.KubernetesSessionCli;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator.ConfigMapData;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
public class FlinkK8SClusterManager extends BasicFlinkK8SClusterCfg implements ILaunchingOrchestrate {

//    @FormField(ordinal = 0, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
//    public final String name = K8S_FLINK_CLUSTER_NAME.getName();

    private static final K8SRCResName<FlinkK8SClusterManager> launchFlinkCluster = new K8SRCResName<FlinkK8SClusterManager>(K8SWorkerCptType.FlinkCluster, (flinkManager) -> {
    });

    @FormField(ordinal = 0, identity = false, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String clusterId;


    @Override
    public ServerLaunchToken getProcessTokenFile() {
        return ServerLaunchToken.createFlinkClusterToken().token(FlinkClusterType.K8SSession, new TargetResName(this.clusterId));
    }

    @Override
    public Map<String, Object> getPayloadInfo() {
        // try {
        Map<String, Object> payloads = Maps.newHashMap();

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

        ExecuteStep step = new ExecuteStep(launchFlinkCluster, "launch Flink Cluster");

        return Lists.newArrayList(step);
    }

    @Override
    public void launchService(SSERunnable launchProcess) {
        processFlinkCluster((cli) -> {
            cli.run(new String[]{});
            launchProcess.run();
        });
    }

    private void processFlinkCluster(KubernetesSessionCliProcess process) {
        final Thread trd = Thread.currentThread();
        final ClassLoader currentClassLoader = trd.getContextClassLoader();
        try {
            trd.setContextClassLoader(FlinkK8SClusterManager.class.getClassLoader());


            final Configuration configuration = createFlinkConfig();// ((DescriptorImpl) this.getDescriptor()).opts.createFlinkCfg(this);//. GlobalConfiguration.loadConfiguration();
            //1600
            // configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(jmMemory));


            // configuration.set(  TaskManagerOptions.TOTAL_PROCESS_MEMORY, );

            //  configuration.set(  TaskManagerOptions.NUM_TASK_SLOTS );

            // configuration.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,) ;

            //  configuration.set(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT,) ;

            //1728
            // configuration.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.ofMebiBytes(tmMemory));

            // configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(tmMemory));


            // "registry.cn-hangzhou.aliyuncs.com/tis/flink-3.1.0:latest"
//            configuration.set(KubernetesConfigOptions.CONTAINER_IMAGE, k8SImageCfg.getImagePath());
//            configuration.set(KubernetesConfigOptions.CLUSTER_ID, clusterId);
//            configuration.set(KubernetesConfigOptions.NAMESPACE, k8SImageCfg.getNamespace());
            // FlinkConfMountDecorator.flinkConfigMapData =
            getCreateAccompanyConfigMapResource();

            //  final String configDir = CliFrontend.getConfigurationDirectoryFromEnv();


            final KubernetesSessionCli cli = new KubernetesSessionCli(configuration, "./conf");

            process.apply(cli);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            trd.setContextClassLoader(currentClassLoader);
        }
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
    protected ImageCategory getK8SImageCategory() {
        return k8sImage();
    }

    @Override
    public List<RcDeployment> getRCDeployments() {
        RcDeployment deployment = new RcDeployment(DataXJobWorker.K8S_FLINK_CLUSTER_NAME.getK8SResName());
        K8sImage k8sImage = this.getK8SImage();
        ApiClient apiClient = k8sImage.createApiClient();

        AppsV1Api appsApi = new AppsV1Api(apiClient);
        CoreV1Api coreApi = new CoreV1Api(apiClient);
        // String name, String namespace, String pretty, Boolean exact, Boolean export
        try {
            V1Deployment deploy
                    = appsApi.readNamespacedDeployment(this.clusterId, k8sImage.getNamespace(), "true", null, null);

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
            throw K8sExceptionUtils.convert(this.clusterId, e);
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
        });
    }


    public static void getCreateAccompanyConfigMapResource() throws IOException {
        Map<String, ConfigMapData> configMap = Maps.newHashMap();
        addResFromCP(configMap, CONFIG_FILE_LOGBACK_NAME);
        addResFromCP(configMap, CONFIG_FILE_LOG4J_NAME);
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
        //try (InputStream input = FlinkK8SClusterManager.class.getResourceAsStream(localClasspath)) {
        ConfigMapData cfgMapper = new ConfigMapData(configFileLogbackName, IOUtils.toString(input, TisUTF8.get()));
        configMap.put(configFileLogbackName, cfgMapper);
        return cfgMapper;
        //}
    }

    @TISExtension()
    public static class DescriptorImpl extends BasicFlinkCfgDescriptor {

        public DescriptorImpl() {
            super();
            addClusterIdOption(opts).overwriteDft("tis-flink-cluster");
        }

        @Override
        public K8SWorkerCptType getWorkerCptType() {
            return K8SWorkerCptType.FlinkCluster;
        }
    }
}
