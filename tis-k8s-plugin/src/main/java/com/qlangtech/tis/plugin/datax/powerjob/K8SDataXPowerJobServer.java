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

package com.qlangtech.tis.plugin.datax.powerjob;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.k8s.HorizontalpodAutoscaler;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.impl.coresource.EmbeddedPowerjobCoreDataSource;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.EnvVarsBuilder;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8sExceptionUtils;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.trigger.jst.ILogListener;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AutoscalingV1Api;
import io.kubernetes.client.openapi.apis.AutoscalingV2beta1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Event;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscaler;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscalerSpec;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscalerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectReference;
import io.kubernetes.client.openapi.models.V2beta1CrossVersionObjectReference;
import io.kubernetes.client.openapi.models.V2beta1HorizontalPodAutoscaler;
import io.kubernetes.client.openapi.models.V2beta1HorizontalPodAutoscalerSpec;
import io.kubernetes.client.openapi.models.V2beta1MetricSpec;
import io.kubernetes.client.openapi.models.V2beta1ResourceMetricSource;
import io.kubernetes.client.util.Watch;
import okhttp3.Call;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.qlangtech.tis.plugin.datax.powerjob.impl.coresource.EmbeddedPowerjobCoreDataSource.K8S_DATAX_POWERJOB_SERVER_SERVICE;
import static com.qlangtech.tis.plugin.k8s.K8SUtils.K8S_DATAX_POWERJOB_SERVER;


/**
 * 配置Powerjob-server启动相关需要的参数
 * https://www.yuque.com/powerjob/guidence/deploy_server <br/>
 * https://segmentfault.com/a/1190000023283434
 * <p>
 * https://www.baeldung.com/java-kubernetes-watch
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-23 18:16
 **/
@Public
public class K8SDataXPowerJobServer extends DataXJobWorker {


    private static final Logger logger = LoggerFactory.getLogger(K8SDataXPowerJobServer.class);

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public ServerPortExport serverPortExport;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String appName;

    @FormField(ordinal = 2, type = FormFieldType.PASSWORD, validate = {Validator.require, Validator.none_blank})
    public String password;

    @FormField(ordinal = 5, validate = {Validator.require})
    public PowerjobCoreDataSource coreDS;

    @FormField(ordinal = 7, validate = {Validator.require})
    public PowerJobOMS omsProfile;

    @FormField(ordinal = 9, validate = {Validator.require})
    public PowerJobOMSStorage omsStorage;

    private transient ApiClient apiClient;
    private transient K8SController k8SController;

    //    public static String getDefaultZookeeperAddress() {
//        return processDefaultHost(Config.getZKHost());
//    }
    public final PowerJobK8SImage getImage() {
        return this.getK8SImage();
    }

    @Override
    protected K8sImage.ImageCategory getK8SImageCategory() {
        return k8sImage();
    }

    private static K8sImage.ImageCategory k8sImage() {
        return K8sImage.ImageCategory.DEFAULT_POWERJOB_DESC_NAME;
    }

    @Override
    public RcHpaStatus getHpaStatus() {

        try {
            AutoscalingV1Api hpaApi = new AutoscalingV1Api(this.getK8SApi());
            K8sImage k8SImage = this.getK8SImage();
            //  String name, String namespace, String pretty
            V1HorizontalPodAutoscaler autoscaler = hpaApi.readNamespacedHorizontalPodAutoscalerStatus(
                    this.getHpaName(), k8SImage.getNamespace(), K8SUtils.resultPrettyShow);

            V1HorizontalPodAutoscalerSpec spec = autoscaler.getSpec();
            RcHpaStatus.HpaAutoscalerSpec autoscalerSpec = new RcHpaStatus.HpaAutoscalerSpec();
            autoscalerSpec.setMaxReplicas(spec.getMaxReplicas());
            autoscalerSpec.setMinReplicas(spec.getMinReplicas());
            autoscalerSpec.setTargetCPUUtilizationPercentage(spec.getTargetCPUUtilizationPercentage());

            V1HorizontalPodAutoscalerStatus status = autoscaler.getStatus();
            RcHpaStatus.HpaAutoscalerStatus autoscalerStatus = new RcHpaStatus.HpaAutoscalerStatus();
            autoscalerStatus.setCurrentCPUUtilizationPercentage(status.getCurrentCPUUtilizationPercentage());
            autoscalerStatus.setCurrentReplicas(status.getCurrentReplicas());
            autoscalerStatus.setDesiredReplicas(status.getDesiredReplicas());
            if (status.getLastScaleTime() != null) {
                autoscalerStatus.setLastScaleTime(status.getLastScaleTime().getMillis());
            }

            V1ObjectMeta metadata = autoscaler.getMetadata();
            Objects.requireNonNull(metadata, "hpa:" + this.getHpaName() + "relevant metadata can not be null");

            Map<String, String> annotations = metadata.getAnnotations();

//            [{
//                "type": "AbleToScale",
//                        "status": "True",
//                        "lastTransitionTime": "2021-06-07T03:52:46Z",
//                        "reason": "ReadyForNewScale",
//                        "message": "recommended		size matches current size "
//            }, {
//                "type ": "ScalingActive ",
//                        "status ": "True ",
//                        "lastTransitionTime": "2021 - 06 - 08 T00: 08: 17 Z ",
//                        "reason ": "ValidMetricFound ",
//                        "message ": "the	HPA was able to successfully calculate a replica count from cpu resource utilization(percentage of request)"
//            }, {
//                "type ": "ScalingLimited ",
//                        "status ": "True ",
//                        "lastTransitionTime ": "2021 - 06 - 08 T00: 12: 19 Z ",
//                        "reason ": "TooFewReplicas ",
//                        "message ": "The		desired replica count is less than the minimum replica count "
//            }]

            List<RcHpaStatus.HpaConditionEvent> conditions
                    = JSON.parseArray(annotations.get("autoscaling.alpha.kubernetes.io/conditions"), RcHpaStatus.HpaConditionEvent.class);
//            JSONObject condition = null;
//            for (int i = 0; i < conditions.size(); i++) {
//                condition = conditions.getJSONObject(i);
//                condition.get
//            }
//            [{
//                "type": "Resource",
//                 "resource": {
//                    "name": "cpu",
//                    "currentAverageUtilization": 0,
//                    "currentAverageValue": "1m"
//                }
//            }]
            List<RcHpaStatus.HpaMetrics> currentMetrics
                    = JSON.parseArray(annotations.get("autoscaling.alpha.kubernetes.io/current-metrics"), RcHpaStatus.HpaMetrics.class);

            RcHpaStatus hpaStatus = new RcHpaStatus(conditions, currentMetrics);
            hpaStatus.setAutoscalerStatus(autoscalerStatus);
            hpaStatus.setAutoscalerSpec(autoscalerSpec);


            return hpaStatus;
        } catch (ApiException e) {
            // throw new RuntimeException("code:" + e.getCode() + ",reason:" + e.getResponseBody(), e);
            throw K8sExceptionUtils.convert(e);
        }
    }

    @Override
    public void remove() {
        K8SController k8SController = getK8SController();
        //  ApiClient k8SApi = getK8SApi();
        k8SController.removeInstance(DataXJobWorker.K8S_DATAX_INSTANCE_NAME);
        try {
            if (supportHPA()) {
                K8sImage k8SImage = this.getK8SImage();
                AutoscalingV2beta1Api hpaApi = new AutoscalingV2beta1Api(this.getK8SApi());
                //            String name,
                //            String namespace,
                //            String pretty,
                //            String dryRun,
                //            Integer gracePeriodSeconds,
                //            Boolean orphanDependents,
                //            String propagationPolicy,
                //            V1DeleteOptions body
                hpaApi.deleteNamespacedHorizontalPodAutoscaler(this.getHpaName(), k8SImage.getNamespace(), K8SUtils.resultPrettyShow
                        , null, null, null, null, null);

            }
        } catch (ApiException e) {
            throw K8sExceptionUtils.convert("code:" + e.getCode(), e); //new RuntimeException("code:" + e.getCode() + ",reason:" + e.getResponseBody(), e);
        }
        this.deleteLaunchToken();
    }


    private K8SController getK8SController() {
        if (k8SController == null) {
            k8SController = new K8SController(this.getK8SImage(), this.getK8SApi());
        }
        return k8SController;
    }

    public ApiClient getK8SApi() {
        if (this.apiClient == null) {
            K8sImage k8SImage = this.getK8SImage();
            this.apiClient = k8SImage.createApiClient();
        }

        return this.apiClient;
    }

    @Override
    public void relaunch() {
        getK8SController().relaunch(DataXJobWorker.K8S_DATAX_INSTANCE_NAME);
    }

    @Override
    public void relaunch(String podName) {
        if (StringUtils.isEmpty(podName)) {
            throw new IllegalArgumentException("param podName can not be null");
        }
        getK8SController().relaunch(DataXJobWorker.K8S_DATAX_INSTANCE_NAME, podName);
    }

    @Override
    public RcDeployment getRCDeployment() {
        // ApiClient api = getK8SApi();//, K8sImage config, String tisInstanceName
        // return K8sIncrSync.getK8SDeploymentMeta(new CoreV1Api(getK8SApi()), this.getK8SImage(), K8S_INSTANCE_NAME);
        return getK8SController().getRCDeployment(DataXJobWorker.K8S_DATAX_INSTANCE_NAME);
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        return getK8SController().listPodAndWatchLog(DataXJobWorker.K8S_DATAX_INSTANCE_NAME, podName, listener);
    }

    @Override
    public void launchService(Runnable launchProcess) {
        if (inService()) {
            throw new IllegalStateException("k8s instance of:" + DataXJobWorker.KEY_FIELD_NAME + " is running can not relaunch");
        }
        try {
            // 启动服务
//            ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
//            CuratorFrameworkFactory.Builder curatorBuilder = CuratorFrameworkFactory.builder();
//            curatorBuilder.retryPolicy(retryPolicy);
            // this.client = curatorBuilder.connectString(this.zkAddress).build();


            K8sImage k8sImage = this.getK8SImage();
            // this.k8sClient = k8SImage.createApiClient();

            //  ReplicasSpec replicasSpec = this.getReplicasSpec();

            //  Objects.requireNonNull(replicasSpec, "replicasSpec can not be null");

            EnvVarsBuilder varsBuilder = new EnvVarsBuilder("tis-datax-executor") {
                @Override
                public String getAppOptions() {
                    // return "-D" + DataxUtils.DATAX_QUEUE_ZK_PATH + "=" + getZkQueuePath() + " -D" + DataxUtils.DATAX_ZK_ADDRESS + "=" + getZookeeperAddress();
                    //  return getZookeeperAddress() + " " + getZkQueuePath();
                    return " ";
                }

                @Override
                public String getExtraSysProps() {
                    return "-D" + Config.SYSTEM_KEY_LOGBACK_PATH_KEY + "=" + Config.SYSTEM_KEY_LOGBACK_PATH_VALUE;
                }

                @Override
                protected String processHost(String address) {
                    return processDefaultHost(address);
                }
            };
            //  K8sImage config, CoreV1Api api, String name, ReplicasSpec incrSpec, List< V1EnvVar > envs
            // CoreV1Api k8sV1Api = new CoreV1Api(k8sClient);
            //  K8sImage k8sImage = this.getK8SImage();
            // 1. 启动内嵌mysql？
            // https://github.com/PowerJob/PowerJob/blob/master/docker-compose.yml
            this.createMetaStoreService();

            // 2. 启动powerjob server
            this.launchPowerjobServer();

            // 3. 启动powerjob worker
            this.launchPowerjobWorker();
            // this.getK8SController().createReplicationController(DataXJobWorker.K8S_DATAX_INSTANCE_NAME, replicasSpec, varsBuilder.build());

            if (supportHPA()) {
                HorizontalpodAutoscaler hap = this.getHpa();
                createHorizontalpodAutoscaler(k8sImage, hap);
            }
            launchProcess.run();
            writeLaunchToken();

        } catch (ApiException e) {
            logger.error(e.getResponseBody(), e);
            throw K8sExceptionUtils.convert(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public final PowerJobK8SImage getPowerJobImage() {
        return this.getK8SImage();
    }

    private void createMetaStoreService() throws ApiException {

        coreDS.launchMetaStoreService(this);

        if (coreDS instanceof EmbeddedPowerjobCoreDataSource) {


        } else {
            // 需要验证外置的mysql是否可用，至少 内部该有的表是否都存在
            // 并且，需要考虑到如何将外置的mysql配置传给powerjob master组件
        }
    }

    /**
     * 启动powerjob 执行容器
     *
     * @throws ApiException
     */
    public void launchPowerjobWorker() throws ApiException {


        K8SDataXPowerJobWorker jobWorker = Objects.requireNonNull(getPowerJobWorker(), "powerjob woker can not be null");
        jobWorker.launchPowerjobWorker(this);


        //  this.getPowerJobClient()
    }

    protected K8SDataXPowerJobWorker getPowerJobWorker() {
        return (K8SDataXPowerJobWorker)
                DataXJobWorker.getJobWorker(DataXJobWorker.K8S_DATAX_INSTANCE_NAME, Optional.of(K8SWorkerCptType.Worker));
    }

    public void launchPowerjobServer() throws ApiException, PowerjobOrchestrateException {

        PowerJobK8SImage powerImage = this.getImage();

        // 2. 启动powerjob server
        DefaultK8SImage powerjobServerImage = new DefaultK8SImage();
        powerjobServerImage.imagePath = powerImage.getImagePath();// "powerjob/powerjob-server:latest";
        powerjobServerImage.namespace = powerImage.getNamespace();

        ReplicasSpec powerjobServerSpec = Objects.requireNonNull(this.getReplicasSpec(), "getReplicasSpec can not be empty");

        final String powerJobServerPort = "pj-server-port";
        List<V1ContainerPort> exportPorts = Lists.newArrayList();
        V1ContainerPort port = new V1ContainerPort();
        port.setContainerPort(Objects.requireNonNull(this.serverPortExport.serverPort, "serverPort can not be null"));
        port.setName(powerJobServerPort);
        port.setProtocol("TCP");
        exportPorts.add(port);

        port = new V1ContainerPort();
        port.setContainerPort(Objects.requireNonNull(this.omsProfile, "omsProfile can not be null").akkaPort);
        port.setProtocol("TCP");
        exportPorts.add(port);

        port = new V1ContainerPort();
        port.setContainerPort(this.omsProfile.httpPort);
        port.setProtocol("TCP");
        exportPorts.add(port);

        List<V1EnvVar> envs = Lists.newArrayList();
        V1EnvVar envVar = new V1EnvVar();
        envVar.setName("JVMOPTIONS");
        envVar.setValue(powerjobServerSpec.toJavaMemorySpec());
        envs.add(envVar);


        envVar = new V1EnvVar();
        envVar.setName("PARAMS");
        // envVar.setValue("--oms.mongodb.enable=false --spring.datasource.core.jdbc-url=jdbc:mysql://powerjob-mysql:3306/powerjob-daily?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai");
        String coreJbdcUrl = this.coreDS.createCoreJdbcUrl();
        if (StringUtils.isEmpty(coreJbdcUrl)) {
            throw new IllegalStateException("coreJbdcUrl can not be empty");
        }
        envVar.setValue("--oms.mongodb.enable=false --spring.datasource.core.jdbc-url=" + coreJbdcUrl);
        envs.add(envVar);

        final CoreV1Api api = new CoreV1Api(this.getK8SApi());
        final String resourceVer = K8SUtils.createReplicationController(
                api, powerjobServerImage, K8S_DATAX_POWERJOB_SERVER, powerjobServerSpec, exportPorts, envs);
        // api.listNamespacedPod()


        // String namespace, String pretty, Boolean allowWatchBookmarks, String _continue, String fieldSelector, String labelSelector, Integer limit, String resourceVersion, Integer timeoutSeconds, Boolean watch, ApiCallback< V1ReplicationControllerList > _callback
        //K8SUtils.LABEL_APP + "=" + K8S_DATAX_POWERJOB_SERVER.getK8SResName()
        Call rcCall = api.listNamespacedEventCall(powerjobServerImage.getNamespace(), K8SUtils.resultPrettyShow, false, null, null
                , null, null, resourceVer, null, true, createApiCallback());

        final Pattern patternPowerJobServer = Pattern.compile(K8S_DATAX_POWERJOB_SERVER.getK8SResName() + "-.+?");
        final int replicaCount = powerjobServerSpec.getReplicaCount();
        Watch<V1Event> rcWatch = Watch.createWatch(this.getK8SApi(), rcCall, new TypeToken<Watch.Response<V1Event>>() {
        }.getType());
        V1Event evt = null;
        V1ObjectReference objRef = null;
        int podCompleteCount = 0;
        int faildCount = 0;
        try {
            for (Watch.Response<V1Event> event : rcWatch) {
                System.out.println("-----------------------------------------");
                evt = event.object;
                //  System.out.println();
                // V1ObjectMeta metadata = evt.getMetadata();
                String msg = evt.getMessage();
                evt.getType();
                evt.getReason();
                objRef = evt.getInvolvedObject();
                //            evt.getInvolvedObject().getName();
                //            evt.getInvolvedObject().getKind();
                // reason:Scheduled,name:datax-worker-powerjob-server-4g4cp,kind:Pod,message:Successfully assigned default/datax-worker-powerjob-server-4g4cp to minikube
                Matcher matcher = patternPowerJobServer.matcher(objRef.getName());
                if ("pod".equalsIgnoreCase(objRef.getKind()) && matcher.matches()) {
                    System.out.println("reason:" + evt.getReason() + ",name:" + evt.getInvolvedObject().getName() + ",kind:" + evt.getInvolvedObject().getKind() + ",message:" + msg);
                    switch (StringUtils.lowerCase(evt.getReason())) {
                        case "failed":
                            faildCount++;
                        case "started":
                            podCompleteCount++;
                            break;
                        default:
                    }
                }
                if (podCompleteCount >= replicaCount) {
                    break;
                }
                // if ("running".equalsIgnoreCase(status.getPhase())) {

                //}
            }
        } finally {
            try {
                rcWatch.close();
            } catch (Throwable e) {

            }
        }

        if (faildCount > 0) {
            throw new PowerjobOrchestrateException(K8S_DATAX_POWERJOB_SERVER.getK8SResName() + " launch faild");
        }

        K8SUtils.createService(api, powerjobServerImage.getNamespace()
                , K8S_DATAX_POWERJOB_SERVER_SERVICE, K8S_DATAX_POWERJOB_SERVER, this.serverPortExport.serverPort, powerJobServerPort);

//        Call rcCall = api.listNamespacedPodCall(
//                powerjobServerImage.getNamespace(), K8SUtils.resultPrettyShow, false, null, null
//                , K8SUtils.LABEL_APP + "=" + K8S_DATAX_POWERJOB_SERVER.getK8SResName(), null, null, null, true, createApiCallback());


//        Watch<V1Pod> rcWatch = Watch.createWatch(this.getK8SApi(), rcCall, new TypeToken<Watch.Response<V1Pod>>() {
//        }.getType());
//        V1Pod pod = null;
//        for (Watch.Response<V1Pod> event : rcWatch) {
//            System.out.println("-----------------------------------------");
//            pod = event.object;
//            //  System.out.println();
//            V1ObjectMeta metadata = pod.getMetadata();
//            V1PodStatus status = pod.getStatus();
//
//            // if ("running".equalsIgnoreCase(status.getPhase())) {
//            System.out.println(metadata.getName() + "  " + status.getPhase());
//            //}
//        }

        //

        // 等待Server 启动完毕
        // 1. 创建对外暴露端口
         this.serverPortExport.exportPort(powerjobServerImage.getNamespace(), api, powerJobServerPort);
//        // 2. 初始化 app
//         ITISPowerJob tisPowerJob = (ITISPowerJob) DataXJobWorker.getJobWorker(K8S_DATAX_INSTANCE_NAME, Optional.of(K8SWorkerCptType.UsingExistCluster));
//         tisPowerJob.registerPowerJobApp();
    }

    private ApiCallback createApiCallback() {
        return new ApiCallback() {
            public void onFailure(ApiException e, int statusCode, Map responseHeaders) {
                System.out.println("fail");
                e.printStackTrace();
            }

            public void onSuccess(Object result, int statusCode, Map responseHeaders) {
                System.out.println("sucess" + statusCode + result);
            }

            public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
                System.out.println("upload");
            }

            public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
                System.out.println("download");
            }
        };
    }


    private static String processDefaultHost(String address) {
        return StringUtils.replace(address, NetUtils.LOCAL_HOST_VALUE, NetUtils.getHost());
    }

    private void createHorizontalpodAutoscaler(K8sImage k8sImage, HorizontalpodAutoscaler hap) throws Exception {
        Objects.requireNonNull(hap, "param HorizontalpodAutoscaler can not be null");

        AutoscalingV2beta1Api apiInstance = new AutoscalingV2beta1Api(this.getK8SApi());


        // String namespace = "namespace_example"; // String | object name and auth scope, such as for teams and projects
        V2beta1HorizontalPodAutoscaler body = new V2beta1HorizontalPodAutoscaler(); // V2beta1HorizontalPodAutoscaler |
        V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(getHpaName());
        body.setMetadata(meta);
        V2beta1CrossVersionObjectReference objectReference = null;
        V2beta1HorizontalPodAutoscalerSpec spec = new V2beta1HorizontalPodAutoscalerSpec();
        spec.setMaxReplicas(hap.getMaxPod());
        spec.setMinReplicas(hap.getMinPod());
        objectReference = new V2beta1CrossVersionObjectReference();
        objectReference.setApiVersion(K8SUtils.REPLICATION_CONTROLLER_VERSION);
        objectReference.setKind("ReplicationController");
        objectReference.setName(DataXJobWorker.K8S_DATAX_INSTANCE_NAME.getK8SResName());
        spec.setScaleTargetRef(objectReference);

        V2beta1MetricSpec monitorResource = new V2beta1MetricSpec();
        V2beta1ResourceMetricSource cpuResource = new V2beta1ResourceMetricSource();
        cpuResource.setName("cpu");
        cpuResource.setTargetAverageUtilization(hap.getCpuAverageUtilization());
        monitorResource.setResource(cpuResource);
        monitorResource.setType("Resource");
        spec.setMetrics(Collections.singletonList(monitorResource));
        body.setSpec(spec);


        String pretty = "pretty_example"; // String | If 'true', then the output is pretty printed.
        String dryRun = "dryRun_example"; // String | When present, indicates that modifications should not be persisted. An invalid or unrecognized dryRun directive will result in an error response and no further processing of the request. Valid values are: - All: all dry run stages will be processed
        String fieldManager = null; // String | fieldManager is a name associated with the actor or entity that is making these changes. The value must be less than or 128 characters long, and only contain printable characters, as defined by https://golang.org/pkg/unicode/#IsPrint.
        try {
            V2beta1HorizontalPodAutoscaler result = apiInstance.createNamespacedHorizontalPodAutoscaler(k8sImage.getNamespace(), body, null, null, null);
            // System.out.println(result);
            logger.info("NamespacedHorizontalPodAutoscaler created");
            logger.info(result.toString());
        } catch (ApiException e) {
            logger.error("Exception when calling AutoscalingV2beta1Api#createNamespacedHorizontalPodAutoscaler");
            logger.error("Status code: " + e.getCode());
            logger.error("Reason: " + e.getResponseBody());
            logger.error("Response headers: " + e.getResponseHeaders());
            // e.printStackTrace();
            // throw e;
            throw K8sExceptionUtils.convert("code:" + e.getCode(), e);
        }

    }

    private String getHpaName() {
        return DataXJobWorker.K8S_DATAX_INSTANCE_NAME.getK8SResName() + "-hpa";
    }


    public static final Pattern zkhost_pattern = Pattern.compile("[\\da-z]{1}[\\da-z.]+:\\d+(/[\\da-z_\\-]{1,})*");
    public static final Pattern zk_path_pattern = Pattern.compile("(/[\\da-z]{1,})+");

    @TISExtension()
    public static class DescriptorImpl extends DataXJobWorker.BasicDescriptor {

        public DescriptorImpl() {
            super();
        }

        @Override
        protected K8sImage.ImageCategory getK8SImageCategory() {
            return k8sImage();
        }

        public boolean validateZkQueuePath(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = zk_path_pattern.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "不符合规范:" + zk_path_pattern);
                return false;
            }
            return true;
        }

        public boolean validateZkAddress(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = zkhost_pattern.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "不符合规范:" + zkhost_pattern);
                return false;
            }
            return true;
        }

        @Override
        protected TargetResName getWorkerType() {
            return DataXJobWorker.K8S_DATAX_INSTANCE_NAME;
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected K8SWorkerCptType getWorkerCptType() {
            return DataXJobWorker.K8SWorkerCptType.Server;
        }
    }

}
