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
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.k8s.HorizontalpodAutoscaler;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.ILaunchingOrchestrate;
import com.qlangtech.tis.datax.job.ITISPowerJob;
import com.qlangtech.tis.datax.job.JobResName;
import com.qlangtech.tis.datax.job.JobResName.OwnerJobExec;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.datax.job.ServerLaunchLog;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.datax.job.SubJobResName;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.impl.PowerJobPodLogListener;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.plugin.k8s.K8SController.UpdatePodNumber;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8SUtils.K8SRCResName;
import com.qlangtech.tis.plugin.k8s.K8SUtils.K8SResChangeReason;
import com.qlangtech.tis.plugin.k8s.K8SUtils.WaitReplicaControllerLaunch;
import com.qlangtech.tis.plugin.k8s.K8sExceptionUtils;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.plugin.k8s.NamespacedEventCallCriteria;
import com.qlangtech.tis.plugin.k8s.ResChangeCallback;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.trigger.jst.ILogListener;
import com.qlangtech.tis.trigger.socket.ExecuteState;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AutoscalingV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1CrossVersionObjectReference;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscaler;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscalerSpec;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscalerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
public class K8SDataXPowerJobServer extends DataXJobWorker implements ITISPowerJob, ILaunchingOrchestrate {


    static final String powerJobServerPort = "pj-server-port";


    private final static DataXJobWorker.K8SWorkerCptType workerCptType = DataXJobWorker.K8SWorkerCptType.Server;

    public static final K8SRCResName<K8SDataXPowerJobServer> K8S_DATAX_POWERJOB_WORKER
            = new K8SRCResName<>(K8SWorkerCptType.Worker
            , new OwnerJobExec<K8SDataXPowerJobServer, NamespacedEventCallCriteria>() {
        @Override
        public NamespacedEventCallCriteria accept(K8SDataXPowerJobServer powerJobServer) throws Exception {
            powerJobServer.launchPowerjobWorker();
            return null;
        }
    });


    public static final SubJobResName<K8SDataXPowerJobServer> K8S_DATAX_POWERJOB_REGISTER_ACCOUNT
            = JobResName.createSubJob("PowerJob Account", (powerJobServer) -> {
        powerJobServer.powerJobRegisterAccount();
    });

//    SubJobResName("datax-worker-powerjob-register-account",) {
//        @Override
//        protected String getResourceType () {
//            return "PowerJob Account";
//        }
//    }


    public static final ServiceResName<K8SDataXPowerJobServer> K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE
            = new ServiceResName<>("powerjob-server-nodeport", (powerJobServer) -> {
        powerJobServer.serverPortExport.exportPort(powerJobServer.getImage().getNamespace(), (powerJobServer.getK8SApi()), powerJobServerPort);
    });
    public static final ServiceResName<K8SDataXPowerJobServer> K8S_DATAX_POWERJOB_MYSQL_SERVICE
            = new ServiceResName<>("powerjob-mysql", (powerJobServer) -> {
        powerJobServer.coreDS.launchMetaStoreService(powerJobServer);
    });

    public static final ServiceResName<K8SDataXPowerJobServer> K8S_DATAX_POWERJOB_SERVER_SERVICE
            = new ServiceResName<K8SDataXPowerJobServer>("powerjob-server-service", (powerJobServer) -> {
        K8SUtils.createService((powerJobServer.getK8SApi()), powerJobServer.getImage().getNamespace()
                , getPowerJobServerService(), getPowerJobServerRes(), powerJobServer.serverPortExport.serverPort, powerJobServerPort);


    });

    public static final K8SRCResName<K8SDataXPowerJobServer> K8S_DATAX_POWERJOB_SERVER
            = new K8SRCResName<>(K8SWorkerCptType.Server,
            new OwnerJobExec<K8SDataXPowerJobServer, NamespacedEventCallCriteria>() {
                @Override
                public NamespacedEventCallCriteria accept(K8SDataXPowerJobServer powerJobServer) throws Exception {
                    return powerJobServer.launchPowerjobServer();
                }
            }

            , K8S_DATAX_POWERJOB_SERVER_SERVICE, K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE);

    private static K8SRCResName<K8SDataXPowerJobServer> getPowerJobServerRes() {
        return K8S_DATAX_POWERJOB_SERVER;
    }

    private static ServiceResName<K8SDataXPowerJobServer> getPowerJobServerService() {
        return K8S_DATAX_POWERJOB_SERVER_SERVICE;
    }


    public static final K8SRCResName<K8SDataXPowerJobServer> K8S_DATAX_POWERJOB_MYSQL
            = new K8SRCResName<>("datax-worker-powerjob-mysql",
            new OwnerJobExec<K8SDataXPowerJobServer, NamespacedEventCallCriteria>() {
                @Override
                public NamespacedEventCallCriteria accept(K8SDataXPowerJobServer powerJobServer) throws Exception {
                    return powerJobServer.coreDS.launchMetaStore(powerJobServer);
                }
            }
            , K8S_DATAX_POWERJOB_MYSQL_SERVICE);

    public static final JobResName[] powerJobRes //
            = new JobResName[]{
            K8S_DATAX_POWERJOB_MYSQL
            , K8S_DATAX_POWERJOB_SERVER
            , K8S_DATAX_POWERJOB_REGISTER_ACCOUNT
            , K8S_DATAX_POWERJOB_WORKER};

    private static final Logger logger = LoggerFactory.getLogger(K8SDataXPowerJobServer.class);

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public ServerPortExport serverPortExport;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String appName;

    @FormField(ordinal = 4, type = FormFieldType.PASSWORD, validate = {Validator.require, Validator.none_blank})
    public String password;

    @FormField(ordinal = 5, validate = {Validator.require})
    public PowerjobCoreDataSource coreDS;

    @FormField(ordinal = 7, validate = {Validator.require})
    public PowerJobOMS omsProfile;

//    @FormField(ordinal = 9, validate = {Validator.require})
//    public PowerJobOMSStorage omsStorage;

    private transient CoreV1Api apiClient;


    public String getPowerJobMasterGateway() {
        final String linkHost = this.serverPortExport
                .getPowerjobClusterHost(this.getK8SApi(), this.getImage().getNamespace());
        return linkHost;
    }

    public CoreV1Api getK8SApi() {
        if (this.apiClient == null) {
            K8sImage k8SImage = this.getK8SImage();
            this.apiClient = new CoreV1Api(k8SImage.createApiClient());
        }

        return this.apiClient;
    }

    private transient K8SController k8SController;

    //    public static String getDefaultZookeeperAddress() {
//        return processDefaultHost(Config.getZKHost());
//    }
    private transient TISPowerJobClient powerJobClient;

    @Override
    public TISPowerJobClient getPowerJobClient() {
        if (powerJobClient == null) {
            powerJobClient = TISPowerJobClient.create(
                    this.serverPortExport.getPowerjobClusterHost(this.getK8SApi(), this.getK8SImage().getNamespace())
                    , this.appName, this.password);
        }
        return powerJobClient;
    }

    @Override
    public Map<String, Object> getPayloadInfo() {
        Map<String, Object> payloads = Maps.newHashMap();
        // http://192.168.64.3:31000/#/welcome
        payloads.put(CLUSTER_ENTRYPOINT_HOST, "http://" + this.serverPortExport.getPowerjobExternalHost(this.getK8SApi(), this.getK8SImage().getNamespace()) + "/#/welcome");
        return payloads;
    }

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
    public List<ExecuteStep> getExecuteSteps() {
        List<ExecuteStep> launchSteps = Lists.newArrayList();

        for (JobResName rcRes : powerJobRes) {

            launchSteps.add(new ExecuteStep(rcRes, null));
            if (rcRes instanceof K8SRCResName) {
                for (ServiceResName svc : ((K8SRCResName) rcRes).getRelevantSvc()) {
                    launchSteps.add(new ExecuteStep(svc, null));
                }
            }
        }

        return launchSteps;
    }

    /**
     * 对运行中的RC进行扩缩容
     *
     * @param podNum
     */
    @Override
    public void updatePodNumber(SSERunnable sse, TargetResName cptType, Integer podNum) {
        // super.updatePodNumber(podNum);
        if (podNum < 1) {
            throw new IllegalArgumentException("illegal param podNum can not small than podNum");
        }
        // SSERunnable sse = SSERunnable.getLocal();
        try {
            UpdatePodNumber updatePodNumber = this.getK8SController().updatePodNumber(cptType, podNum);

            int replicaChangeCount = updatePodNumber.getReplicaChangeCount();
            if (replicaChangeCount < 1) {
                // TODO 没有变化
                return;
            }

            K8SUtils.waitReplicaControllerLaunch(this.getImage()
                    , cptType, replicaChangeCount, this.getK8SApi(), updatePodNumber.getResourceVersion(), new ResChangeCallback() {
                        @Override
                        public void apply(K8SResChangeReason changeReason, String podName) {


                            boolean kill = false;
                            boolean rcChange = false;
                            switch (changeReason) {
                                case SuccessfulDelete:
                                    kill = true;
                                case SuccessfulCreate:
                                    rcChange = true;
                                default:
                                    if (rcChange) {
                                        try {
                                            Thread.sleep(3000);
                                        } catch (Exception e) {

                                        }
                                        sse.writeComplete(new TargetResName(podName + (kill ? '-' : '+')), true);
                                    }

//                                case FailedScheduling:
//                                case FAILD:
//                                    sse.writeComplete(new TargetResName(podName), false);
//                                    break;
//                                case LAUNCHED:
//                                    kill = false;
//                                case KILL:
//                                    sse.writeComplete(new TargetResName(podName + (kill ? '-' : '+')), true);

                            }
                        }

//                        @Override
//                        public boolean isBreakEventWatch(int podFaildCount, int podCompleteCount, int rcCompleteCount, int expectResChangeCount) {
//                            return rcCompleteCount >= expectResChangeCount;
//                        }
                    });


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public RcHpaStatus getHpaStatus() {

        try {
            AutoscalingV1Api hpaApi = new AutoscalingV1Api(this.getK8SApi().getApiClient());
            K8sImage k8SImage = this.getK8SImage();
            //  String name, String namespace, String pretty

            V1HorizontalPodAutoscaler autoscaler
                    = hpaApi.readNamespacedHorizontalPodAutoscalerStatus(
                    this.getHpaName(), k8SImage.getNamespace())
                    .pretty(K8SUtils.resultPrettyShow).execute();

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
                autoscalerStatus.setLastScaleTime(status.getLastScaleTime().toInstant().toEpochMilli());
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
        // k8SController.removeInstance(DataXJobWorker.K8S_DATAX_INSTANCE_NAME);
        try {

            for (K8SRCResName rcResName : K8SUtils.getPowerJobRCRes()) {
                // 删除RC

                try {
                    k8SController.removeInstance(rcResName);
                } catch (Exception e) {
                    logger.warn("delete rc faild:" + rcResName.getK8SResName(), e);
                }
                // 删除服务
                for (ServiceResName svc : rcResName.getRelevantSvc()) {
                    try {
                        k8SController.deleteSerivce(svc);
                    } catch (Exception e) {
                        logger.warn("delete svc faild:" + svc.getK8SResName(), e);
                    }
                }
            }

            if (supportHPA()) {
                K8sImage k8SImage = this.getK8SImage();
                AutoscalingV1Api hpaApi = new AutoscalingV1Api(this.getK8SApi().getApiClient());
                //            String name,
                //            String namespace,
                //            String pretty,
                //            String dryRun,
                //            Integer gracePeriodSeconds,
                //            Boolean orphanDependents,
                //            String propagationPolicy,
                //            V1DeleteOptions body
                hpaApi.deleteNamespacedHorizontalPodAutoscaler(this.getHpaName(), k8SImage.getNamespace())
                        .pretty(K8SUtils.resultPrettyShow)
                        .execute();

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


    @Override
    public void relaunch() {
        getK8SController().relaunch(DataXJobWorker.K8S_DATAX_INSTANCE_NAME);
    }

    @Override
    public void relaunch(String podName) {
        if (StringUtils.isEmpty(podName)) {
            throw new IllegalArgumentException("param podName can not be null");
        }
        TargetResName powerJobRCName = K8SUtils.getPowerJobReplicationControllerName(podName);
        getK8SController().relaunch(powerJobRCName, podName);
    }

    @Override
    public List<RcDeployment> getRCDeployments() {
        // ApiClient api = getK8SApi();//, K8sImage config, String tisInstanceName
        // return K8sIncrSync.getK8SDeploymentMeta(new CoreV1Api(getK8SApi()), this.getK8SImage(), K8S_INSTANCE_NAME);
        K8SController k8SController = getK8SController();
        RcDeployment powerjobServer = k8SController.getRCDeployment(K8S_DATAX_POWERJOB_SERVER);
        if (powerjobServer == null) {
            throw TisException.create("the powerJob has been loss of communication");
        }
        powerjobServer.setReplicaScalable(false);

        RcDeployment powerjobWorker = k8SController.getRCDeployment(K8S_DATAX_POWERJOB_WORKER);
        powerjobWorker.setReplicaScalable(true);
        ServerLaunchToken workerPodsTokenFile
                = this.getProcessTokenFile(K8S_DATAX_POWERJOB_WORKER, true, K8SWorkerCptType.K8SPods);
        ServerLaunchLog serverLaunchLog = workerPodsTokenFile.buildWALLog(Collections.emptyList());
        powerjobWorker.setRcScalaLog(serverLaunchLog);

        ArrayList<RcDeployment> rcs = Lists.newArrayList(powerjobServer, powerjobWorker);
        RcDeployment mysqlRC = this.coreDS.getRCDeployment(k8SController);
        if (mysqlRC != null) {
            rcs.add(mysqlRC);
        }
        return rcs;
        // return getK8SController().getRCDeployment(DataXJobWorker.K8S_DATAX_INSTANCE_NAME);
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        return getK8SController().listPodAndWatchLog(DataXJobWorker.K8S_DATAX_INSTANCE_NAME, podName, listener);
    }

    @Override
    public Optional<JSONObject> launchService(SSERunnable launchProcess) {
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

            for (ExecuteStep execStep : getExecuteSteps()) {
                execStep.getSubJob().execSubJob(this);
            }

            // this.k8sClient = k8SImage.createApiClient();

            //  ReplicasSpec replicasSpec = this.getReplicasSpec();

            //  Objects.requireNonNull(replicasSpec, "replicasSpec can not be null");

//            EnvVarsBuilder varsBuilder = new EnvVarsBuilder("tis-datax-executor") {
//                @Override
//                public String getAppOptions() {
//                    // return "-D" + DataxUtils.DATAX_QUEUE_ZK_PATH + "=" + getZkQueuePath() + " -D" + DataxUtils.DATAX_ZK_ADDRESS + "=" + getZookeeperAddress();
//                    //  return getZookeeperAddress() + " " + getZkQueuePath();
//                    return " ";
//                }
//
//                @Override
//                public String getExtraSysProps() {
//                    return "-D" + Config.SYSTEM_KEY_LOGBACK_PATH_KEY + "=" + Config.SYSTEM_KEY_LOGBACK_PATH_VALUE;
//                }
//
//                @Override
//                protected String processHost(String address) {
//                    return processDefaultHost(address);
//                }
//            };
            //  K8sImage config, CoreV1Api api, String name, ReplicasSpec incrSpec, List< V1EnvVar > envs
            // CoreV1Api k8sV1Api = new CoreV1Api(k8sClient);
            //  K8sImage k8sImage = this.getK8SImage();
            // 1. 启动内嵌mysql？
            // https://github.com/PowerJob/PowerJob/blob/master/docker-compose.yml
            boolean success = false;
            //try {
            // this.createMetaStoreService();
//                success = true;
//            } finally {
//                launchProcess.writeComplete(K8SUtils.K8S_DATAX_POWERJOB_MYSQL, success);
//            }


            // 2. 启动powerjob server
            //  this.launchPowerjobServer();


//            success = false;
//            try {
//                // 3. 启动powerjob worker
//                this.launchPowerjobWorker();
//                success = true;
//                // this.getK8SController().createReplicationController(DataXJobWorker.K8S_DATAX_INSTANCE_NAME, replicasSpec, varsBuilder.build());
//            } finally {
//                launchProcess.writeComplete(K8S_DATAX_POWERJOB_WORKER, success);
//            }
            if (supportHPA()) {
                HorizontalpodAutoscaler hap = this.getHpa();
                createHorizontalpodAutoscaler(k8sImage, hap);
            }
            launchProcess.run();
            // writeLaunchToken();
            return Optional.empty();
        } catch (ApiException e) {
            launchProcess.error(null, TimeFormat.getCurrentTimeStamp(), e.getResponseBody());
            logger.error(e.getResponseBody(), e);
            throw K8sExceptionUtils.convert(e);
        } catch (Exception e) {
            launchProcess.error(null, TimeFormat.getCurrentTimeStamp(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public final PowerJobK8SImage getPowerJobImage() {
        return this.getK8SImage();
    }

    // private void createMetaStoreService() throws ApiException, PowerjobOrchestrateException {


//        if (coreDS instanceof EmbeddedPowerjobCoreDataSource) {
//
//
//        } else {
//            // 需要验证外置的mysql是否可用，至少 内部该有的表是否都存在
//            // 并且，需要考虑到如何将外置的mysql配置传给powerjob master组件
//        }
    // }

    /**
     * 启动powerjob 执行容器
     *
     * @throws ApiException
     */
    public void launchPowerjobWorker() throws ApiException, PowerjobOrchestrateException {


        K8SDataXPowerJobWorker jobWorker = Objects.requireNonNull(getPowerJobWorker(), "powerjob woker can not be null");
        jobWorker.launchPowerjobWorker(this);


        //  this.getPowerJobClient()
    }

    protected K8SDataXPowerJobWorker getPowerJobWorker() {
        return K8SUtils.getK8SDataXPowerJobWorker();
    }


    public NamespacedEventCallCriteria launchPowerjobServer() throws ApiException, PowerjobOrchestrateException {
        SSERunnable sse = SSERunnable.getLocal();

        PowerJobK8SImage powerImage = this.getImage();
        final CoreV1Api api = this.getK8SApi();
        DefaultK8SImage powerjobServerImage = new DefaultK8SImage();
        powerjobServerImage.imagePath = powerImage.getImagePath();
        powerjobServerImage.namespace = powerImage.getNamespace();

        // boolean success = false;
        WaitReplicaControllerLaunch relevantPodNames = null;
        NamespacedEventCallCriteria resourceVer = null;
        try {

            // 2. 启动powerjob server
            ReplicasSpec powerjobServerSpec = Objects.requireNonNull(this.getReplicasSpec(), "getReplicasSpec can not be empty");


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
            String coreJbdcParams = this.coreDS.createCoreJdbcParams();
            if (StringUtils.isEmpty(coreJbdcParams)) {
                throw new IllegalStateException("coreJbdcUrl can not be empty");
            }
            // --spring.datasource.core.jdbc-url=
            envVar.setValue("--oms.mongodb.enable=false " + coreJbdcParams);
            envs.add(envVar);


            resourceVer = K8SUtils.createReplicationController(
                    api, powerjobServerImage, K8S_DATAX_POWERJOB_SERVER, powerjobServerSpec, exportPorts, envs);
            // api.listNamespacedPod()


            ///////////////////////////
//            System.out.println("resourceVer.getResourceVersion():" + resourceVer.getResourceVersion());
//            // test
//            Watch<V1Pod> rcWatch = Watch.createWatch(api.getApiClient()
//                    //
//                    ,
//                    this.getK8SApi().listNamespacedPod(powerjobServerImage.getNamespace()).allowWatchBookmarks(false).watch(true)
//                            .resourceVersion(resourceVer.getResourceVersion())
//                            .buildCall(K8SUtils.createApiCallback())
//
//                    //
//                    , new TypeToken<Response<V1Pod>>() {
//                    }.getType());
//            try {
//                for (Watch.Response<V1Pod> event : rcWatch) {
//
//                    System.out.println("type:" + event.type + ",object:" + event.object.getMetadata().getName());
//                }
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }


            //////////////////////////////


            // String namespace, String pretty, Boolean allowWatchBookmarks, String _continue, String fieldSelector, String labelSelector, Integer limit, String resourceVersion, Integer timeoutSeconds, Boolean watch, ApiCallback< V1ReplicationControllerList > _callback
            //K8SUtils.LABEL_APP + "=" + K8S_DATAX_POWERJOB_SERVER.getK8SResName()
            relevantPodNames = K8SUtils.waitReplicaControllerLaunch(
                    powerjobServerImage, K8S_DATAX_POWERJOB_SERVER, powerjobServerSpec, this.getK8SApi(), resourceVer);

            // sse.info(K8S_DATAX_POWERJOB_SERVER.getName(), TimeFormat.getCurrentTimeStamp(), " successful publish '" + K8S_DATAX_POWERJOB_SERVER.getK8SResName() + "'");
            //  success = true;
        } catch (Exception e) {
            // sse.error(K8S_DATAX_POWERJOB_SERVER.getName(), TimeFormat.getCurrentTimeStamp(), e.getMessage());
            throw e;
        } finally {
            //  sse.writeComplete(K8S_DATAX_POWERJOB_SERVER, success);
        }

        relevantPodNames.validate();


        WatchPodLog watchPodLog = watchOneOfPowerJobPodLog(relevantPodNames, new PowerJobPodLogListener() {
            @Override
            protected void consumePodLogMsg(ExecuteState<String> log) {
                sse.info(K8S_DATAX_POWERJOB_SERVER.getName(), TimeFormat.getCurrentTimeStamp(), log.getMsg());
            }
        });
        sse.setContextAttr(WatchPodLog.class, watchPodLog);
        //   this.k8SController.listPodAndWatchLog();

//        K8SUtils.createService(api, powerjobServerImage.getNamespace()
//                , K8S_DATAX_POWERJOB_SERVER_SERVICE, K8S_DATAX_POWERJOB_SERVER, this.serverPortExport.serverPort, powerJobServerPort);


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
        return resourceVer;
        // 等待Server 启动完毕
        // 1. 创建对外暴露端口
        //  this.serverPortExport.exportPort(powerjobServerImage.getNamespace(), api, powerJobServerPort);
//      // 2. 初始化 app

        //  powerJobRegisterAccount();

    }

    private void powerJobRegisterAccount() {
        SSERunnable sse = SSERunnable.getLocal();

        WatchPodLog watchPodLog = sse.getContextAttr(WatchPodLog.class);
//        boolean success;
//        success = false;
        try {

            this.coreDS.waitServerLaunchedAndInitialPowerjobAccount(this);
//            int tryCount = 0;
//            final int tryLimit = 100;
//            while (true) {
//                try {
//                    //this.serverPortExport.initialPowerjobAccount(this);
//
//                    // 需要打印日志
//                    logger.info("success initialPowerjobAccount");
//                    sse.info(K8S_DATAX_POWERJOB_REGISTER_ACCOUNT.getName(), TimeFormat.getCurrentTimeStamp(), "success initialPowerjobAccount with appName:" + this.appName);
//                    break;
//                } catch (Exception e) {
//                    int idxOfError = -1;
//                    if (tryCount++ < tryLimit && ( //
//                            (idxOfError = ExceptionUtils.indexOfThrowable(e, java.net.SocketTimeoutException.class)) > -1 ||
//                                    (idxOfError = ExceptionUtils.indexOfThrowable(e, java.net.ConnectException.class)) > -1)
//                    ) {
//
//                        // 说明是超时等待片刻即可
//                        logger.warn("contain " + ExceptionUtils.getThrowableList(e).get(idxOfError).getMessage()
//                                + " tryCount:" + tryCount + " " + e.getMessage());
//
//                        try {
//                            Thread.sleep(4500);
//                        } catch (InterruptedException ex) {
//                            throw new RuntimeException(ex);
//                        }
//                    } else {
//                        // 其他日志直接中断了
//                        throw new RuntimeException(e);
//                    }
//                }
//            }
            //  success = true;
        } finally {
            if (watchPodLog != null) {
                watchPodLog.close();
            }
            //  sse.writeComplete(K8S_DATAX_POWERJOB_REGISTER_ACCOUNT, success);
        }
    }

    WatchPodLog watchOneOfPowerJobPodLog(WaitReplicaControllerLaunch relevantPodNames, PowerJobPodLogListener logListener) {
        if (relevantPodNames.isSkipWaittingPhase()) {
            return new WatchPodLog() {
                @Override
                public void addListener(ILogListener listener) {
                }

                @Override
                public void close() {
                }
            };
        }
        WatchPodLog watchPodLog = null;
        for (String onePodOf : relevantPodNames.getRelevantPodNames()) {
            watchPodLog = this.getK8SController().listPodAndWatchLog(K8S_DATAX_POWERJOB_SERVER, onePodOf, logListener);
            return watchPodLog;
        }
        throw new IllegalStateException("must return a watchPodLog instance");
    }


    private static String processDefaultHost(String address) {
        return StringUtils.replace(address, NetUtils.LOCAL_HOST_VALUE, NetUtils.getHost());
    }

    private void createHorizontalpodAutoscaler(K8sImage k8sImage, HorizontalpodAutoscaler hap) throws Exception {
        Objects.requireNonNull(hap, "param HorizontalpodAutoscaler can not be null");

        AutoscalingV1Api apiInstance = new AutoscalingV1Api(this.getK8SApi().getApiClient());


        // String namespace = "namespace_example"; // String | object name and auth scope, such as for teams and projects
        V1HorizontalPodAutoscaler body = new V1HorizontalPodAutoscaler(); // V2beta1HorizontalPodAutoscaler |
        V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(getHpaName());
        body.setMetadata(meta);
        V1CrossVersionObjectReference objectReference = null;
        V1HorizontalPodAutoscalerSpec spec = new V1HorizontalPodAutoscalerSpec();
        spec.setMaxReplicas(hap.getMaxPod());
        spec.setMinReplicas(hap.getMinPod());
        objectReference = new V1CrossVersionObjectReference();
        objectReference.setApiVersion(K8SUtils.REPLICATION_CONTROLLER_VERSION);
        objectReference.setKind("ReplicationController");
        objectReference.setName(DataXJobWorker.K8S_DATAX_INSTANCE_NAME.getK8SResName());
        spec.setScaleTargetRef(objectReference);

//        V1MetricSpec monitorResource = new V1MetricSpec();
//        V1ResourceMetricSource cpuResource = new V2beta1ResourceMetricSource();
//        cpuResource.setName("cpu");
//        cpuResource.setTargetAverageUtilization(hap.getCpuAverageUtilization());
//        monitorResource.setResource(cpuResource);
//        monitorResource.setType("Resource");
        spec.setTargetCPUUtilizationPercentage(hap.getCpuAverageUtilization());
        //  spec.setMetrics(Collections.singletonList(monitorResource));
        body.setSpec(spec);


        String pretty = "pretty_example"; // String | If 'true', then the output is pretty printed.
        String dryRun = "dryRun_example"; // String | When present, indicates that modifications should not be persisted. An invalid or unrecognized dryRun directive will result in an error response and no further processing of the request. Valid values are: - All: all dry run stages will be processed
        String fieldManager = null; // String | fieldManager is a name associated with the actor or entity that is making these changes. The value must be less than or 128 characters long, and only contain printable characters, as defined by https://golang.org/pkg/unicode/#IsPrint.
        try {
            V1HorizontalPodAutoscaler result = apiInstance.createNamespacedHorizontalPodAutoscaler(k8sImage.getNamespace(), body).execute();
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
    public static class DescriptorImpl extends DataXJobWorker.BasicDescriptor implements IEndTypeGetter {

        public DescriptorImpl() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.PowerJob;
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
        public K8SWorkerCptType getWorkerCptType() {
            return workerCptType;
        }
    }

}
