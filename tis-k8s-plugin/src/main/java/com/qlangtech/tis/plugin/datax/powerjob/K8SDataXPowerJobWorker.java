package com.qlangtech.tis.plugin.datax.powerjob;

import com.google.common.collect.Lists;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.trigger.jst.ILogListener;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER_SERVICE;
import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_WORKER;

/**
 * 配置PowerJob Worker执行器
 * https://www.yuque.com/powerjob/guidence/deploy_worker
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class K8SDataXPowerJobWorker extends DataXJobWorker {


//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.host})
//    public String serverAddress;

    /**
     * "label": "WorkerPort",
     * "help": "Worker 工作端口",
     * "dftVal": "27777"
     */
    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer port;

    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {Validator.require})
    public String storeStrategy;

    public static List<Option> getStoreStrategy() {
        List<Option> opts = Lists.newArrayList();
        for (StoreStrategy ss : StoreStrategy.values()) {
            opts.add(ss.createOpt());
        }

        return opts;
    }

//    @FormField(ordinal = 7, type = FormFieldType.ENUM, validate = {Validator.require, Validator.integer})
//    public String protocol;

    @FormField(ordinal = 9, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer maxResultLength;

//    @FormField(ordinal = 11, type = FormFieldType.INPUTTEXT, advance = true, validate = {Validator.require, Validator.identity})
//    public String userContext;

    @FormField(ordinal = 13, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer maxLightweightTaskNum;

    @FormField(ordinal = 15, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer maxHeavyweightTaskNum;

    @FormField(ordinal = 17, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer healthReportInterval;

    public enum StoreStrategy {
        DISK("磁盘"),
        MEMORY("内存");
        private final String des;

        StoreStrategy(String des) {
            this.des = des;
        }

        private Option createOpt() {
            return new Option(this.des, this.name());
        }
    }

    @Override
    protected K8sImage.ImageCategory getK8SImageCategory() {
        throw new UnsupportedOperationException("shall be invoke from " + K8SDataXPowerJobServer.class);
    }





    /**
     * 启动powerjob 执行容器
     *
     * @throws ApiException
     */
    public void launchPowerjobWorker(K8SDataXPowerJobServer pjServer) throws ApiException, PowerjobOrchestrateException {
        //K8SDataXPowerJobWorker jobWorker = Objects.requireNonNull(getPowerJobWorker(), "powerjob woker can not be null");
        if (StringUtils.isEmpty(pjServer.appName)) {
            throw new IllegalArgumentException("prop appName can not be null");
        }

        // 确保powerjob app 已经创建
//        ITISPowerJob tisPowerJob = (ITISPowerJob) DataXJobWorker.getJobWorker( //
//                DataXJobWorker.K8S_DATAX_INSTANCE_NAME, Optional.of(K8SWorkerCptType.UsingExistCluster));

        // tisPowerJob.registerPowerJobApp();
        PowerJobK8SImage powerJobImage = pjServer.getImage();
        DefaultK8SImage powerjobServerImage = new DefaultK8SImage();
        powerjobServerImage.imagePath = powerJobImage.powerJobWorkerImagePath;// "registry.cn-hangzhou.aliyuncs.com/tis/powerjob-worker:4.0.0";
        powerjobServerImage.namespace = powerJobImage.getNamespace();// this.getK8SImage().getNamespace();
        ReplicasSpec replicasSpec = Objects.requireNonNull(this.getReplicasSpec(), "ReplicasSpec can not be null");


        CoreV1Api api = new CoreV1Api(pjServer.getK8SApi());

        // api.
        List<V1ContainerPort> exportPorts = Lists.newArrayList();
        V1ContainerPort port = new V1ContainerPort();
        port.setContainerPort(8081);
        port.setName("port8081");
        port.setProtocol("TCP");
        exportPorts.add(port);

        port = new V1ContainerPort();
        port.setContainerPort(Objects.requireNonNull(this.port, "worker port can not be null"));
        port.setName("worker-port");
        port.setProtocol("TCP");
        exportPorts.add(port);

        List<V1EnvVar> envs = Lists.newArrayList();
        V1EnvVar envVar = new V1EnvVar();
        envVar.setName("PARAMS");
        /**
         * param: tis-datax/executor/powerjob-worker-samples/src/main/resources/application.properties
         */
        //  --powerjob.worker.server-address=powerjob-server:7700
        final String powerJobServerHostReplacement = K8S_DATAX_POWERJOB_SERVER_SERVICE.getHostPortReplacement();

        List<Option> params = Lists.newArrayList(new Option("powerjob.worker.app-name", pjServer.appName),
                new Option("powerjob.worker.server-address", powerJobServerHostReplacement),
                new Option("powerjob.worker.port", this.port),
                new Option("powerjob.worker.store-strategy", StringUtils.lowerCase(this.storeStrategy))
                , new Option("powerjob.worker.max-result-length", this.maxResultLength));

        envVar.setValue(params.stream().map((p) -> "--" + p.getName() + "=" + p.getValue()).collect(Collectors.joining(" ")));
        envs.add(envVar);


        final String reVersion = K8SUtils.getResourceVersion(K8SUtils.createReplicationController(
                api, powerjobServerImage, K8S_DATAX_POWERJOB_WORKER, () -> {
                    V1Container container = new V1Container();
                    container.setCommand(Lists.newArrayList("sh", "-c"
                            , "chmod +x wait-for-it.sh && ./wait-for-it.sh " + powerJobServerHostReplacement
                                    + " --strict -- java " + replicasSpec.toJavaMemorySpec()
                                    + " -jar /powerjob-worker-samples.jar $PARAMS"));
                    return container;
                }, replicasSpec, exportPorts, envs));

        K8SUtils.waitReplicaControllerLaunch( //
                powerjobServerImage, K8S_DATAX_POWERJOB_WORKER, replicasSpec, pjServer.getK8SApi(), reVersion);
    }

    //private transient ApiClient apiClient;

//    public ApiClient getK8SApi() {
//        if (this.apiClient == null) {
//            K8sImage k8SImage = this.getK8SImage();
//            this.apiClient = k8SImage.createApiClient();
//        }
//
//        return this.apiClient;
//    }

    @Override
    public void relaunch() {

    }

    @Override
    public void relaunch(String podName) {

    }

    @Override
    public List<RcDeployment> getRCDeployments() {
        return null;
    }

    @Override
    public RcHpaStatus getHpaStatus() {
        return null;
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        return null;
    }


    @Override
    public void remove() {

    }

    @Override
    public void launchService(SSERunnable launchProcess) {

    }


    @TISExtension()
    public static class DescriptorImpl extends DataXJobWorker.BasicDescriptor {

        public DescriptorImpl() {
            super();
        }

        @Override
        public DataXJobWorker.K8SWorkerCptType getWorkerCptType() {
            return DataXJobWorker.K8SWorkerCptType.Worker;
        }

        @Override
        protected TargetResName getWorkerType() {
            return DataXJobWorker.K8S_DATAX_INSTANCE_NAME;
        }
    }
}
