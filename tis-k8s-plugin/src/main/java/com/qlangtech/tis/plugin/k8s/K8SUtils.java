package com.qlangtech.tis.plugin.k8s;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.Specification;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobWorker;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1Event;
import io.kubernetes.client.openapi.models.V1HostAlias;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ObjectReference;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ReplicationController;
import io.kubernetes.client.openapi.models.V1ReplicationControllerSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.util.Watch;
import okhttp3.Call;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/13
 */
public class K8SUtils {

    public static final String REPLICATION_CONTROLLER_VERSION = "v1";
    public static final String resultPrettyShow = "true";
    public static final String LABEL_APP = "app";

    public static final ServiceResName K8S_DATAX_POWERJOB_MYSQL_SERVICE = new ServiceResName("powerjob-mysql");
    public static final ServiceResName K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE = new ServiceResName("powerjob-server-nodeport");
    public static final ServiceResName K8S_DATAX_POWERJOB_SERVER_SERVICE = new ServiceResName("powerjob-server");


    public static final PowerJobRCResName K8S_DATAX_POWERJOB_MYSQL
            = new PowerJobRCResName("datax-worker-powerjob-mysql", K8S_DATAX_POWERJOB_MYSQL_SERVICE);

    public static final PowerJobRCResName K8S_DATAX_POWERJOB_SERVER = new PowerJobRCResName("datax-worker-powerjob-server"
            , K8S_DATAX_POWERJOB_SERVER_SERVICE, K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE);

    public static final TargetResName K8S_DATAX_POWERJOB_REGISTER_ACCOUNT = new TargetResName("datax-worker-powerjob-register-account");

    public static final PowerJobRCResName K8S_DATAX_POWERJOB_WORKER = new PowerJobRCResName("datax-worker-powerjob-worker");

    public static final TargetResName[] powerJobRes //
            = new TargetResName[]{K8S_DATAX_POWERJOB_MYSQL, K8S_DATAX_POWERJOB_SERVER, K8S_DATAX_POWERJOB_REGISTER_ACCOUNT, K8S_DATAX_POWERJOB_WORKER};

    public static final List<PowerJobRCResName> getPowerJobRCRes() {
        List<PowerJobRCResName> result = Lists.newArrayList();
        for (TargetResName res : powerJobRes) {
            if (res instanceof PowerJobRCResName) {
                result.add((PowerJobRCResName) res);
            }
        }
        return result;
    }

    public static TargetResName getPowerJobReplicationControllerName(String podName) {

        for (PowerJobRCResName res : getPowerJobRCRes()) {
            if (res.isPodMatch(podName)) {
                return res;
            }
        }
        throw new IllegalStateException("podName is illegal:" + podName);
    }

    private static PowerJobRCResName targetResName(TargetResName resName) {
        for (PowerJobRCResName res : getPowerJobRCRes()) {
            if (StringUtils.equals(res.getName(), resName.getName())) {
                return res;
            }
        }
        throw new IllegalStateException("podName is illegal:" + resName.getName());
    }

    public static class PowerJobRCResName extends TargetResName {
        final Pattern patternTargetResource;
        final K8SUtils.ServiceResName[] relevantSvc;


        public PowerJobRCResName(String name) {
            this(name, new K8SUtils.ServiceResName[0]);
        }


        public PowerJobRCResName(String name, K8SUtils.ServiceResName... relevantSvc) {
            super(name);
            this.relevantSvc = relevantSvc;
            this.patternTargetResource = Pattern.compile("(" + this.getK8SResName() + ")-.+?");
        }

        public boolean isPodMatch(String podName) {
            Matcher matcher = this.patternTargetResource.matcher(podName);
            return matcher.matches();
        }

        public K8SUtils.ServiceResName[] getRelevantSvc() {
            return relevantSvc;
        }
    }

    public static String createReplicationController(final CoreV1Api api
            , final K8sImage config, TargetResName name //
            , ReplicasSpec replicasSpec, List<V1ContainerPort> exportPorts, List<V1EnvVar> envs) throws ApiException {
        V1ReplicationController newRC = createReplicationController(api, config, name, () -> new V1Container(), replicasSpec, exportPorts, envs);
        // newRC.getMetadata().
        return getResourceVersion(newRC);// newRC.getMetadata().getResourceVersion();
    }

    public static void createService(final CoreV1Api api, String namespace //
            , ServiceResName svcRes, TargetResName selector, Integer exportPort, String targetPortName
    ) throws ApiException {
        createService(api, namespace, svcRes, selector, exportPort, targetPortName, () -> {
            V1ServiceSpec svcSpec = new V1ServiceSpec();
            svcSpec.setType("ClusterIP");
            return Pair.of(svcSpec, new V1ServicePort());
        });
    }

    public static void createService(final CoreV1Api api, String namespace //
            , ServiceResName svcRes, TargetResName selector, Integer exportPort, String targetPortName
            , Supplier<Pair<V1ServiceSpec, V1ServicePort>> specCreator) throws ApiException {
        SSERunnable sse = SSERunnable.getLocal();
        boolean success = false;
        try {
            sse.info(svcRes.getName(), TimeFormat.getCurrentTimeStamp(), "start to publish service'" + svcRes.getName() + "'");
            V1Service svcBody = new V1Service();
            svcBody.apiVersion(K8SUtils.REPLICATION_CONTROLLER_VERSION);
            V1ObjectMeta meta = new V1ObjectMeta();
            meta.setName(svcRes.getName());
            svcBody.setMetadata(meta);

            V1ServiceSpec svcSpec = specCreator.get().getKey();// new V1ServiceSpec();
            //svcSpec.setType("ClusterIP");
            svcSpec.setSelector(Collections.singletonMap(K8SUtils.LABEL_APP, selector.getK8SResName()));

            V1ServicePort svcPort = specCreator.get().getRight();// new V1ServicePort();
            svcPort.setName(targetPortName);
            svcPort.setTargetPort(new IntOrString(targetPortName));
            svcPort.setPort(exportPort);
            svcPort.setProtocol("TCP");
            svcSpec.setPorts(Lists.newArrayList(svcPort));

            svcBody.setSpec(svcSpec);

            V1Service svc = api.createNamespacedService(namespace, svcBody, K8SUtils.resultPrettyShow, null, null);
            System.out.println(svc);
            sse.info(svcRes.getName(), TimeFormat.getCurrentTimeStamp(), "success to publish service'" + svcRes.getName() + "'");
            success = true;
        } finally {
            if (!success) {
                sse.info(svcRes.getName(), TimeFormat.getCurrentTimeStamp(), "faild to publish service'" + svcRes.getName() + "'");
            }
            sse.writeComplete(svcRes, success);
        }
    }

    public static String getResourceVersion(V1ReplicationController newRC) {
        return Objects.requireNonNull(newRC, "newRC can not be null").getMetadata().getResourceVersion();
    }

    /**
     * 在k8s容器容器中创建一个RC
     *
     * @param
     * @param name
     * @param replicasSpec
     * @param envs
     * @throws ApiException
     */
    public static V1ReplicationController createReplicationController(final CoreV1Api api //
            , final K8sImage config, TargetResName name, Supplier<V1Container> containerCreator //
            , ReplicasSpec replicasSpec, List<V1ContainerPort> exportPorts, List<V1EnvVar> envs) throws ApiException {
        if (replicasSpec == null) {
            throw new IllegalArgumentException("param replicasSpec can not be null");
        }
        V1ReplicationController rc = new V1ReplicationController();
        V1ReplicationControllerSpec spec = new V1ReplicationControllerSpec();
        spec.setReplicas(replicasSpec.getReplicaCount());
        V1PodTemplateSpec templateSpec = new V1PodTemplateSpec();
        V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(name.getK8SResName());
        Map<String, String> labes = Maps.newHashMap();
        labes.put(LABEL_APP, name.getK8SResName());
        meta.setLabels(labes);
        templateSpec.setMetadata(meta);
        V1PodSpec podSpec = new V1PodSpec();
        List<V1Container> containers = Lists.newArrayList();
        V1Container container = containerCreator.get();
        container.setName(name.getK8SResName());

        Objects.requireNonNull(config, "K8sImage can not be null");

        container.setImage(config.getImagePath());
        List<V1ContainerPort> ports = Lists.newArrayList();
//        V1ContainerPort port = new V1ContainerPort();
//        port.setContainerPort(8080);
//        port.setName("http");
//        port.setProtocol("TCP");

        for (V1ContainerPort port : exportPorts) {
            ports.add(port);
        }

        container.setPorts(ports);

        //V1Container c  c.setEnv(envVars);
        container.setEnv(envs);

        V1ResourceRequirements rRequirements = new V1ResourceRequirements();
        Map<String, Quantity> limitQuantityMap = Maps.newHashMap();
        limitQuantityMap.put("cpu", new Quantity(replicasSpec.getCpuLimit().literalVal()));
        limitQuantityMap.put("memory", new Quantity(replicasSpec.getMemoryLimit().literalVal()));
        rRequirements.setLimits(limitQuantityMap);
        Map<String, Quantity> requestQuantityMap = Maps.newHashMap();
        requestQuantityMap.put("cpu", new Quantity(replicasSpec.getCpuRequest().literalVal()));
        requestQuantityMap.put("memory", new Quantity(replicasSpec.getMemoryRequest().literalVal()));
        rRequirements.setRequests(requestQuantityMap);
        container.setResources(rRequirements);


        containers.add(container);
        if (containers.size() < 1) {
            throw new IllegalStateException("containers size can not small than 1");
        }

        List<HostAlias> hostAliases = config.getHostAliases();
        if (CollectionUtils.isNotEmpty(hostAliases)) {
            List<V1HostAlias> setHostAliases = Lists.newArrayList();
            V1HostAlias v1host = null;
            for (HostAlias ha : hostAliases) {
                v1host = new V1HostAlias();
                v1host.setIp(ha.getIp());
                v1host.setHostnames(ha.getHostnames());
                setHostAliases.add(v1host);
            }
            podSpec.setHostAliases(setHostAliases);
        }


        podSpec.setContainers(containers);
        templateSpec.setSpec(podSpec);
        spec.setTemplate(templateSpec);
        rc.setSpec(spec);
        rc.setApiVersion(REPLICATION_CONTROLLER_VERSION);
        meta = new V1ObjectMeta();
        meta.setName(name.getK8SResName());
        rc.setMetadata(meta);

        return api.createNamespacedReplicationController(config.getNamespace(), rc, resultPrettyShow, null, null);
    }

    public static ReplicasSpec createDftReplicasSpec() {
        ReplicasSpec mysqlRcSpec = new ReplicasSpec();
        mysqlRcSpec.setReplicaCount(1);
        mysqlRcSpec.setMemoryLimit(Specification.parse("800M"));
        mysqlRcSpec.setMemoryRequest(Specification.parse("800M"));
        mysqlRcSpec.setCpuRequest(Specification.parse("250m"));
        mysqlRcSpec.setCpuLimit(Specification.parse("250m"));
        return mysqlRcSpec;
    }

    public static ReplicasSpec createDftPowerjobServerReplicasSpec() {
        ReplicasSpec mysqlRcSpec = new ReplicasSpec();
        mysqlRcSpec.setReplicaCount(1);
        mysqlRcSpec.setMemoryLimit(Specification.parse("900M"));
        mysqlRcSpec.setMemoryRequest(Specification.parse("800M"));
        mysqlRcSpec.setCpuRequest(Specification.parse("350m"));
        mysqlRcSpec.setCpuLimit(Specification.parse("500m"));
        return mysqlRcSpec;
    }

    /**
     * 等待RC资源启动
     *
     * @param powerjobServerImage
     * @param powerjobServerSpec
     * @param apiClient
     * @param resourceVer
     * @throws ApiException
     * @throws PowerjobOrchestrateException
     */
    public static Set<String> waitReplicaControllerLaunch(DefaultK8SImage powerjobServerImage //
            , TargetResName targetResName, ReplicasSpec powerjobServerSpec, ApiClient apiClient, String resourceVer)  //
            throws ApiException, PowerjobOrchestrateException {
        SSERunnable sse = SSERunnable.getLocal();
        CoreV1Api api = new CoreV1Api(apiClient);

        final Set<String> relevantPodNames = Sets.newHashSet();
        final PowerJobRCResName powerJobRCResName = targetResName(targetResName);

        // Pattern patternTargetResource = Pattern.compile("(" + targetResName.getK8SResName() + ")-.+?");
        final int replicaCount = powerjobServerSpec.getReplicaCount();
        int tryProcessWatcherLogsCount = 0;
        processWatcherLogs:
        while (true) { // 处理 watcher SocketTimeoutException 超时的错误
            Call rcCall = api.listNamespacedEventCall(powerjobServerImage.getNamespace() //
                    , resultPrettyShow, false, null, null //
                    , null, null, resourceVer, null, true, createApiCallback());
            Watch<V1Event> rcWatch = Watch.createWatch(apiClient, rcCall, new TypeToken<Watch.Response<V1Event>>() {
            }.getType());
            V1Event evt = null;
            V1ObjectReference objRef = null;
            int podCompleteCount = 0;
            int faildCount = 0;
            String formatMessage = null;
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
                    // Matcher matcher = patternTargetResource.matcher(objRef.getName());

                    //;

                    if ("pod".equalsIgnoreCase(objRef.getKind()) && powerJobRCResName.isPodMatch(objRef.getName())) {
                        relevantPodNames.add(objRef.getName());
                        formatMessage = "reason:" + evt.getReason() + ",name:" + evt.getInvolvedObject().getName() + ",kind:" + evt.getInvolvedObject().getKind() + ",message:" + msg;
                        System.out.println();
                        sse.error(targetResName.getName(), TimeFormat.getCurrentTimeStamp(), formatMessage);
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
                        break processWatcherLogs;
                    }
                    // if ("running".equalsIgnoreCase(status.getPhase())) {

                    //}
                }
            } catch (Exception e) {
                if (tryProcessWatcherLogsCount++ < 3 //
                        && ExceptionUtils.indexOfThrowable(e, java.net.SocketTimeoutException.class) > -1) {
                    continue processWatcherLogs;
                } else {
                    throw e;
                }
            } finally {
                try {
                    rcWatch.close();
                } catch (Throwable e) {

                }
            }

            if (faildCount > 0) {
                throw new PowerjobOrchestrateException(
                        targetResName.getK8SResName() + " launch faild,faild count:" + faildCount);
            }

        }
        return relevantPodNames;
    }

    private static ApiCallback createApiCallback() {
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

    public static K8SDataXPowerJobWorker getK8SDataXPowerJobWorker() {
        return (K8SDataXPowerJobWorker)
                DataXJobWorker.getJobWorker(DataXJobWorker.K8S_DATAX_INSTANCE_NAME
                        , Optional.of(DataXJobWorker.K8SWorkerCptType.Worker));
    }

    public static class ServiceResName extends TargetResName {
        private static final String HOST_SUFFIX = "_SERVICE_HOST";
        private static final String PORT_SUFFIX = "_SERVICE_PORT";

        public ServiceResName(String name) {
            super(name);
        }

        public String getHostEvnName() {
            return replaceAndUpperCase(getName()) + HOST_SUFFIX;
        }

        public String getPortEvnName() {
            return replaceAndUpperCase(getName()) + PORT_SUFFIX;
        }

        public String getHostPortReplacement() {
            return toVarReplacement(getHostEvnName()) + ":" + toVarReplacement(getPortEvnName());
        }

        private String toVarReplacement(String val) {
            return "$(" + Objects.requireNonNull(val, "val can not be null") + ")";
        }

        private String replaceAndUpperCase(String val) {
            return StringUtils.upperCase(StringUtils.replace(val, "-", "_"));
        }
    }
}
