package com.qlangtech.tis.plugin.k8s;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.Specification;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.OwnerJobResName;
import com.qlangtech.tis.datax.job.OwnerJobResName.SSEExecuteOwner;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8SRCResNameWithFieldSelector;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobWorker;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort.ServiceType;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiCallback;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1HostAlias;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ReplicationController;
import io.kubernetes.client.openapi.models.V1ReplicationControllerSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER;

//import okhttp3;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/13
 */
public class K8SUtils {
    private static final Logger logger = LoggerFactory.getLogger(K8SUtils.class);
    public static final String REPLICATION_CONTROLLER_VERSION = "v1";
    public static final String REPLICATION_CONTROLLER_KIND = "ReplicationController";
    public static final String resultPrettyShow = "true";
    public static final String LABEL_APP = "app";
    public static final String LABEL_APP_TIMESTAMP = "appTimestamp";


    public static final List<K8SRCResName> getPowerJobRCRes() {
        List<K8SRCResName> result = Lists.newArrayList();
        for (TargetResName res : K8SDataXPowerJobServer.powerJobRes) {
            if (res instanceof K8SRCResName) {
                result.add((K8SRCResName) res);
            }
        }
        return result;
    }

    public static TargetResName getPowerJobReplicationControllerName(String podName) {

        for (K8SRCResName res : getPowerJobRCRes()) {
            if (res.isPodMatch(podName)) {
                return res;
            }
        }
        throw new IllegalStateException("podName is illegal:" + podName);
    }

    public static K8SRCResName targetResName(TargetResName resName) {
        for (K8SRCResName res : getPowerJobRCRes()) {
            if (StringUtils.equals(res.getName(), resName.getName())) {
                return res;
            }
        }
        throw new IllegalStateException("podName is illegal:" + resName.getName());
    }

    //    public interface SubJobExec<T> {
//        public void accept(T t) throws Exception;
//    }

    public static NamespacedEventCallCriteria createReplicationController(final CoreV1Api api
            , final K8sImage config, TargetResName name //
            , ReplicasSpec replicasSpec, List<V1ContainerPort> exportPorts, List<V1EnvVar> envs) throws ApiException {
        return createReplicationController(api, config, name, () -> new V1Container(), replicasSpec, exportPorts, envs);
        // newRC.getMetadata().
        // return createResVersion(newRC);// newRC.getMetadata().getResourceVersion();
    }

    public static void createService(final CoreV1Api api, String namespace //
            , ServiceResName svcRes, TargetResName selector, Integer exportPort, String targetPortName
    ) throws ApiException {

//        () -> {
//            V1ServiceSpec svcSpec = new V1ServiceSpec();
//            svcSpec.setType("ClusterIP");
//            return Pair.of(svcSpec, new V1ServicePort());
//        }

        V1ServiceSpec svcSpec = new V1ServiceSpec();
        svcSpec.setType(ServiceType.ClusterIP.token);
        createService(api, namespace, svcRes, selector, exportPort, targetPortName, svcSpec, new V1ServicePort());
    }

    public static ServiceResName createService(final CoreV1Api api, String namespace //
            , ServiceResName svcRes, TargetResName selector, Integer exportPort, String targetPortName
            , V1ServiceSpec svcSpec, V1ServicePort svcPort) throws ApiException {
        return createService(api, namespace, svcRes, selector, exportPort, targetPortName, new IntOrString(targetPortName), Optional.empty(), svcSpec, svcPort);
    }

    /**
     * @param api
     * @param namespace
     * @param svcRes
     * @param selector
     * @param exportPort     服务对于外暴露端口
     * @param targetPortName
     * @param targetPort     容器端口
     * @param svcSpec
     * @param svcPort
     * @return
     * @throws ApiException
     */
    public static ServiceResName createService(final CoreV1Api api, String namespace //
            , ServiceResName svcRes, TargetResName selector, Integer exportPort, String targetPortName, IntOrString targetPort
            , Optional<V1OwnerReference> ownerRef, V1ServiceSpec svcSpec, V1ServicePort svcPort) throws ApiException {
        // SSERunnable sse = SSERunnable.getLocal();
        // boolean success = false;
        Objects.requireNonNull(svcSpec, "param svcSpec can not be null");
        Objects.requireNonNull(svcPort, "param servicePort can not be null");
        try {

            // SSERunnable sse = SSERunnable.getLocal();


            V1Service svcBody = new V1Service();
            svcBody.apiVersion(K8SUtils.REPLICATION_CONTROLLER_VERSION);
            V1ObjectMeta meta = new V1ObjectMeta();
            meta.setName(svcRes.getName());

            // V1OwnerReference ownerRef = createOwnerReference();
            List<V1OwnerReference> ownerRefs = Collections.singletonList(ownerRef.orElseGet(() -> createOwnerReference()));
            meta.setOwnerReferences(ownerRefs);
            svcBody.setMetadata(meta);

            // V1ServiceSpec svcSpec = specCreator.get().getKey();// new V1ServiceSpec();
            //svcSpec.setType("ClusterIP");
            svcSpec.setSelector(Collections.singletonMap(K8SUtils.LABEL_APP, selector.getK8SResName()));

            // V1ServicePort svcPort = specCreator.get().getRight();// new V1ServicePort();
            svcPort.setName(targetPortName);
            svcPort.setTargetPort(targetPort);
            svcPort.setPort(exportPort);
            svcPort.setProtocol("TCP");
            svcSpec.setPorts(Lists.newArrayList(svcPort));

            svcBody.setSpec(svcSpec);

            V1Service svc
                    = api.createNamespacedService(namespace, svcBody).pretty(K8SUtils.resultPrettyShow).execute();
            return svcRes;
            //  System.out.println(svc);
            // sse.info(svcRes.getName(), TimeFormat.getCurrentTimeStamp(), "success to publish service'" + svcRes.getName() + "'");
            //   success = true;
        } finally {

        }
    }

    public static V1OwnerReference createOwnerReference() {
        SSEExecuteOwner contextAttr = OwnerJobResName.getSSEExecuteOwner();
        NamespacedEventCallCriteria criteria = (NamespacedEventCallCriteria) contextAttr.owner;
        return createOwnerReference(criteria.getOwnerUid(), criteria.getOwnerName());
    }

    public static V1OwnerReference createOwnerReference(V1ReplicationController rc) {
        V1ObjectMeta metadata = rc.getMetadata();
        return createOwnerReference(metadata.getUid(), metadata.getName());
    }

    private static V1OwnerReference createOwnerReference(String ownerId, String ownerName) {
        V1OwnerReference ownerRef = new V1OwnerReference();
        ownerRef.setUid(ownerId);
        ownerRef.setName(ownerName);
        ownerRef.setApiVersion(REPLICATION_CONTROLLER_VERSION);
        ownerRef.setKind(REPLICATION_CONTROLLER_KIND);
        ownerRef.setController(true);
        ownerRef.setBlockOwnerDeletion(true);
        return ownerRef;
    }

    public static UID createUID(V1ReplicationController newRC) {
        return new UID(Objects.requireNonNull(newRC, "newRC can not be null").getMetadata().getUid());
    }


    public static V1ResourceRequirements createResourceRequirements(ReplicasSpec replicasSpec) {
        V1ResourceRequirements rRequirements = new V1ResourceRequirements();
        Map<String, Quantity> limitQuantityMap = Maps.newHashMap();
        limitQuantityMap.put("cpu", new Quantity(replicasSpec.getCpuLimit().literalVal()));
        limitQuantityMap.put("memory", new Quantity(replicasSpec.getMemoryLimit().literalVal()));
        rRequirements.setLimits(limitQuantityMap);
        Map<String, Quantity> requestQuantityMap = Maps.newHashMap();
        requestQuantityMap.put("cpu", new Quantity(replicasSpec.getCpuRequest().literalVal()));
        requestQuantityMap.put("memory", new Quantity(replicasSpec.getMemoryRequest().literalVal()));
        rRequirements.setRequests(requestQuantityMap);
        return rRequirements;
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
    public static NamespacedEventCallCriteria createReplicationController(final CoreV1Api api //
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

//        AppTimestampLabelSelector evtCallCriteria
//                = NamespacedEventCallCriteria.createAppTimestampLabelSelector(name, TimeFormat.getCurrentTimeStamp());
//        evtCallCriteria.setSelectLabel(labes);
//        final String timeStamp = name.getK8SResName() + TimeFormat.getCurrentTimeStamp();
//        labes.put(LABEL_APP_TIMESTAMP, timeStamp);
//        final NamespacedEventCallCriteria evtCallCriteria = new NamespacedEventCallCriteria() {
//            @Override
//            public String getLabelSelector() {
//                return LABEL_APP_TIMESTAMP + "=" + timeStamp;
//            }
//        };
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

//        V1ResourceRequirements rRequirements = new V1ResourceRequirements();
//        Map<String, Quantity> limitQuantityMap = Maps.newHashMap();
//        limitQuantityMap.put("cpu", new Quantity(replicasSpec.getCpuLimit().literalVal()));
//        limitQuantityMap.put("memory", new Quantity(replicasSpec.getMemoryLimit().literalVal()));
//        rRequirements.setLimits(limitQuantityMap);
//        Map<String, Quantity> requestQuantityMap = Maps.newHashMap();
//        requestQuantityMap.put("cpu", new Quantity(replicasSpec.getCpuRequest().literalVal()));
//        requestQuantityMap.put("memory", new Quantity(replicasSpec.getMemoryRequest().literalVal()));
//        rRequirements.setRequests(requestQuantityMap);
        container.setResources(createResourceRequirements(replicasSpec));

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


        V1ReplicationController createdRC =
                api.createNamespacedReplicationController(config.getNamespace(), rc)
                        .pretty(resultPrettyShow)
                        .execute();
//        try {
//            Thread.sleep(8000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        createdRC = api.readNamespacedReplicationController(
//                createdRC.getMetadata().getName(), config.getNamespace())
//                .execute();

        //  UID uid = K8SUtils.createUID(createdRC);
        // String namespace, String pretty, Boolean allowWatchBookmarks, String _continue, String fieldSelector, String labelSelector, Integer limit, String resourceVersion, Integer timeoutSeconds, Boolean watch
        return NamespacedEventCallCriteria.createResVersion(createdRC);
        // return evtCallCriteria;

//        for (V1ReplicationController fetchRC : api.listNamespacedReplicationController(
//                config.getNamespace(), resultPrettyShow, null, null
//                , uid.fieldSelector(), null, null, null, null, null).getItems()) {
//            return fetchRC;
//        }
//        throw new IllegalStateException("uid:" + uid.val + " can not find relevant replicaController");
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

    public static WaitReplicaControllerLaunch waitReplicaControllerLaunch(DefaultK8SImage powerjobServerImage //
            , K8SRCResNameWithFieldSelector targetResName, ReplicasSpec powerjobServerSpec, CoreV1Api apiClient, NamespacedEventCallCriteria resVer)  //
            throws ApiException, PowerjobOrchestrateException {
        return waitReplicaControllerLaunch(powerjobServerImage, targetResName, powerjobServerSpec.getReplicaCount(), apiClient, resVer, new ResChangeCallback() {

        });
    }

    public enum K8SResChangeReason {
        FAILD("failed"), KILL("killing"), FailedScheduling("FailedScheduling"), LAUNCHED("started"),
        // RC 成功删除 Pod
        // reason:SuccessfulDelete,name:powerjob-worker,kind:ReplicationController,message:Deleted pod: powerjob-worker-r6hxn
        // reason:SuccessfulCreate,name:powerjob-worker,kind:ReplicationController,message:Created pod: powerjob-worker-wknhr
        SuccessfulDelete("SuccessfulDelete"), SuccessfulCreate("SuccessfulCreate");

        private final String token;

        K8SResChangeReason(String token) {
            this.token = token;
        }

        public static K8SResChangeReason parse(String reason) {

            for (K8SResChangeReason r : K8SResChangeReason.values()) {
                if (StringUtils.equalsIgnoreCase(r.token, reason)) {
                    return r;
                }
            }
            logger.warn("unresolve pod change reason:" + reason);
            return null;
        }
    }

    public static class UID {
        private final String val;

        public UID(String val) {
            if (StringUtils.isEmpty(val)) {
                throw new IllegalArgumentException("param val can not be null");
            }
            this.val = val;
        }

        public String fieldSelector() {
            return "metadata.uid=" + this.val;
        }
    }


    static final boolean skipWaittingPhase = false;

    public static class WaitReplicaControllerLaunch {
        private final Set<String> relevantPodNames;
        private final boolean skipWaittingPhase;

        public WaitReplicaControllerLaunch() {
            this(Collections.emptySet(), true);
            try {
                Thread.sleep(3000l);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public Set<String> getRelevantPodNames() {
            return this.relevantPodNames;
        }

        public boolean isSkipWaittingPhase() {
            return skipWaittingPhase;
        }

        public WaitReplicaControllerLaunch(Set<String> relevantPodNames) {
            this(relevantPodNames, false);
        }

        public WaitReplicaControllerLaunch(Set<String> relevantPodNames, boolean skipWaittingPhase) {
            this.relevantPodNames = relevantPodNames;
            this.skipWaittingPhase = skipWaittingPhase;
        }

        public void validate() {
            if (!this.skipWaittingPhase && CollectionUtils.isEmpty(relevantPodNames)) {
                throw new IllegalStateException("resource name:" + K8S_DATAX_POWERJOB_SERVER.getName() + " relevant pods can not be null");
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {

            }
        }
    }

    private static void setPodStatus(ResChangeCallback changeCallback
            , Map<String, RunningStatus> relevantPodNames
            , V1ObjectMeta podMeta, boolean runState, boolean podBeCreate) {

        if (podBeCreate) {
            if (relevantPodNames.put(podMeta.getName(), runState ? RunningStatus.SUCCESS : RunningStatus.FAILD) == null) {
                changeCallback.apply(
                        K8SResChangeReason.SuccessfulCreate
                        , podMeta.getName());
            }
        } else {
            if (relevantPodNames.remove(podMeta.getName()) != null) {
                changeCallback.apply(
                        K8SResChangeReason.SuccessfulDelete
                        , podMeta.getName());
            }
        }
    }

    /**
     * 等待RC资源启动
     *
     * @param powerjobServerImage
     * @param expectResCount
     * @param api
     * @param resourceVer
     * @throws ApiException
     * @throws PowerjobOrchestrateException
     */
    public static WaitReplicaControllerLaunch waitReplicaControllerLaunch(DefaultK8SImage powerjobServerImage //
            , K8SRCResNameWithFieldSelector targetResName, final int expectResCount, CoreV1Api api
            , NamespacedEventCallCriteria resourceVer, ResChangeCallback changeCallback)  //
            throws ApiException, PowerjobOrchestrateException {
        SSERunnable sse = SSERunnable.getLocal();
        // CoreV1Api api = new CoreV1Api(apiClient);

        if (skipWaittingPhase) {
            return new WaitReplicaControllerLaunch();
        }
        //  RunningStatus status;
        final Map<String, RunningStatus> relevantPodNames = Maps.newHashMap();
        if (changeCallback.shallGetExistPods()) {
            /**
             * 在Waiting等待过程中是否要获取已有的Pods，在scala pods避免要出现刚添加的pod，随即马上去掉，此时程序识别成又添加了一个pod的情况
             */
            V1PodList pods = targetResName.setFieldSelector(
                    api
                            .listNamespacedPod(powerjobServerImage.getNamespace())
                            .resourceVersion(resourceVer.getResourceVersion()))
                    .execute();
            for (V1Pod pod : pods.getItems()) {
                for (V1OwnerReference oref : pod.getMetadata().getOwnerReferences()) {
                    if (StringUtils.equalsIgnoreCase(oref.getUid(), resourceVer.getOwnerUid())) {
                        relevantPodNames.put(pod.getMetadata().getName(), RunningStatus.SUCCESS);
                    }
                }
            }
            if (MapUtils.isEmpty(relevantPodNames)) {
                logger.warn("relevantPodNames shall not be empty");
            } else {
                logger.warn("relevantPodNames size:{},pods:{}", relevantPodNames.size(), String.join(",", relevantPodNames.keySet()));
            }
            if (changeCallback.isBreakEventWatch(relevantPodNames, expectResCount)) {
                return new WaitReplicaControllerLaunch(relevantPodNames.keySet());
            }
        }
        String currentResVer = resourceVer.getResourceVersion();
        int tryProcessWatcherLogsCount = 0;
        processWatcherLogs:
        while (true) { // 处理 watcher SocketTimeoutException 超时的错误


            int podCompleteCount = 0;
            int podFaildCount = 0;

            Watch<V1Pod> rcWatch = Watch.createWatch(api.getApiClient()
                    //
                    ,
                    targetResName.setFieldSelector(
                            api.listNamespacedPod(powerjobServerImage.getNamespace())
                                    .allowWatchBookmarks(false)
                                    .watch(true)
                                    .resourceVersion(currentResVer))
                            .buildCall(K8SUtils.createApiCallback())

                    //
                    , new TypeToken<Response<V1Pod>>() {
                    }.getType());
            V1Pod pod = null;
            V1ObjectMeta podMeta = null;
            try {
                for (Watch.Response<V1Pod> event : rcWatch) {

                    pod = event.object;
                    podMeta = pod.getMetadata();
                    // 以免下次watch list 收到重复的event
                    currentResVer = podMeta.getResourceVersion();
                    boolean isChildOf = false;
                    for (V1OwnerReference oref : podMeta.getOwnerReferences()) {
                        if (StringUtils.equals(oref.getUid(), resourceVer.getOwnerUid())) {
                            isChildOf = true;
                            break;
                        }
                    }

                    if (!isChildOf) {
                        continue;
                    }
                    boolean podBeCreate = false;
                    // relevantPodNames.add(podMeta.getName());
                    //   System.out.println("type:" + event.type + ",object:" + event.object.getMetadata().getName());
                    String phase = StringUtils.lowerCase(pod.getStatus().getPhase());
                    logger.info("pod:{},change to phase:{}", podMeta.getName(), phase);
                    switch (phase) {
                        case "failed":
                            podFaildCount++;
                            setPodStatus(changeCallback, relevantPodNames, podMeta, false, false);
                            //}
                            break;
                        case "pending":
                            podBeCreate = true;
                        case "succeeded":
                        case "terminating": // pod 被终止
                            podCompleteCount++;
                            setPodStatus(changeCallback, relevantPodNames, podMeta, true, podBeCreate);
                            break;
                        default: {
                            changeCallback.applyDefaultPodPhase(relevantPodNames, pod);
                        }
                    }

                    if (changeCallback.isBreakEventWatch(relevantPodNames, expectResCount)) {
                        break processWatcherLogs;
                    }
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

            if (podFaildCount > 0) {
                throw new PowerjobOrchestrateException(
                        targetResName.getK8SResName() + " launch faild,faild count:" + podFaildCount);
            }

        }
        return new WaitReplicaControllerLaunch(relevantPodNames.keySet());
    }


    public static ApiCallback createApiCallback() {
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
                DataXJobWorker.getJobWorker(TargetResName.K8S_DATAX_INSTANCE_NAME
                        , Optional.of(DataXJobWorker.K8SWorkerCptType.Worker));
    }

}
