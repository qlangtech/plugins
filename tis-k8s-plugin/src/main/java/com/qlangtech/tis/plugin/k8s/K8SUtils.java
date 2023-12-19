package com.qlangtech.tis.plugin.k8s;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.Specification;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1HostAlias;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1ReplicationController;
import io.kubernetes.client.openapi.models.V1ReplicationControllerSpec;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/13
 */
public class K8SUtils {

    public static final String REPLICATION_CONTROLLER_VERSION = "v1";
    public static final String resultPrettyShow = "true";
    public static final String LABEL_APP = "app";
    public static final TargetResName K8S_DATAX_POWERJOB_MYSQL = new TargetResName("datax-worker-powerjob-mysql");
    public static final TargetResName K8S_DATAX_POWERJOB_SERVER = new TargetResName("datax-worker-powerjob-server");
    public static final TargetResName K8S_DATAX_POWERJOB_WORKER = new TargetResName("datax-worker-powerjob-worker");

    public static String createReplicationController(final CoreV1Api api
            , final K8sImage config, TargetResName name //
            , ReplicasSpec replicasSpec, List<V1ContainerPort> exportPorts, List<V1EnvVar> envs) throws ApiException {
        V1ReplicationController newRC = createReplicationController(api, config, name, () -> new V1Container(), replicasSpec, exportPorts, envs);

        return newRC.getMetadata().getResourceVersion();
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
        mysqlRcSpec.setMemoryLimit(Specification.parse("2048M"));
        mysqlRcSpec.setMemoryRequest(Specification.parse("1024M"));
        mysqlRcSpec.setCpuRequest(Specification.parse("1"));
        mysqlRcSpec.setCpuLimit(Specification.parse("2"));
        return mysqlRcSpec;
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
