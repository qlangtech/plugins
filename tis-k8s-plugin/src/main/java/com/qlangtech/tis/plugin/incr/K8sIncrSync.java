/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.plugin.incr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.coredefine.module.action.IIncrSync;
import com.qlangtech.tis.coredefine.module.action.IncrDeployment;
import com.qlangtech.tis.coredefine.module.action.IncrSpec;
import com.qlangtech.tis.coredefine.module.action.Specification;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.pubhook.common.RunEnvironment;
import com.qlangtech.tis.trigger.jst.ILogListener;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import okhttp3.Call;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @create: 2020-04-12 11:12
 * @date 2020/04/13
 */
public class K8sIncrSync implements IIncrSync {

    private final Logger logger = LoggerFactory.getLogger(K8sIncrSync.class);

    private final String resultPrettyShow = "true";

    private final DefaultIncrK8sConfig config;

    private final ApiClient client;

    private final CoreV1Api api;

    public K8sIncrSync(DefaultIncrK8sConfig k8sConfig) {
        this.config = k8sConfig;
        client = config.getK8SContext().createConfigInstance();
        this.api = new CoreV1Api(client);
    }

    @Override
    public void relaunch(String collection) {

//String namespace, String pretty, Boolean allowWatchBookmarks, String _continue, String fieldSelector, String labelSelector, Integer limit, String resourceVersion, Integer timeoutSeconds, Boolean watch
        try {
            // V1PodList v1PodList = api.listNamespacedPod(this.config.namespace, null, null, null, null, "app=" + collection, 100, null, 600, false);
            V1DeleteOptions options = new V1DeleteOptions();
            Call call = null;
            for (V1Pod pod : this.getRCPods(collection)) {
//String name, String namespace, String pretty, String dryRun, Integer gracePeriodSeconds, Boolean orphanDependents, String propagationPolicy, V1DeleteOptions body
                call = api.deleteNamespacedPodCall(pod.getMetadata().getName(), this.config.namespace, "true", null, 20, true, null, options, null);
                this.client.execute(call, null);
                logger.info(" delete pod {}", pod.getMetadata().getName());
            }
        } catch (ApiException e) {
            throw new RuntimeException(collection, e);
        }

    }

    private List<V1Pod> getRCPods(String collection) throws ApiException {

        V1PodList v1PodList = api.listNamespacedPod(this.config.namespace, null, null
                , null, null, "app=" + collection, 100, null, 600, false);
        return v1PodList.getItems();
    }

    @Override
    public void removeInstance(String indexName) throws Exception {
        if (StringUtils.isBlank(indexName)) {
            throw new IllegalArgumentException("param indexName can not be null");
        }
        if (this.config == null || StringUtils.isBlank(this.config.namespace)) {
            throw new IllegalArgumentException("this.config.namespace can not be null");
        }
        //String name, String namespace, String pretty, V1DeleteOptions body, String dryRun, Integer gracePeriodSeconds, Boolean orphanDependents, String propagationPolicy
        //https://raw.githubusercontent.com/kubernetes-client/java/master/kubernetes/docs/CoreV1Api.md
        V1DeleteOptions body = new V1DeleteOptions();
        body.setOrphanDependents(true);
        // Boolean orphanDependents = true;
        try {
            // this.api.deleteNamespacedReplicationControllerCall()
            Call call = this.api.deleteNamespacedReplicationControllerCall(
                    indexName, this.config.namespace, resultPrettyShow, null, null, true, null, null, null);
            client.execute(call, null);

            this.relaunch(indexName);

        } catch (Throwable e) {
//            if (ExceptionUtils.indexOfThrowable(e, JsonSyntaxException.class) > -1) {
//                //TODO: 不知道为啥api的代码里面一直没有解决这个问题
//                //https://github.com/kubernetes-client/java/issues/86
//                logger.warn(indexName + e.getMessage());
//                return;
//            } else {
            throw new RuntimeException(indexName, e);
            //}
        }


        // this.api.deleteNamespacedReplicationControllerCall()
    }

    public void deploy(String indexName, IncrSpec incrSpec, final long timestamp) throws Exception {
        if (timestamp < 1) {
            throw new IllegalArgumentException("argument timestamp can not small than 1");
        }
        V1ReplicationController rc = new V1ReplicationController();
        V1ReplicationControllerSpec spec = new V1ReplicationControllerSpec();
        spec.setReplicas(incrSpec.getReplicaCount());
        V1PodTemplateSpec templateSpec = new V1PodTemplateSpec();
        V1ObjectMeta meta = new V1ObjectMeta();
        meta.setName(indexName);
        Map<String, String> labes = Maps.newHashMap();
        labes.put("app", indexName);
        meta.setLabels(labes);
        templateSpec.setMetadata(meta);
        V1PodSpec podSpec = new V1PodSpec();
        List<V1Container> containers = Lists.newArrayList();
        V1Container c = new V1Container();
        c.setName(indexName);
        c.setImage(config.imagePath);
        List<V1ContainerPort> ports = Lists.newArrayList();
        V1ContainerPort port = new V1ContainerPort();
        port.setContainerPort(8080);
        port.setName("http");
        port.setProtocol("TCP");
        ports.add(port);
        c.setPorts(ports);

        //V1Container c  c.setEnv(envVars);
        c.setEnv(this.addEnvVars(indexName, timestamp));

        V1ResourceRequirements rRequirements = new V1ResourceRequirements();
        Map<String, Quantity> limitQuantityMap = Maps.newHashMap();
        limitQuantityMap.put("cpu", new Quantity(incrSpec.getCpuLimit().literalVal()));
        limitQuantityMap.put("memory", new Quantity(incrSpec.getMemoryLimit().literalVal()));
        rRequirements.setLimits(limitQuantityMap);
        Map<String, Quantity> requestQuantityMap = Maps.newHashMap();
        requestQuantityMap.put("cpu", new Quantity(incrSpec.getCpuRequest().literalVal()));
        requestQuantityMap.put("memory", new Quantity(incrSpec.getMemoryRequest().literalVal()));
        rRequirements.setRequests(requestQuantityMap);
        c.setResources(rRequirements);
        containers.add(c);
        if (containers.size() < 1) {
            throw new IllegalStateException("containers size can not small than 1");
        }
        podSpec.setContainers(containers);
        templateSpec.setSpec(podSpec);
        spec.setTemplate(templateSpec);
        rc.setSpec(spec);
        rc.setApiVersion("v1");
        meta = new V1ObjectMeta();
        meta.setName(indexName);
        rc.setMetadata(meta);
        api.createNamespacedReplicationController(this.config.namespace, rc, resultPrettyShow, null, null);
    }

    private List<V1EnvVar> addEnvVars(String indexName, long timestamp) {
        List<V1EnvVar> envVars = Lists.newArrayList();
        V1EnvVar var = new V1EnvVar();
        var.setName("JVM_PROPERTY");
        var.setValue("-Ddata.dir=/opt/data -D" + Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS + "=true");
        envVars.add(var);

        RunEnvironment runtime = RunEnvironment.getSysRuntime();
        var = new V1EnvVar();
        var.setName("JAVA_RUNTIME");
        var.setValue(runtime.getKeyName());
        envVars.add(var);
        var = new V1EnvVar();
        var.setName("APP_OPTIONS");
        var.setValue(indexName + " " + timestamp);
        envVars.add(var);

        var = new V1EnvVar();
        var.setName("APP_NAME");
        var.setValue("tis-incr");
        envVars.add(var);

        var = new V1EnvVar();
        var.setName(Config.KEY_RUNTIME);
        var.setValue(runtime.getKeyName());
        envVars.add(var);

        var = new V1EnvVar();
        var.setName(Config.KEY_ZK_HOST);
        var.setValue(Config.getZKHost());
        envVars.add(var);

        var = new V1EnvVar();
        var.setName(Config.KEY_ASSEMBLE_HOST);
        var.setValue(Config.getAssembleHost());
        envVars.add(var);

        var = new V1EnvVar();
        var.setName(Config.KEY_TIS_HOST);
        var.setValue(Config.getTisHost());
        envVars.add(var);

        return envVars;

    }

    /**
     * 取得RC实体对象，有即证明已经完成发布流程
     *
     * @return
     */
    @Override
    public IncrDeployment getRCDeployment(String collection) {
        IncrDeployment rcDeployment = null;
        try {
            V1ReplicationController rc = api.readNamespacedReplicationController(collection, this.config.namespace, resultPrettyShow, null, null);
            if (rc == null) {
                return null;
            }
            rcDeployment = new IncrDeployment();
            rcDeployment.setReplicaCount(rc.getSpec().getReplicas());

            for (V1Container container : rc.getSpec().getTemplate().getSpec().getContainers()) {
                for (V1EnvVar env : container.getEnv()) {
                    rcDeployment.addEnv(env.getName(), env.getValue());
                }
                rcDeployment.setDockerImage(container.getImage());

                V1ResourceRequirements resources = container.getResources();
                String cpu = "cpu";
                String memory = "memory";
                Map<String, Quantity> requests = resources.getRequests();
                Map<String, Quantity> limits = resources.getLimits();

                rcDeployment.setMemoryLimit(Specification.parse(limits.get(memory).toSuffixedString()));
                rcDeployment.setMemoryRequest(Specification.parse(requests.get(memory).toSuffixedString()));
                rcDeployment.setCpuLimit(Specification.parse(limits.get(cpu).toSuffixedString()));
                rcDeployment.setCpuRequest(Specification.parse(requests.get(cpu).toSuffixedString()));
                break;
            }

            V1ReplicationControllerStatus status = rc.getStatus();

            IncrDeployment.ReplicationControllerStatus rControlStatus = new IncrDeployment.ReplicationControllerStatus();
            rControlStatus.setAvailableReplicas(status.getAvailableReplicas());
            rControlStatus.setFullyLabeledReplicas(status.getFullyLabeledReplicas());
            rControlStatus.setObservedGeneration(status.getObservedGeneration());
            rControlStatus.setReadyReplicas(status.getReadyReplicas());
            rControlStatus.setReplicas(status.getReplicas());

            rcDeployment.setStatus(rControlStatus);

            DateTime creationTimestamp = rc.getMetadata().getCreationTimestamp();
            rcDeployment.setCreationTimestamp(creationTimestamp.getMillis());


            List<V1Pod> rcPods = this.getRCPods(collection);

            //Call call = api.listNamespacedPodCall(this.config.namespace, null, null, null, null, "app=" + collection, 100, null, 600, true, null);
//            Watch<V1Pod> podWatch = Watch.createWatch(client, call, new TypeToken<Watch.Response<V1Pod>>() {
//            }.getType());
            V1PodStatus podStatus = null;
            IncrDeployment.PodStatus pods;
            V1ObjectMeta metadata = null;

            for (V1Pod item : rcPods) {
                metadata = item.getMetadata();
                pods = new IncrDeployment.PodStatus();
                pods.setName(metadata.getName());
                podStatus = item.getStatus();
                pods.setPhase(podStatus.getPhase());
                pods.setStartTime(podStatus.getStartTime().getMillis());
                rcDeployment.addPod(pods);
            }

        } catch (ApiException e) {
            if (e.getCode() == 404) {
                logger.warn("can not get collection rc deployment:" + collection);
                return null;
            } else {
                throw new RuntimeException("code:" + e.getCode(), e);
            }
        }
        return rcDeployment;
    }

//    /**
//     * 是否存在RC，有即证明已经完成发布流程
//     *
//     * @return
//     */
//    public boolean isRCDeployment(String indexName) {
//        try {
//            V1ReplicationController rc = api.readNamespacedReplicationController(indexName, this.config.namespace, resultPrettyShow, null, null);
//            return rc != null;
//        } catch (ApiException e) {
//            if (e.getCode() == 404) {
//                return false;
//            } else {
//                throw new RuntimeException("code:" + e.getCode(), e);
//            }
//        }
//    }

    /**
     * 列表pod，并且显示日志
     */
    @Override
    public WatchPodLog listPodAndWatchLog(String indexName, String podName, ILogListener listener) {
        DefaultWatchPodLog podlog = new DefaultWatchPodLog(indexName, podName, client, api, config);
        podlog.addListener(listener);
        podlog.startProcess();
        return podlog;
    }
}
