/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.plugin.incr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.coredefine.module.action.IIncrSync;
import com.qlangtech.tis.coredefine.module.action.IncrSpec;
import com.qlangtech.tis.pubhook.common.RunEnvironment;
import com.qlangtech.tis.trigger.jst.ILogListener;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/*
 * @create: 2020-04-12 11:12
 *
 * @author 百岁（baisui@qlangtech.com）
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
//            try (Reader reader = new StringReader(config.getK8SContext().getKubeConfigContent())) {
//                client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(reader)).setBasePath(config.getK8SContext().getKubeBasePath()).build();
//                client.getHttpClient().setReadTimeout(720, TimeUnit.SECONDS);
//                Configuration.setDefaultApiClient(client);
//                this.api = new CoreV1Api(client);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
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
        List<V1EnvVar> envVars = Lists.newArrayList();
        V1EnvVar var = new V1EnvVar();
        var.setName("JVM_PROPERTY");
        var.setValue("-Ddata.dir=/opt/data");
        envVars.add(var);
        var = new V1EnvVar();
        var.setName("JAVA_RUNTIME");
        var.setValue(RunEnvironment.getSysRuntime().getKeyName());
        envVars.add(var);
        var = new V1EnvVar();
        var.setName("APP_OPTIONS");
        var.setValue(indexName + " " + timestamp);
        envVars.add(var);
        c.setEnv(envVars);
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
        api.createNamespacedReplicationController(this.config.namespace, rc, true, resultPrettyShow, null);
        // loopQueue.cleanBuffer();
    }

    /**
     * 是否存在RC，有即证明已经完成发布流程
     *
     * @return
     */
    public boolean isRCDeployment(String indexName) {
        try {
            V1ReplicationController rc = api.readNamespacedReplicationController(indexName, this.config.namespace, resultPrettyShow, null, null);
            return rc != null;
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return false;
            } else {
                throw new RuntimeException("code:" + e.getCode(), e);
            }
        }
    }

    /**
     * 列表pod，并且显示日志
     */
    public WatchPodLog listPodAndWatchLog(String indexName, ILogListener listener) {
        DefaultWatchPodLog podlog = new DefaultWatchPodLog(indexName, client, api, config);
        podlog.addListener(listener);
        podlog.startProcess();
        return podlog;
    }
}
