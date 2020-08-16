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
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.pubhook.common.RunEnvironment;
import com.qlangtech.tis.trigger.jst.ILogListener;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import org.apache.commons.lang.StringUtils;
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
    public void removeInstance(String indexName) throws Exception {
        if (StringUtils.isBlank(indexName)) {
            throw new IllegalArgumentException("param indexName can not be null");
        }
        if (this.config == null || StringUtils.isBlank(this.config.namespace)) {
            throw new IllegalArgumentException("this.config.namespace can not be null");
        }
        //String name, String namespace, String pretty, V1DeleteOptions body, String dryRun, Integer gracePeriodSeconds, Boolean orphanDependents, String propagationPolicy
        this.api.deleteNamespacedReplicationController(
                indexName, this.config.namespace, resultPrettyShow, null, null, null, null, null);
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
