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

package com.qlangtech.plugins.incr.flink.launch.clustertype;

import com.qlangtech.plugins.incr.flink.cluster.FlinkK8SClusterManager;
import com.qlangtech.plugins.incr.flink.cluster.KubernetesApplicationClusterConfig;
import com.qlangtech.plugins.incr.flink.common.FlinkK8SImage;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.config.flink.JobManagerAddress;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.config.k8s.impl.DefaultK8sContext;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.LoadBalance;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-08 14:29
 **/
public class TestKubernetesApplication {

    @Before
    public void executeBefore() {
        SSERunnable.setLocalThread(SSERunnable.createMock());
    }

    @Test
    public void testGetRestEndpoint() throws Exception {
        final String clusterId = "mysql-mysql-1";
        KubernetesApplicationClusterConfig clusterConfig = new KubernetesApplicationClusterConfig() {

            @Override
            public Descriptor<DataXJobWorker> getDescriptor() {
                return new KubernetesApplicationClusterConfig.DescriptorImpl();
            }

            @Override
            protected FlinkK8SImage getK8SImage() {
                FlinkK8SImage flinkK8SImage = new FlinkK8SImage() {
                    @Override
                    public DefaultK8sContext getK8SCfg() {
                        try {
                            DefaultK8sContext k8sCfg = new DefaultK8sContext();
                            k8sCfg.kubeConfigContent = FileUtils.readFileToString(new File("/Users/mozhenghua/.kube/aliyun"), TisUTF8.get());
                            k8sCfg.kubeBasePath = "https://120.55.127.239:6443";
                            return k8sCfg;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
             //   flinkK8SImage.impower = false;
                return flinkK8SImage;
            }
        };
        LoadBalance loadBalance = new LoadBalance();
        loadBalance.serverPort = 8081;
        clusterConfig.serverPortExport = loadBalance;
        Pair<Configuration, IK8sContext> flinkConfig = clusterConfig.createFlinkConfig();
        flinkConfig.getLeft().set(KubernetesConfigOptions.CLUSTER_ID, clusterId);
        final String externalService = FlinkK8SClusterManager.createExternalServiceSuppler(clusterId, clusterConfig.serverPortExport);


        FlinkKubeClientFactory kubeClientFactory = FlinkKubeClientFactory.getInstance();
        try (FlinkKubeClient kubeClient = kubeClientFactory.fromConfiguration(flinkConfig.getKey(), "client")) {
            Endpoint endpoint = FlinkK8SClusterManager.getEndpoint(clusterId, clusterConfig.getFlinkK8SImage() ,clusterConfig.serverPortExport, externalService, kubeClient);
            Assert.assertNotNull(endpoint);
        }


    }

    @Test
    public void testDeploy() throws Exception {

        // FlinkK8SImage k8SImage = new FlinkK8SImage();
        //   k8SImage.namespace =

        KubernetesApplication k8sApp = new KubernetesApplication() {
            @Override
            protected KubernetesApplicationClusterConfig getK8SClusterCfg() {
                KubernetesApplicationClusterConfig clusterCfg = new KubernetesApplicationClusterConfig();
                clusterCfg.k8sImage = "local-tis";
                // k8SClusterManager.clusterId = "tis-flink-cluster";
                clusterCfg.jmMemory = 1238400;
                clusterCfg.tmMemory = 1169472;
                clusterCfg.tmCPUCores = 150;
                clusterCfg.taskSlot = 1;
                // clusterCfg.svcExposedType = "NodePort";
                clusterCfg.svcAccount = "default";
                return clusterCfg;
            }

            @Override
            protected ClusterClient createClient(Pair<Configuration, IK8sContext> flinkConfig, boolean execDeploy) {
                return super.createClient(flinkConfig, false);
            }
        };
        k8sApp.clusterId = "flink1";

        // k8sApp.clusterCfg = "";
        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        TargetResName coll = new TargetResName("mysql_mysql");
        File streamUberJar = new File(".");
//        k8sApp.deploy(streamFactory, coll, streamUberJar //
//                , (request) -> {
//
//                }, (jobId) -> {
//
//                });

        JobManagerAddress jobManagerAddress = k8sApp.getJobManagerAddress();
        Assert.assertNotNull(jobManagerAddress);
        System.out.println(jobManagerAddress.getURL());
    }
}
