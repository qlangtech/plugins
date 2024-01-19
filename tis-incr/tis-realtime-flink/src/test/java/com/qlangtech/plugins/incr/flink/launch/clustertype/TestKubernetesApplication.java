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

import com.qlangtech.plugins.incr.flink.cluster.KubernetesApplicationClusterConfig;
import com.qlangtech.plugins.incr.flink.common.FlinkK8SImage;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import org.junit.Test;

import java.io.File;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-08 14:29
 **/
public class TestKubernetesApplication {

    @Test
    public void testDeploy() throws Exception {

       // FlinkK8SImage k8SImage = new FlinkK8SImage();
        //   k8SImage.namespace =

        KubernetesApplication k8sApp = new KubernetesApplication() {
            @Override
            protected KubernetesApplicationClusterConfig getK8SClusterCfg() {
                KubernetesApplicationClusterConfig clusterCfg = new KubernetesApplicationClusterConfig();
                clusterCfg.k8sImage = "tis_flink_image";
                // k8SClusterManager.clusterId = "tis-flink-cluster";
                clusterCfg.jmMemory = 1238400;
                clusterCfg.tmMemory = 1169472;
                clusterCfg.tmCPUCores = 150;
                clusterCfg.taskSlot = 1;
                clusterCfg.svcExposedType = "NodePort";
                clusterCfg.svcAccount = "default";
                return clusterCfg;
            }
        };
        k8sApp.clusterId = "tis-flink-cluster";
        // k8sApp.clusterCfg = "";
        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        TargetResName coll = new TargetResName("mysql_mysql4");
        File streamUberJar = new File(".");
        k8sApp.deploy(streamFactory, coll, streamUberJar //
                , (request) -> {

                }, (jobId) -> {

                });
    }
}
