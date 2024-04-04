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

package com.qlangtech.plugins.incr.flink.cluster;

import com.qlangtech.plugins.incr.flink.common.FlinkK8SImage;
import com.qlangtech.tis.datax.job.DefaultSSERunnable;
import com.qlangtech.tis.datax.job.ILaunchingOrchestrate.ExecuteStep;
import com.qlangtech.tis.datax.job.ILaunchingOrchestrate.ExecuteSteps;
import com.qlangtech.tis.plugin.common.PluginDesc;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-04 10:54
 **/
public class TestFlinkK8SClusterManager {

    @Test
    public void testGetExecuteSteps() {
        FlinkK8SClusterManager clusterManager = new FlinkK8SClusterManager();
        List<ExecuteStep> executeSteps = clusterManager.getExecuteSteps();
        Assert.assertEquals(1, executeSteps.size());
    }

    @Test
    public void testDescJson() {

        PluginDesc.testDescGenerate(FlinkK8SClusterManager.class, "flink-k8s-cluster-manager.json");
    }

    @Test
    public void testLaunchService() {
        FlinkK8SClusterManager clusterManager = getClusterManager();
        // clusterManager.svcExposedType = "NodePort";


        try (PrintWriter clientWriter = new PrintWriter(new StringWriter())) {
            // DataXJobWorker dataxJobWorker
            List<ExecuteStep> executeSteps = clusterManager.getExecuteSteps();
            Runnable runnable = () -> {
            };
            DefaultSSERunnable sseRunnable = new DefaultSSERunnable(clientWriter, new ExecuteSteps(clusterManager, executeSteps), runnable);
            clusterManager.launchService(sseRunnable);
        }
    }


    private FlinkK8SClusterManager getClusterManager() {
        FlinkK8SClusterManager clusterManager = new FlinkK8SClusterManager();
        clusterManager.clusterId = "tis-flink-cluster";
        clusterManager.k8sImage = "local-tis";
        clusterManager.jmMemory = 812000;
        clusterManager.tmMemory = 1012000;
        clusterManager.tmCPUCores = 700;
        clusterManager.taskSlot = 1;
        clusterManager.svcAccount = "default";
        clusterManager.impower = true;
        return clusterManager;
    }

    @Test
    public void testGetFlinkK8SImage() {
        FlinkK8SClusterManager clusterManager = getClusterManager();
        FlinkK8SImage flinkK8SImage = clusterManager.getFlinkK8SImage();

        clusterManager.readRoleBinding();
    }
}
