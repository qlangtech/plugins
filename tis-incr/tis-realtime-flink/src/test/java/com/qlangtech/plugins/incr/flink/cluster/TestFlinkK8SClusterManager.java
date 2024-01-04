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

import com.qlangtech.tis.datax.job.DefaultSSERunnable;
import com.qlangtech.tis.datax.job.ILaunchingOrchestrate.ExecuteStep;
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
    public void testLaunchService() {
        FlinkK8SClusterManager clusterManager = new FlinkK8SClusterManager();
        clusterManager.clusterId = "tis-flink-cluster";
        clusterManager.k8sImage = "tis_flink_image";
        clusterManager.jmMemory = 1600;
        clusterManager.tmMemory = 1728;

        try (PrintWriter clientWriter = new PrintWriter(new StringWriter())) {
            // DataXJobWorker dataxJobWorker
            List<ExecuteStep> executeSteps = clusterManager.getExecuteSteps();
            Runnable runnable = () -> {
            };
            DefaultSSERunnable sseRunnable = new DefaultSSERunnable(clientWriter, clusterManager, executeSteps, runnable);
            clusterManager.launchService(sseRunnable);
        }
    }
}
