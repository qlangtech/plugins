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

import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.TISFlinkCDCStart;
import com.qlangtech.plugins.incr.flink.cluster.FlinkK8SClusterManager;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.config.flink.JobManagerAddress;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;

import java.io.File;
import java.util.function.Consumer;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-07 10:39
 * @see KubernetesClusterDescriptor
 **/
public class KubernetesApplication extends ClusterType {
    @Override
    public void checkUseable() throws TisException {

    }

    @Override
    public RestClusterClient createRestClusterClient() {
        return null;
    }

    @Override
    public JobManagerAddress getJobManagerAddress() {
        return null;
    }

    @Override
    public void deploy(TISFlinkCDCStreamFactory factory
            , TargetResName collection, File streamUberJar
            , Consumer<JarSubmitFlinkRequest> requestSetter, Consumer<JobID> afterSuccess) throws Exception {

        DefaultClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();


        FlinkK8SClusterManager k8SClusterManager = new FlinkK8SClusterManager();
        k8SClusterManager.k8sImage = "tis_flink_image";
        k8SClusterManager.clusterId = "tis-flink-cluster";
        k8SClusterManager.jmMemory = 1238400;
        k8SClusterManager.tmMemory = 1269472;
        k8SClusterManager.tmCPUCores = 500;
        k8SClusterManager.taskSlot = 1;
        k8SClusterManager.svcExposedType = "NodePort";
        k8SClusterManager.svcAccount = "default";


        Configuration flinkConfig = k8SClusterManager.createFlinkConfig();
        flinkConfig.set(ApplicationConfiguration.APPLICATION_MAIN_CLASS, TISFlinkCDCStart.class.getName());
        flinkConfig.set(ApplicationConfiguration.APPLICATION_ARGS, Lists.newArrayList(collection.getName()));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());

        ClusterClientFactory<String> kubernetesClusterClientFactory
                = clusterClientServiceLoader.getClusterClientFactory(flinkConfig);

        ClusterSpecification clusterSpecification = kubernetesClusterClientFactory.getClusterSpecification(
                flinkConfig);

        FlinkKubeClient client
                = FlinkKubeClientFactory.getInstance().fromConfiguration(flinkConfig, "client");
        KubernetesClusterDescriptor kubernetesClusterDescriptor
                = new KubernetesClusterDescriptor(flinkConfig, client);

        FlinkK8SClusterManager.getCreateAccompanyConfigMapResource();

        kubernetesClusterDescriptor.deployApplicationCluster(clusterSpecification
                , ApplicationConfiguration.fromConfiguration(flinkConfig));
    }


    @TISExtension
    public static class DftDescriptor extends Descriptor<ClusterType> {
//        @Override
//        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            super.verify(msgHandler, context, postFormVals);
//            return true;
//        }
    }
}
