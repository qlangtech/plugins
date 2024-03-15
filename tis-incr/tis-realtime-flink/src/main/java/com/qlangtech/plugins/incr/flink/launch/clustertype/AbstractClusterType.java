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

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.IFlinkIncrJobStatus;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterTokenManager;
import com.qlangtech.tis.plugins.flink.client.FlinkClient;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

import java.io.File;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-19 17:06
 **/
public abstract class AbstractClusterType extends ClusterType {
    protected static final String KEY_FIELD_FLINK_CLUSTER = "flinkCluster";

    @Override
    public void removeInstance(TISFlinkCDCStreamFactory factory, TargetResName collection) throws Exception {
        // JobID jobID = getCreatedJobID(collection);
        IFlinkIncrJobStatus<JobID> incrJobStatus = factory.getIncrJobStatus(collection);

        JobID launchJobID = incrJobStatus.getLaunchJobID();
        if (launchJobID != null) {
            this.createRestClusterClient().cancel(launchJobID);
        }
        this.getLaunchToken(collection).deleteLaunchToken();
    }

    /**
     * @param factory
     * @param collection
     * @param streamUberJar
     * @param requestSetter
     * @param afterSuccess
     * @throws Exception
     */
    public final void deploy(TISFlinkCDCStreamFactory factory, TargetResName collection, File streamUberJar
            , Consumer<JarSubmitFlinkRequest> requestSetter, Consumer<JobID> afterSuccess) throws Exception {


        ServerLaunchToken launchToken = this.getLaunchToken(collection);
        launchToken.writeLaunchToken(() -> {

            final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(TIS.get().getPluginManager().uberClassLoader);
            try (ClusterClient restClient = createRestClusterClient()) {


                FlinkClient flinkClient = new FlinkClient();

                JarSubmitFlinkRequest request
                        = JarSubmitFlinkRequest.createFlinkJobRequest(factory, collection, streamUberJar, requestSetter);
                JSONObject clusterMeta = createClusterMeta(restClient);

                JobID jobID = flinkClient.submitJar(restClient, request);

                afterSuccess.accept(jobID);
                return Optional.of(clusterMeta);
            } finally {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }


        });
    }

    protected abstract JSONObject createClusterMeta(ClusterClient restClient);


}
