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
import com.qlangtech.plugins.incr.flink.common.FlinkK8SImage;
import com.qlangtech.plugins.incr.flink.launch.FlinkSlotValidator;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.config.flink.IFlinkClusterConfig;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterTokenManager;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtensible;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.lang.ErrorValue;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.lang.TisException.ErrorCode;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugins.flink.client.JarSubmitFlinkRequest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-07 10:36
 * // @see KubernetesApplication
 * // @see KubernetesSession
 * @see Standalone
 **/
@TISExtensible
public abstract class ClusterType implements Describable<ClusterType>, IFlinkClusterConfig {

    private static final Logger logger = LoggerFactory.getLogger(ClusterType.class);

    public ServerLaunchToken getLaunchToken(TargetResName collection) {
        FlinkClusterTokenManager flinkClusterToken = ServerLaunchToken.createFlinkClusterToken();
        return flinkClusterToken.token(getClusterType(), collection);
    }


    /**
     *
     * @param collection
     * @param deploying
     * @param requiredSlots
     * @return skip flowing check?
     */
    protected boolean skipFlowingCheck(TargetResName collection, boolean deploying, int requiredSlots) {
        return false;
    }

    /**
     *
     * @param collection
     * @param deploying  是否是在部署流程中
     * @throws TisException
     */
    public final void checkUseable(TargetResName collection, boolean deploying, int requiredSlots) throws TisException {

        if (this.skipFlowingCheck(collection, deploying, requiredSlots)) {
            return;
        }

        try {
            try (ClusterClient restClient = createRestClusterClient()) {
                //       webInterfaceURL = restClient.getWebInterfaceURL();
                // restClient.getClusterId();
                CompletableFuture<Collection<JobStatusMessage>> status = restClient.listJobs();
                Collection<JobStatusMessage> jobStatus = status.get();
            }
        } catch (Throwable e) {
            Map<String, Object> payload = Collections.emptyMap();
            if (collection != null) {
                payload = new HashMap<>();
                payload.put(IFullBuildContext.KEY_APP_NAME, collection.getName());
                payload.put(IFlinkClusterConfig.KEY_JOB_MANAGER_ADDRESS, this.getJobManagerAddress());
            }
            throw TisException.create(
                    ErrorValue.create(ErrorCode.FLINK_INSTANCE_LOSS_OF_CONTACT, payload)
                    , ExceptionUtils.getRootCauseMessage(e), e);
        }
        if (deploying) {
            if (requiredSlots < 1) {
                throw new IllegalStateException("requiredSlots must big than 1");
            }
            try (ClusterClient restClient = createRestClusterClient()) {
                // Validate available slots before deploying the job
                if (restClient instanceof RestClusterClient) {
                    RestClusterClient<?> restClusterClient = (RestClusterClient<?>) restClient;
                    // int requiredSlots = factory.parallelism;

                    logger.info("Validating Flink cluster slots before deploying job: {}, required parallelism: {}",
                            collection.getName(), requiredSlots);
                    // 对flink slot 是否满足进行校验
                    FlinkSlotValidator.validateAvailableSlots(restClusterClient, requiredSlots, collection);
                } else {
                    logger.warn("Unable to validate slots: restClient is not an instance of RestClusterClient");
                }
            }
        }

    }

    protected JSONObject getCreatedJobID(TargetResName collection) throws IOException {
        FlinkClusterTokenManager flinkClusterToken = ServerLaunchToken.createFlinkClusterToken();
        ServerLaunchToken launchToken = flinkClusterToken.token(this.getClusterType(), collection);
        JSONObject meta = JSONObject.parseObject(FileUtils.readFileToString(launchToken.getLaunchingToken(), TisUTF8.get()));
        return meta;
        // return JobID.fromHexString(meta.getString(FlinkClusterTokenManager.JSON_KEY_NEW_CREATED_JOB_ID));
    }

    public static JSONObject createClusterMeta(
            FlinkClusterType clusterType, ClusterClient<String> clusterClient, FlinkK8SImage flinkK8SImage) {
        return createClusterMeta(clusterType, Optional.empty(), clusterClient, flinkK8SImage);
    }

    public static JSONObject createClusterMeta(
            FlinkClusterType clusterType, ClusterEndpoint endpoint, ClusterClient<String> clusterClient, FlinkK8SImage flinkK8SImage) {
        return createClusterMeta(clusterType, Optional.of("http://" + endpoint.getAddress() + ":" + endpoint.getPort()), clusterClient, flinkK8SImage);
    }

    public static class ClusterEndpoint {
        private final String address;
        private final int port;

        public ClusterEndpoint(String address, int port) {
            this.address = (String) Preconditions.checkNotNull(address, "Address should not be null.");
            this.port = port;
        }

        public String getAddress() {
            return this.address;
        }

        public int getPort() {
            return this.port;
        }
    }


    public static JSONObject createClusterMeta(
            FlinkClusterType clusterType, Optional<String> entryUrl, ClusterClient<String> clusterClient, FlinkK8SImage flinkK8SImage) {
        JSONObject token = new JSONObject();
        // final String entryUrl = clusterClient.getWebInterfaceURL();
        // System.out.println(clusterClient.getWebInterfaceURL());
        token.put(FlinkClusterTokenManager.JSON_KEY_WEB_INTERFACE_URL
                , Objects.requireNonNull(entryUrl, "entryUrl can not be null")
                        .orElse(clusterClient.getWebInterfaceURL()));

        token.put(FlinkClusterTokenManager.JSON_KEY_CLUSTER_ID, clusterClient.getClusterId());
        // token.put(FlinkClusterTokenManager.JSON_KEY_APP_NAME, collection.getName());
        token.put(FlinkClusterTokenManager.JSON_KEY_CLUSTER_TYPE, clusterType.getToken());

        if (flinkK8SImage != null) {
            token.put(FlinkClusterTokenManager.JSON_KEY_K8S_NAMESPACE, flinkK8SImage.getNamespace());
            token.put(FlinkClusterTokenManager.JSON_KEY_K8S_BASE_PATH, flinkK8SImage.getK8SCfg().getKubeBasePath());
            token.put(FlinkClusterTokenManager.JSON_KEY_K8S_ID, flinkK8SImage.identityValue());
        }
        return token;
    }


    public abstract ClusterClient createRestClusterClient();

    /**
     * 部署flinkJob
     *
     * @param collection
     * @param streamUberJar
     * @param requestSetter
     * @param afterSuccess
     */
    public abstract void deploy(TISFlinkCDCStreamFactory factory, TargetResName collection, File streamUberJar
            , Consumer<JarSubmitFlinkRequest> requestSetter, Consumer<JobID> afterSuccess) throws Exception;

    /**
     * 删除增量实例
     *
     * @param collection
     * @throws Exception
     */
    public abstract void removeInstance(TISFlinkCDCStreamFactory factory, TargetResName collection) throws Exception;

    public abstract FlinkClusterType getClusterType();
}
