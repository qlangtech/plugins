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

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.plugins.incr.flink.cluster.FlinkK8SClusterManager;
import com.qlangtech.tis.config.flink.JobManagerAddress;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.HeteroEnum;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-07 10:40
 **/
public class KubernetesSession extends AbstractClusterType {
    private static final Logger logger = LoggerFactory.getLogger(KubernetesSession.class);

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String flinkCluster;

    @Override
    public FlinkClusterType getClusterType() {
        return FlinkClusterType.K8SSessionReference;
    }

    @Override
    protected JSONObject createClusterMeta(ClusterClient restClient) {
        FlinkK8SClusterManager sessionCluster = HeteroEnum.getFlinkK8SSessionCluster(flinkCluster);
        return ClusterType.createClusterMeta(getClusterType(), restClient, sessionCluster.getFlinkK8SImage());
    }


    @Override
    public ClusterClient createRestClusterClient() {

        //   flinkCluster
        FlinkK8SClusterManager cluster = HeteroEnum.getFlinkK8SSessionCluster(flinkCluster);


        // HeteroEnum.K8S_SESSION_WORKER.getPluginStore(null,) ;
        //FlinkCluster flinkCluster = new FlinkCluster();
        return cluster.createClusterClient();
        // return this.getClusterCfg().createFlinkRestClusterClient(Optional.of(clusterId), Optional.empty());
    }

    @Override
    public JobManagerAddress getJobManagerAddress() {
        return new JobManagerAddress(null, -1) {
            @Override
            public String getUrl() {
                return createRestClusterClient().getWebInterfaceURL();
            }
        };
    }

    public static class ClusterId implements IdentityName {
        private final String val;

        public ClusterId(String val) {
            this.val = val;
        }

        @Override
        public String identityValue() {
            return this.val;
        }
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<ClusterType> {

        public DftDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_FLINK_CLUSTER, () -> {
                return ServerLaunchToken.createFlinkClusterToken()
                        .getAllFlinkSessionClusters()
                        .stream().map((c) -> new ClusterId(c.getClusterId())).collect(Collectors.toList());
            });
        }


        @Override
        public String getDisplayName() {
            return KubernetesDeploymentTarget.SESSION.getName();
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.verify(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            KubernetesSession session = postFormVals.newInstance();

            try {
                session.createRestClusterClient();
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, KEY_FIELD_FLINK_CLUSTER, "请确认连接是否正常可用");
                return false;
            }

            return true;
        }
    }

}
