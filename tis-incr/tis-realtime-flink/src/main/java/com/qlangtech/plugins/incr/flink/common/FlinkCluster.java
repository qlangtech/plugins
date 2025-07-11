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

package com.qlangtech.plugins.incr.flink.common;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.annotation.JSONField;
import com.qlangtech.plugins.incr.flink.cluster.BasicFlinkK8SClusterCfg;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.Options;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.flink.IFlinkCluster;
import com.qlangtech.tis.config.flink.JobManagerAddress;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.client.program.rest.retry.ExponentialWaitStrategy;
import org.apache.flink.client.program.rest.retry.WaitStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.DefaultClientHighAvailabilityServicesFactory;
import org.apache.flink.runtime.rest.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-23 12:10
 **/
@Public
public class FlinkCluster extends ParamsConfig implements IFlinkCluster {

    private static final Logger logger = LoggerFactory.getLogger(FlinkCluster.class);

    private static final String FLINK_DEFAULT_CLUSTER_ID = "default_cluster_id";

    public static void main(String[] args) {
        System.out.println(IFlinkCluster.class.isAssignableFrom(FlinkCluster.class));
    }

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @JSONField(serialize = false)
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.host, Validator.require})
    public String jobManagerAddress;

    @FormField(ordinal = 2, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.integer, Validator.require})
    public Integer maxRetry;

    @FormField(ordinal = 3, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.integer, Validator.require})
    public Long retryDelay;


    @Override
    public JobManagerAddress getJobManagerAddress() {

        return JobManagerAddress.parse(this.jobManagerAddress);
    }

    @JSONField(serialize = false)
    @Override
    public Class<?> getDescribleClass() {
        return super.getDescribleClass();
    }


    @Override
    public ClusterClient createConfigInstance() {
        return createFlinkRestClusterClient(Optional.empty(), Optional.of(2000l));
    }

    /**
     * @param connTimeout The maximum time in ms for the client to establish a TCP connection.
     * @return
     */
    public ClusterClient createFlinkRestClusterClient(Optional<String> clusterId, Optional<Long> connTimeout) {

        try {
            JobManagerAddress managerAddress = this.getJobManagerAddress();
            Configuration configuration = new Configuration();
            configuration.setString(JobManagerOptions.ADDRESS, managerAddress.host);
            configuration.setInteger(JobManagerOptions.PORT, managerAddress.port);
            configuration.setInteger(RestOptions.PORT, managerAddress.port);
            configuration.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, this.maxRetry);
            configuration.set(RestOptions.RETRY_DELAY, Duration.ofMillis(this.retryDelay));

            if (connTimeout.isPresent()) {
                configuration.set(RestOptions.CONNECTION_TIMEOUT, Duration.ofSeconds(connTimeout.get()));
                configuration.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 0);
                configuration.set(RestOptions.RETRY_DELAY, Duration.ofMillis(0l));
            }


            // WaitStrategy waitStrategy = new ExponentialWaitStrategy(10L, 2000L);


            return new RestClusterClient<>(configuration, clusterId.orElse(this.FLINK_DEFAULT_CLUSTER_ID));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String identityValue() {
        return this.name;
    }


    @TISExtension
    public static class DefaultDescriptor extends BasicParamsConfigDescriptor implements IEndTypeGetter {
        protected final Options<FlinkCluster> opts;

        // private List<YarnConfig> installations;
        @Override
        public String getDisplayName() {
            return KEY_DISPLAY_NAME;
        }

        public DefaultDescriptor() {
            super(KEY_DISPLAY_NAME);
            // this.load();
            opts = FlinkPropAssist.createOpts(this);
            opts.addFieldDescriptor("maxRetry", RestOptions.RETRY_MAX_ATTEMPTS, OverwriteProps.dft(1));
            opts.addFieldDescriptor("retryDelay", RestOptions.RETRY_DELAY);
        }

        @Override
        public EndType getEndType() {
            return EndType.Flink;
        }

        /**
         * 校验是否可用
         *
         * @throws TisException
         */
        private void checkUseable(FlinkCluster cluster) throws TisException {
            try {
                try (ClusterClient restClient = cluster.createFlinkRestClusterClient(Optional.empty(), Optional.of(1000l))) {
                    // restClient.getClusterId();
                    CompletableFuture<Collection<JobStatusMessage>> status = restClient.listJobs();
                    Collection<JobStatusMessage> jobStatus = status.get();
                }
            } catch (Throwable e) {
                throw TisException.create("Please check link is valid:" + cluster.getJobManagerAddress().getUrl(), e);
            }
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            FlinkCluster flinkCluster = postFormVals.newInstance();
            checkUseable(flinkCluster);
            return true;
        }
    }
}
