/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.plugins.incr.flink.common;

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.flink.IFlinkCluster;
import com.qlangtech.tis.config.flink.JobManagerAddress;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-23 12:10
 **/
public class FlinkCluster extends ParamsConfig implements IFlinkCluster {

    public static void main(String[] args) {
        System.out.println(IFlinkCluster.class.isAssignableFrom(FlinkCluster.class));
    }

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.host, Validator.require})
    public String jobManagerAddress;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.identity, Validator.require})
    public String clusterId;

    @Override
    public JobManagerAddress getJobManagerAddress() {
        return JobManagerAddress.parse(this.jobManagerAddress);
    }

    @Override
    public String getClusterId() {
        return clusterId;
    }

    @Override
    public RestClusterClient createConfigInstance() {
        return createFlinkRestClusterClient();
    }

    private RestClusterClient createFlinkRestClusterClient() {


//        String[] address = StringUtils.split(factory.jobManagerAddress, ":");
//        if (address.length != 2) {
//            throw new IllegalArgumentException("illegal jobManagerAddress:" + factory.jobManagerAddress);
//        }
        try {
            JobManagerAddress managerAddress = this.getJobManagerAddress();
            Configuration configuration = new Configuration();
            configuration.setString(JobManagerOptions.ADDRESS, managerAddress.host);
            configuration.setInteger(JobManagerOptions.PORT, managerAddress.port);
            configuration.setInteger(RestOptions.PORT, managerAddress.port);
            return new RestClusterClient<>(configuration, this.clusterId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<ParamsConfig> {

        // private List<YarnConfig> installations;
        @Override
        public String getDisplayName() {
            return "Flink-Cluster";
        }

        public DefaultDescriptor() {
            super();
            // this.load();
        }
    }
}
