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
import com.qlangtech.plugins.incr.flink.common.FlinkCluster;
import com.qlangtech.plugins.incr.flink.launch.FlinkTaskNodeController;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.flink.IFlinkCluster;
import com.qlangtech.tis.config.flink.JobManagerAddress;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServerLaunchToken.FlinkClusterType;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.flink.client.program.ClusterClient;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-07 10:39
 **/
public class Standalone extends AbstractClusterType {


    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String flinkCluster;

    @Override
    protected JSONObject createClusterMeta(ClusterClient restClient) {
        return ClusterType.createClusterMeta(getClusterType(), restClient, null);
    }

    @Override
    public void removeInstance(TISFlinkCDCStreamFactory factory, TargetResName collection) throws Exception {
        FlinkTaskNodeController nodeController = new FlinkTaskNodeController(factory);
        nodeController.removeInstance(collection);
    }

    @Override
    public FlinkClusterType getClusterType() {
        return FlinkClusterType.Standalone;
    }

    public FlinkCluster getClusterCfg() {
        return ParamsConfig.getItem(this.flinkCluster, FlinkCluster.KEY_DISPLAY_NAME);
    }

    @Override
    public ClusterClient createRestClusterClient() {
        return this.getClusterCfg().createConfigInstance();
    }

    @Override
    public JobManagerAddress getJobManagerAddress() {
        return this.getClusterCfg().getJobManagerAddress();
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<ClusterType> {
        public DftDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_FLINK_CLUSTER, () -> ParamsConfig.getItems(IFlinkCluster.KEY_DISPLAY_NAME));
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            try {
                Standalone standalone = postFormVals.newInstance();
                standalone.checkUseable(null);
            } catch (TisException e) {
                msgHandler.addFieldError(context, KEY_FIELD_FLINK_CLUSTER, e.getMessage());
                return false;
            }
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.verify(msgHandler, context, postFormVals);
        }

        //        @Override
//        public boolean secondVerify(IControlMsgHandler msgHandler
//                , Context context, PostFormVals postFormVals, PostFormVals parentPostFormVals) {
//            return super.secondVerify(msgHandler, context, postFormVals, parentPostFormVals);
//        }
    }
}
