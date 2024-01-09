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
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.flink.client.program.rest.RestClusterClient;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-07 10:40
 **/
public class KubernetesSession extends Standalone {
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.identity, Validator.require})
    public String clusterId;

    public String getClusterId() {
        return clusterId;
    }

    @Override
    public RestClusterClient createRestClusterClient() {
        return this.getClusterCfg().createFlinkRestClusterClient(Optional.of(clusterId), Optional.empty());
    }

    @TISExtension
    public static class DftDescriptor extends Standalone.DftDescriptor {
        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            super.verify(msgHandler, context, postFormVals);
            return true;
        }
    }

}
