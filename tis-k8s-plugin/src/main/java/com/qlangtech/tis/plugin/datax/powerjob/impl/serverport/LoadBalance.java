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

package com.qlangtech.tis.plugin.datax.powerjob.impl.serverport;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort.ServiceType;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberRange;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-25 11:37
 **/
public class LoadBalance extends ServerPortExport {
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.hostWithoutPort})
    public String host;
    @Override
    public void exportPort(String nameSpace, CoreV1Api api, String targetPortName) throws ApiException {
        //  NodePort.createService(nameSpace, api, targetPortName, this, ServiceType.LoadBalancer);
        NodePort.createService(nameSpace, api, targetPortName, this, () -> {
            V1ServiceSpec svcSpec = new V1ServiceSpec();
            svcSpec.setType(ServiceType.LoadBalancer.token);
            //svcSpec.setType("LoadBalancer");
            V1ServicePort servicePort = new V1ServicePort();
            // servicePort.setNodePort(Objects.requireNonNull(this.nodePort, "nodePort can not be null"));
            return Pair.of(svcSpec, servicePort);
        });
    }

    @Override
    public String getPowerjobHost() {
        if (StringUtils.isEmpty(this.host)) {
            throw new IllegalStateException("prop host can not be empty");
        }
        return host + ":" + Objects.requireNonNull(123, "node port can not be null");
    }

    @TISExtension
    public static class DftDesc extends Descriptor<ServerPortExport> {
        public DftDesc() {
            super();
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            final NodePort portExport = postFormVals.newInstance();
            if (!NetUtils.isReachable(portExport.host)) {
                msgHandler.addFieldError(context, NodePort.KEY_HOST, "不能连通");
                return false;
            }
            return true;
        }

        public boolean validateNodePort(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            NumberRange range = new NumberRange(30000, 32767);

            if (!range.containsNumber(Integer.parseInt(value))) {
                msgHandler.addFieldError(context, fieldName, "必须符合范围" + range);
                return false;
            }

            return true;
        }


        @Override
        public String getDisplayName() {
            return LoadBalance.class.getSimpleName();
        }
    }
}
