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
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort.ServiceType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1ServicePort;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberRange;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-25 11:37
 **/
public class LoadBalance extends ServerPortExport {
    private transient String host;

    @Override
    protected Integer getExportPort() {
        return this.serverPort;
    }

    @Override
    public void exportPort(String nameSpace, CoreV1Api api, String targetPortName
            , Pair<ServiceResName, TargetResName> serviceResAndOwner, Optional<V1OwnerReference> ownerRef) throws ApiException {
        super.exportPort(nameSpace, api, targetPortName, serviceResAndOwner, ownerRef);
        createService(nameSpace, api, targetPortName, (spec) -> {
            V1ServicePort servicePort = new V1ServicePort();
            return servicePort;
        }, serviceResAndOwner, ownerRef);
        this.host = null;

    }

    @Override
    public <T> T accept(ServerPortExportVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    protected ServiceType getServiceType() {
        return ServiceType.LoadBalancer;
    }


    @Override
    public String getExternalHost(CoreV1Api api, String nameSpace, Pair<ServiceResName, TargetResName> serviceResAndOwner) {

        if (StringUtils.isEmpty(this.host)) {
            // K8SUtils.resultPrettyShow
//            String namespace, String pretty, Boolean allowWatchBookmarks, String _continue
//                    , String fieldSelector, String labelSelector, Integer limit
//                    , String resourceVersion, Integer timeoutSeconds, Boolean watch
            // Pair<ServiceResName, TargetResName> serviceResAndOwner = getServiceResAndOwner();
            this.host = getHost(api, nameSpace, getServiceType(), serviceResAndOwner.getKey(), false);

        }

        return host + ":" + Objects.requireNonNull(serverPort, "node port can not be null");
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
            //final LoadBalance portExport = postFormVals.newInstance();
//            if (!NetUtils.isReachable(portExport.host)) {
//                msgHandler.addFieldError(context, NodePort.KEY_HOST, "不能连通");
//                return false;
//            }
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
