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

import com.google.common.collect.Lists;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort.ServiceType;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import io.kubernetes.client.openapi.models.V1HTTPIngressPath;
import io.kubernetes.client.openapi.models.V1HTTPIngressRuleValue;
import io.kubernetes.client.openapi.models.V1Ingress;
import io.kubernetes.client.openapi.models.V1IngressBackend;
import io.kubernetes.client.openapi.models.V1IngressRule;
import io.kubernetes.client.openapi.models.V1IngressServiceBackend;
import io.kubernetes.client.openapi.models.V1IngressSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1ServiceBackendPort;
import io.kubernetes.client.openapi.models.V1ServicePort;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-25 11:37
 **/
public class Ingress extends ServerPortExport {

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.hostWithoutPort})
    public String host;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String path;

    @Override
    protected Integer getExportPort() {
        return this.serverPort;
    }

    @Override
    public <T> T accept(ServerPortExportVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void exportPort(String nameSpace, CoreV1Api api, String targetPortName
            , Pair<ServiceResName, TargetResName> serviceResAndOwner, Optional<V1OwnerReference> ownerRef) throws ApiException {
        super.exportPort(nameSpace, api, targetPortName, serviceResAndOwner, ownerRef);
        final ServiceResName svc = createService(nameSpace, api, targetPortName, (spec) -> {
//            V1ServiceSpec svcSpec = new V1ServiceSpec();
//            svcSpec.setType(ServiceType.ClusterIP.token);
            //svcSpec.setType("LoadBalancer");
            return new V1ServicePort();
        }, serviceResAndOwner, ownerRef);

        // api.createnamespacedIn
        NetworkingV1Api extendApi = new NetworkingV1Api(api.getApiClient());

//        String namespace,
        V1Ingress ingressBody = new V1Ingress();
        V1ObjectMeta metadata = new V1ObjectMeta();
        metadata.setName(svc.getName() + "-ingress");

        metadata.setOwnerReferences(Lists.newArrayList(ownerRef.orElseGet(() -> K8SUtils.createOwnerReference())));
        ingressBody.setMetadata(metadata);

        V1IngressSpec spec = new V1IngressSpec();
        V1IngressRule rule = new V1IngressRule();
        rule.setHost(host);
        V1HTTPIngressRuleValue httpRuleVal = new V1HTTPIngressRuleValue();

        V1HTTPIngressPath path = new V1HTTPIngressPath();
        V1IngressBackend backend = new V1IngressBackend();

        V1IngressServiceBackend svcBackend = new V1IngressServiceBackend();
        svcBackend.setName(svc.getName());
        V1ServiceBackendPort port = new V1ServiceBackendPort();
        port.setName(targetPortName);
        // new IntOrString(targetPortName)
        svcBackend.setPort(port);
        backend.setService(svcBackend);
//        backend.setServiceName(svc.getName());
//        backend.servicePort(new IntOrString(targetPortName));
        path.setBackend(backend);
        path.setPath(this.path);
        path.setPathType("Prefix");
        httpRuleVal.setPaths(Collections.singletonList(path));
        rule.setHttp(httpRuleVal);
        spec.setRules(Collections.singletonList(rule));
        ingressBody.setSpec(spec);


//        String pretty,
//        String dryRun,
//        String fieldManager

        extendApi.createNamespacedIngress(nameSpace, ingressBody)
                .pretty(K8SUtils.resultPrettyShow)
                .execute();
        // Call call = extendApi.createNamespacedIngressCall();

    }

    @Override
    protected ServiceType getServiceType() {
        return ServiceType.ClusterIP;
    }

    @Override
    public String getExternalHost(CoreV1Api api, DefaultK8SImage k8SImage, Pair<ServiceResName, TargetResName> serviceResAndOwner) {
        return this.host + ("/".equals(this.path) ? StringUtils.EMPTY : this.path);
    }

    @TISExtension
    public static class DftDesc extends Descriptor<ServerPortExport> {
        public DftDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return Ingress.class.getSimpleName();
        }
    }
}
