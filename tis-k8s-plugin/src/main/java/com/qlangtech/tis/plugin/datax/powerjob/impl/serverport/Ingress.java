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

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort.ServiceType;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8SUtils.ServiceResName;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.ExtensionsV1beta1Api;
import io.kubernetes.client.openapi.models.ExtensionsV1beta1HTTPIngressPath;
import io.kubernetes.client.openapi.models.ExtensionsV1beta1HTTPIngressRuleValue;
import io.kubernetes.client.openapi.models.ExtensionsV1beta1Ingress;
import io.kubernetes.client.openapi.models.ExtensionsV1beta1IngressBackend;
import io.kubernetes.client.openapi.models.ExtensionsV1beta1IngressRule;
import io.kubernetes.client.openapi.models.ExtensionsV1beta1IngressSpec;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-25 11:37
 **/
public class Ingress extends ServerPortExport {

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.hostWithoutPort})
    public String host;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String path;

    @Override
    public void exportPort(String nameSpace, CoreV1Api api, String targetPortName) throws ApiException {

        final ServiceResName svc = NodePort.createService(nameSpace, api, targetPortName, this, () -> {
            V1ServiceSpec svcSpec = new V1ServiceSpec();
            svcSpec.setType(ServiceType.ClusterIP.token);
            //svcSpec.setType("LoadBalancer");
            V1ServicePort servicePort = new V1ServicePort();
            return Pair.of(svcSpec, servicePort);
        });

        // api.createnamespacedIn
        ExtensionsV1beta1Api extendApi = new ExtensionsV1beta1Api(api.getApiClient());

//        String namespace,
        ExtensionsV1beta1Ingress ingressBody = new ExtensionsV1beta1Ingress();
        ExtensionsV1beta1IngressSpec spec = new ExtensionsV1beta1IngressSpec();
        ExtensionsV1beta1IngressRule rule = new ExtensionsV1beta1IngressRule();
        rule.setHost(host);
        ExtensionsV1beta1HTTPIngressRuleValue httpRuleVal = new ExtensionsV1beta1HTTPIngressRuleValue();

        ExtensionsV1beta1HTTPIngressPath path = new ExtensionsV1beta1HTTPIngressPath();
        ExtensionsV1beta1IngressBackend backend = new ExtensionsV1beta1IngressBackend();
        backend.setServiceName(svc.getName());
        backend.servicePort(new IntOrString(targetPortName));
        path.setBackend(backend);
        path.setPath(this.path);
        httpRuleVal.setPaths(Collections.singletonList(path));
        rule.setHttp(httpRuleVal);
        spec.setRules(Collections.singletonList(rule));
        ingressBody.setSpec(spec);


//        String pretty,
//        String dryRun,
//        String fieldManager

        extendApi.createNamespacedIngress(nameSpace
                , ingressBody, K8SUtils.resultPrettyShow, null, null);
        // Call call = extendApi.createNamespacedIngressCall();

    }

    @Override
    public String getPowerjobHost() {
        return "http://" + host;
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
