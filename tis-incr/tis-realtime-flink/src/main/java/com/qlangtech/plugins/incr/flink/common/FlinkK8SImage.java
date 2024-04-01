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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cluster.BasicFlinkK8SClusterCfg;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8sExceptionUtils;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1RoleRef;
import io.kubernetes.client.openapi.models.V1Subject;

import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-04 08:49
 **/
public class FlinkK8SImage extends DefaultK8SImage {

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean impower;

    private final static transient Set<String> processedCluster = Sets.newHashSet();

    public AppsV1Api createAppsV1Api() {
        return new AppsV1Api(this.createApiClient());
    }


    @Override
    public ApiClient createApiClient() {
        ApiClient apiClient = super.createApiClient();
        IK8sContext cfg = this.getK8SCfg();
        final String cacheKey = this.getNamespace() + "-" + cfg.getKubeBasePath();
        if (this.impower && !processedCluster.contains(cacheKey)) {

            try {
                final String bindingName = "tis-flink-manager";
                //kubectl  create clusterrolebinding tis-flink-manager --clusterrole=cluster-admin --serviceaccount=default:default
                RbacAuthorizationV1Api authorizationApi = new RbacAuthorizationV1Api(apiClient);
                V1RoleBinding roleBinding = new V1RoleBinding();
                V1ObjectMeta meta = new V1ObjectMeta();
                meta.setName(bindingName);
                meta.setNamespace(this.getNamespace());
                roleBinding.setMetadata(meta);

                V1RoleRef roleRef = new V1RoleRef();
                roleRef.setName("cluster-admin");
                roleBinding.setRoleRef(roleRef);

                V1Subject subject = new V1Subject();
                subject.setName("default");
                subject.setNamespace(this.getNamespace());
                roleBinding.setSubjects(Lists.newArrayList(subject));
                authorizationApi.createNamespacedRoleBinding(
                        this.getNamespace(), roleBinding).pretty(K8SUtils.resultPrettyShow).execute();
            } catch (ApiException e) {
                throw K8sExceptionUtils.convert(e);
            }

            processedCluster.add(cacheKey);
        }

        return apiClient;
    }

    public CoreV1Api createCoreV1Api() {
        return new CoreV1Api(this.createApiClient());
    }

    @TISExtension()
    public static class FlinkDescriptorImpl extends DescriptorImpl {
        @Override
        protected ImageCategory getImageCategory() {
            return BasicFlinkK8SClusterCfg.k8sImage();
        }
    }
}
