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

package com.qlangtech.plugins.incr.flink.cluster;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.common.FlinkK8SImage;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.Options;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist.TISFlinkProp;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.ServerLaunchToken;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport.DefaultExportPortProvider;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8sExceptionUtils;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.plugin.k8s.K8sImage.ImageCategory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.RbacV1Subject;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1RoleRef;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.MemorySize.MemoryUnit;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.ServiceExposedType;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-13 08:34
 **/
public abstract class BasicFlinkK8SClusterCfg extends DataXJobWorker {
    public static final String KEY_FIELD_CLUSTER_ID = "clusterId";
    private static final Logger logger = LoggerFactory.getLogger(BasicFlinkK8SClusterCfg.class);

    private final static transient Set<String> processedCluster = Sets.newHashSet();

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public ServerPortExport serverPortExport;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer jmMemory;

    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer tmMemory;

    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer tmCPUCores;


    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer taskSlot;

    @FormField(ordinal = 10, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String svcAccount;
    @FormField(ordinal = 11, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean impower;

    // @FormField(ordinal = 12, type = FormFieldType.ENUM, validate = {Validator.require, Validator.identity})
    // 固定暴露的就用ClusterIP类型，其他向用户暴露的入口，再由
    // public final String svcExposedType = ServiceExposedType.ClusterIP.name();


    public static ImageCategory k8sImage() {
        return ImageCategory.DEFAULT_FLINK_DESC_NAME;
    }

    @Override
    protected final ImageCategory getK8SImageCategory() {
        return BasicFlinkK8SClusterCfg.k8sImage();
    }

    public static TISFlinkProp addClusterIdOption(Options<?> opts) {
        //String clusterId = "clusterId";
        TISFlinkProp tisFlinkProp = TISFlinkProp.create(KubernetesConfigOptions.CLUSTER_ID);
        try {
            int size = ServerLaunchToken.createFlinkClusterToken().getAllFlinkSessionClusters().size();
            tisFlinkProp.overwriteDft("tis-flink-cluster-" + (size + 1));
            opts.add(KEY_FIELD_CLUSTER_ID, tisFlinkProp);
            return tisFlinkProp;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ServerLaunchToken getProcessTokenFile() {

        throw new UnsupportedOperationException();
    }

    final V1RoleBinding readRoleBinding() {
        try {
            FlinkK8SImage k8s = this.getK8SImage();
            RbacAuthorizationV1Api authorizationApi = new RbacAuthorizationV1Api(k8s.createApiClient());
            V1RoleBinding binding = authorizationApi.readNamespacedRoleBinding(getRoleBindingName(), k8s.getNamespace()).execute();
            return binding;
        } catch (ApiException e) {
            logger.warn(e.getMessage());
            return null;
        }
    }

    public final FlinkK8SImage getFlinkK8SImage() {
        FlinkK8SImage k8s = this.getK8SImage();
        IK8sContext cfg = k8s.getK8SCfg();
        ApiClient apiClient = k8s.createApiClient();
        final String cacheKey = this.svcAccount + "-" + k8s.getNamespace() + "-" + cfg.getKubeBasePath();
        if (this.impower && !processedCluster.contains(cacheKey)) {

            try {

                final String bindingName = getRoleBindingName();

                //kubectl  create clusterrolebinding tis-flink-manager --clusterrole=cluster-admin --serviceaccount=default:default
                RbacAuthorizationV1Api authorizationApi = new RbacAuthorizationV1Api(apiClient);

                V1RoleBinding roleBinding = this.readRoleBinding();

                if (roleBinding != null) {
                    processedCluster.add(cacheKey);
                    return k8s;
                }

                roleBinding = new V1RoleBinding();
                V1ObjectMeta meta = new V1ObjectMeta();
                meta.setName(bindingName);
                meta.setNamespace(k8s.getNamespace());
                roleBinding.setMetadata(meta);

                V1RoleRef roleRef = new V1RoleRef();
                roleRef.setName("cluster-admin");
                //  supported values: \"Role\", \"ClusterRole\"",
                roleRef.kind("ClusterRole");
                roleBinding.setRoleRef(roleRef);

                RbacV1Subject subject = new RbacV1Subject();
                subject.setName(this.svcAccount);
                subject.setNamespace(k8s.getNamespace());
                // supported values: \"ServiceAccount\", \"User\", \"Group\"",
                subject.setKind("ServiceAccount");
                roleBinding.setSubjects(Lists.newArrayList(subject));
                authorizationApi.createNamespacedRoleBinding(
                        k8s.getNamespace(), roleBinding).pretty(K8SUtils.resultPrettyShow).execute();
            } catch (ApiException e) {
                throw K8sExceptionUtils.convert(e);
            }

            processedCluster.add(cacheKey);
        }
        return k8s;
    }


    private String getRoleBindingName() {
        return "tis-flink-manager-" + this.svcAccount;
    }

    public final Pair<Configuration, IK8sContext> createFlinkConfig() throws Exception {
        K8sImage k8SImageCfg = this.getFlinkK8SImage();
        IK8sContext kubeConfig = k8SImageCfg.getK8SCfg();
        FlinkKubeClientFactory.kubeConfig
                = org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.Config.fromKubeconfig(kubeConfig.getKubeConfigContent());
        final Configuration configuration = ((BasicFlinkCfgDescriptor) this.getDescriptor()).opts.createFlinkCfg(this);
        configuration.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, ServiceExposedType.ClusterIP);
        configuration.set(RestOptions.BIND_PORT, String.valueOf(serverPortExport.serverPort));
        configuration.set(RestOptions.PORT, (serverPortExport.serverPort));
//        configuration.set(KubernetesConfigOptions.KUBE_CONFIG_FILE
//                , configuration.getString(KubernetesConfigOptions.FLINK_CONF_DIR)
//                        + File.separator + FlinkK8SClusterManager.CONFIG_FILE_KUBE_CONFIG);

        return Pair.of(configuration, kubeConfig);
    }

    public abstract static class BasicFlinkCfgDescriptor extends BasicDescriptor implements IEndTypeGetter, DefaultExportPortProvider {

        private static final MemorySize MEMORY_8G = MemorySize.ofMebiBytes(8 * 1024);
        protected final Options<BasicFlinkK8SClusterCfg> opts;

        @Override
        public final EndType getEndType() {
            return EndType.Flink;
        }


        /**
         * @return
         * @see DefaultExportPortProvider
         */
        @Override
        public Integer get() {
            return RestOptions.PORT.defaultValue();
        }


        public BasicFlinkCfgDescriptor() {
            super();
            //FlinkK8SClusterManager
            //this.addFieldDescriptor("serverPortExport.serverPort", 8081, null);
            this.opts = FlinkPropAssist.createOpts(this);
            opts.add("tmMemory", TISFlinkProp.create(TaskManagerOptions.TOTAL_PROCESS_MEMORY)
                            .overwriteDft((MemorySize.ofMebiBytes(1728)))
                    , (fm) -> {
                        if (fm.tmMemory == null) {
                            return null;
                        }
                        return MemorySize.parse(String.valueOf(fm.tmMemory), MemoryUnit.KILO_BYTES);
                    }
            );

            opts.add("tmCPUCores", TISFlinkProp.create(TaskManagerOptions.CPU_CORES)
                            .setOverwriteProp(OverwriteProps.dft(1000).setAppendHelper("*1000个单位代表一个1 CPU Core"))
                    , (fm) -> {
                        if (fm.tmCPUCores == null) {
                            return null;
                        }
                        return ((double) fm.tmCPUCores) / 1000;
                        //  return MemorySize.parse(String.valueOf(), MemoryUnit.KILO_BYTES);
                    }
            );


            opts.add("jmMemory", TISFlinkProp.create(JobManagerOptions.TOTAL_PROCESS_MEMORY)
                            .setOverwriteProp(OverwriteProps.dft(MemorySize.ofMebiBytes(1600)))
                    , (fm) -> {
                        if (fm.jmMemory == null) {
                            return null;
                        }
                        return MemorySize.parse(String.valueOf(fm.jmMemory), MemoryUnit.KILO_BYTES);
                    }
            );
            opts.add(KubernetesConfigOptions.CONTAINER_IMAGE, (fm) -> {
                return fm.getK8SImage().getImagePath();
            });
            //  configuration.set(KubernetesConfigOptions.CONTAINER_IMAGE, k8SImageCfg.getImagePath());
            //  addClusterIdOption(opts).overwriteDft("tis-flink-cluster");
            opts.add(KubernetesConfigOptions.NAMESPACE, (fm) -> fm.getK8SImage().getNamespace());


            opts.add("taskSlot", TISFlinkProp.create(TaskManagerOptions.NUM_TASK_SLOTS));
            opts.add("svcAccount", TISFlinkProp.create(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT));
            // opts.add("svcExposedType", TISFlinkProp.create(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE));

        }


        /**
         * 校验task Manager CPU
         *
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param value
         * @return
         */
        public boolean validateTmCPUCores(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            if (Integer.parseInt(value) > (ReplicasSpec.maxCpuCoresLimit * 1024)) {
                msgHandler.addFieldError(context, fieldName, "不能大于" + ReplicasSpec.maxCpuCoresLimit + "个CPU Core");
                return false;
            }
            return true;
        }

        public boolean validateTmMemory(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateJmMemory(msgHandler, context, fieldName, value);
        }

        public boolean validateJmMemory(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            MemorySize zero = MemorySize.ofMebiBytes(0);
            MemorySize memory = new MemorySize(Long.parseLong(value));
            if (MEMORY_8G.compareTo((memory)) < 0) {
                msgHandler.addFieldError(context, fieldName, "内存不能大于:" + MEMORY_8G.toHumanReadableString());
                return false;
            }
            if (zero.compareTo(memory) >= 0) {
                msgHandler.addFieldError(context, fieldName, "内存不能小于:" + zero.toHumanReadableString());
                return false;
            }
            return true;
        }

        @Override
        protected ImageCategory getK8SImageCategory() {
            return k8sImage();
        }


        @Override
        public IPluginStore<DataXJobWorker> getJobWorkerStore() {
            return DataXJobWorker.getJobWorkerStore(getWorkerType(), Optional.empty());
        }

        @Override
        protected TargetResName getWorkerType() {
            return DataXJobWorker.K8S_FLINK_CLUSTER_NAME.group();
        }

//        @Override
//        public K8SWorkerCptType getWorkerCptType() {
//            return K8SWorkerCptType.FlinkCluster;
//        }
    }
}
