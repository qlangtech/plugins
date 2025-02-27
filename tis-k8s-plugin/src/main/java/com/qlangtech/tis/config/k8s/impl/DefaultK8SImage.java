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
package com.qlangtech.tis.config.k8s.impl;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore.AfterPluginSaved;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.k8s.HostAlias;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8sExceptionUtils;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeAddress;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * k8s image 插件
 *
 * @author 百岁（baisui@qlangtech.com）
 * @create: 2020-04-12 11:06
 * @date 2020/04/13
 */
@Public
public class DefaultK8SImage extends K8sImage implements AfterPluginSaved {

    public static final String KEY_FIELD_NAME = "k8sCfg";
    private static final Logger logger = LoggerFactory.getLogger(DefaultK8SImage.class);
    private static final Yaml yaml = new Yaml();


    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String k8sCfg;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String namespace;

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String // = "docker-registry.default.svc:5000/tis/tis-incr:latest";
            imagePath;

    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean useExternalIP;

    @FormField(ordinal = 9, type = FormFieldType.TEXTAREA, advance = true, validate = {})
    public String hostAliases;

    /**
     * 会否能联通到K8S集群内网，tis-console节点是否和K8S集群同在一个内网
     */
    public transient ClusterIPAvailable clusterIPAvailable;

    @Override
    public boolean internalClusterAvailable() {
        this.createApiClient();
        return Objects.requireNonNull(clusterIPAvailable).getClusterIPAvailable();
    }

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this.clusterIPAvailable = null;
    }

    /**
     * ParamsConfig.createConfigInstance():
     *
     * @param
     * @return
     */
    public io.kubernetes.client.openapi.ApiClient createApiClient() {
        io.kubernetes.client.openapi.ApiClient apiClient = super.createApiClient();
        try {
            if (clusterIPAvailable == null) {
                StringBuffer debug = new StringBuffer("============================\n");
                if (useExternalIP) {
                    clusterIPAvailable = new ClusterIPAvailable(false, null);
                    debug.append("useExternalIP is true,use externalIP directly\n");
                } else {

                    CoreV1Api core = new CoreV1Api(apiClient);
                    V1NodeList nodes = core.listNode().pretty(K8SUtils.resultPrettyShow).execute();
                    V1NodeSpec spec = null;
                    V1NodeStatus status = null;


                    for (V1Node node : nodes.getItems()) {
                        spec = node.getSpec();
                        status = node.getStatus();
                        debug.append(status).append("\n");
                        if (spec.getUnschedulable() != null
                                && spec.getUnschedulable()) {
                            continue;
                        }

                        for (V1NodeAddress address : status.getAddresses()) {
                            // @see org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.NodePortAddressType
                            if ("InternalIP".equals(address.getType())) {

                                clusterIPAvailable
                                        = new ClusterIPAvailable(NetUtils.isReachable(address.getAddress()), address.getAddress());
                                debug.append("testNode:" + address.getAddress()
                                        + ",reachable:" + clusterIPAvailable.getClusterIPAvailable() + ",type:" + address.getType()).append("\n");
                            }
                        }
                    }
                    if (clusterIPAvailable == null) {
                        clusterIPAvailable = new ClusterIPAvailable(false, null);
                        debug.append("have not found any cluster nodes.\n");
                    }
                }
                debug.append("============================");
                logger.info(debug.toString());
            }
        } catch (ApiException e) {
            throw K8sExceptionUtils.convert(e);
        }
        return apiClient;
    }

    @Override
    public List<HostAlias> getHostAliases() {
        return parseHostAliases((err) -> {
            throw new IllegalStateException(err);
        }, this.hostAliases);
    }

    private static List<HostAlias> parseHostAliases(IErrorFieldMsgHandler errorFieldMsgHandler, String val) {
        if (StringUtils.isBlank(val)) {
            return Collections.emptyList();
        }
        HostAlias host = null;
        List<HostAlias> result = Lists.newArrayList();
        List<Map<String, Object>> hostAliaes = yaml.load(val);
        int tupleIndex = 0;
        for (Map<String, Object> hostAlia : hostAliaes) {
            host = new HostAlias();
            host.setIp((String) hostAlia.get("ip"));
            host.setHostnames((List<String>) hostAlia.get("hostnames"));
            if (StringUtils.isBlank(host.getIp())) {
                errorFieldMsgHandler.addErr("第" + tupleIndex + "个配置'ip'属性必须填写");
                return result;
            }
            if (CollectionUtils.isEmpty(host.getHostnames())) {
                errorFieldMsgHandler.addErr("第" + tupleIndex + "个配置'hostnames'属性必须填写");
                return result;
            }
            result.add(host);
            tupleIndex++;
        }
        return result;
    }

    @Override
    public String getK8SName() {
        return this.k8sCfg;
    }

    @Override
    public String getNamespace() {
        return this.namespace;
    }

    @Override
    public String getImagePath() {
        return this.imagePath;
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    interface IErrorFieldMsgHandler {
        void addErr(String msg);
    }

    @TISExtension()
    public static class DescriptorImpl extends BasicDesc implements IEndTypeGetter {
        private static final Logger logger = LoggerFactory.getLogger(DescriptorImpl.class);
        /**
         * https://stackoverflow.com/questions/65004095/regex-for-testing-that-a-docker-image-name-is-prefixed-with-a-registry
         */
        static final Pattern PATTERN_IMAGE_PATH = Pattern.compile("([^/]+\\.[^/.]+/)?([^/.]+/)?[^/.]+(:.+)?");

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME, () -> ParamsConfig.getItems(IK8sContext.KEY_DISPLAY_NAME));
        }

        @Override
        public EndType getEndType() {
            return EndType.Docker;
        }


        public final boolean validateImagePath(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = PATTERN_IMAGE_PATH.matcher(value);
            if (!matcher.matches() || StringUtils.indexOfAny(value, new char[]{' ', '\t'}) > -1) {
                msgHandler.addFieldError(context, fieldName, "不符合格式:" + PATTERN_IMAGE_PATH.pattern());
                return false;
            }

            return true;
        }


        public boolean validateHostAliases(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                AtomicBoolean hasErr = new AtomicBoolean();
                parseHostAliases((err) -> {
                    msgHandler.addFieldError(context, fieldName, err);
                    hasErr.set(true);
                }, value);

                if (hasErr.get()) {
                    return false;
                }

//                Iterable<Object> hostAliases = yaml.loadAll(value);
//                for (Object h : hostAliases) {
//                    System.out.println(h);
//                }

//                for (HostAlias ha : hostAliases) {
//                    System.out.println(ha.getIp());
//                }
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
            return true;
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            ;
            //  ParseDescribable<Describable> k8s = this.newInstance((IPluginContext) msgHandler, postFormVals.rawFormData, Optional.empty());
            K8sImage k8sCfg = postFormVals.newInstance();// k8s.getInstance();
            try {
                ApiClient client = k8sCfg.createApiClient();
                CoreV1Api api = new CoreV1Api(client);

                //String name, String pretty, Boolean exact, Boolean export
                V1Namespace v1Namespace = api.readNamespace(k8sCfg.getNamespace()).execute();

                //  V1NamespaceList namespaceList = api.listNamespace(null, null, null, null, null, null, null, null, null);
                if (v1Namespace == null) {
                    // msgHandler.addActionMessage(context, " namespace is empty");
                    msgHandler.addFieldError(context, "namespace", "is not exist,please create it ahead");
                    return false;
                }
//                else {
//                    msgHandler.addActionMessage(context, "exist namespace is:" + namespaceList.getItems().stream().map((ns) -> {
//                        return ns.getMetadata().getName();
//                    }).collect(Collectors.joining(",")));
//                }
            } catch (ApiException e) {
                throw K8sExceptionUtils.convert(e);
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addErrorMessage(context, e.getMessage());
                return false;
            }

            return true;
        }

//        @Override
//        public String getDisplayName() {
//            return K8sImage.DEFAULT_DESC_NAME;
//        }

        @Override
        protected ImageCategory getImageCategory() {
            return ImageCategory.DEFAULT_DESC_NAME;
        }
    }
}
