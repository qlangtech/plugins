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
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.flink.JobManagerAddress;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DefaultK8sContext
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
@Public
public class DefaultK8sContext extends ParamsConfig implements IK8sContext {
    private static final Logger logger = LoggerFactory.getLogger(DefaultK8sContext.class);

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.url})
    public String kubeBasePath;

    @FormField(ordinal = 2, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String kubeConfigContent;

    @Override
    public String identityValue() {
        return this.name;
    }
//    @Override
//    public String getName() {
//        return this.name;
//    }

    @Override
    public String getKubeConfigContent() {
        return kubeConfigContent;
    }

    @Override
    public String getKubeBasePath() {
        return kubeBasePath;
    }

    @Override
    public ApiClient createConfigInstance() {

        ApiClient client = null;
        URL basePath = null;
        try {
            basePath = new URL(this.kubeBasePath);
            (new JobManagerAddress(basePath.getHost(), basePath.getPort())).telnet();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        } catch (SocketTimeoutException e) {
            throw new RuntimeException("basePath:" + basePath, e);
        }
        try {
            try (Reader reader = new StringReader(this.kubeConfigContent)) {
                client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(reader)).setBasePath(this.kubeBasePath).build();
                // 30秒连接超时
                client.setConnectTimeout(30000);
                client.setReadTimeout(30000);
                // client.getHttpClient().setReadTimeout(720, TimeUnit.SECONDS);
                Configuration.setDefaultApiClient(client);


            }
            return client;
        } catch (Exception e) {
            throw new RuntimeException("kubeConfigContent illegal:\n" + kubeConfigContent, e);
        }
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicParamsConfigDescriptor implements IEndTypeGetter {
        public DefaultDescriptor() {
            super(IK8sContext.KEY_DISPLAY_NAME);
            this.load();
        }

        @Override
        public final EndType getEndType() {
            return EndType.K8S;
        }

        @Override
        public String getDisplayName() {
            return IK8sContext.KEY_DISPLAY_NAME;
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            //return super.validate(msgHandler, context, postFormVals);
            //ParseDescribable<Describable> k8s = this.newInstance((IPluginContext) msgHandler,
//                    postFormVals.rawFormData, Optional.empty());
            DefaultK8sContext k8sCfg = postFormVals.newInstance();// (DefaultK8sContext) k8s.getInstance();
            try {
                ApiClient client = k8sCfg.createConfigInstance();
                CoreV1Api api = new CoreV1Api(client);
                //String pretty, Boolean allowWatchBookmarks, String _continue, String fieldSelector, String labelSelector, Integer limit, String resourceVersion, Integer timeoutSeconds, Boolean watch
                V1NamespaceList namespaceList = api.listNamespace().execute();
                if (namespaceList.getItems().size() < 1) {
                    msgHandler.addActionMessage(context, "now the namespace is empty");
                } else {
                    msgHandler.addActionMessage(context
                            , "exist namespace is:" + namespaceList.getItems().stream().map((ns) -> {
                                return ns.getMetadata().getName();
                            }).collect(Collectors.joining(",")));
                }
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addErrorMessage(context, e.getMessage());
                return false;
            }
            return true;
        }

        public boolean validateKubeConfigContent(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            final Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
            try {
                try (Reader reader = new StringReader(value)) {
                    Object config = yaml.load(reader);
                }
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
            return true;
        }
    }
}
