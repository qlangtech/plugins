/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.config.k8s.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.k8s.K8sImage;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * k8s image 插件
 *
 * @author 百岁（baisui@qlangtech.com）
 * @create: 2020-04-12 11:06
 * @date 2020/04/13
 */
public class DefaultK8SImage extends K8sImage {

    public static final String KEY_FIELD_NAME = "k8sCfg";

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String k8sCfg;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String namespace;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String // = "docker-registry.default.svc:5000/tis/tis-incr:latest";
            imagePath;

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

    //    public ParamsConfig getK8SContext() {
//        return (ParamsConfig)ParamsConfig.getItem(this.k8sCfg, IK8sContext.class);
//    }
    @TISExtension()
    public static class DescriptorImpl extends Descriptor<K8sImage> {
        private static final Logger logger = LoggerFactory.getLogger(DescriptorImpl.class);

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME, () -> ParamsConfig.getItems(IK8sContext.class));
        }

        @Override
        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ParseDescribable<K8sImage> k8s = this.newInstance((IPluginContext) msgHandler, postFormVals.rawFormData, Optional.empty());
            K8sImage k8sCfg = k8s.instance;
            try {
                ApiClient client = k8sCfg.createApiClient();
                CoreV1Api api = new CoreV1Api(client);

                //String name, String pretty, Boolean exact, Boolean export
                V1Namespace v1Namespace = api.readNamespace(k8sCfg.getNamespace(), null, null, null);

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
            } catch (Throwable e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addErrorMessage(context, e.getMessage());
                return false;
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return "image";
        }
    }
}
