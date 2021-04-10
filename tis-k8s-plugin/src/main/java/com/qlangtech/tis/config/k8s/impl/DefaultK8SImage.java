/**
 * Copyright 2020 QingLang, Inc.
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.k8s.K8sImage;

/**
 * k8s image 插件
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

    public ParamsConfig getK8SContext() {
        return (ParamsConfig) ParamsConfig.getItem(this.k8sCfg, IK8sContext.class);
    }


    @TISExtension()
    public static class DescriptorImpl extends Descriptor<K8sImage> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME, () -> ParamsConfig.getItems(IK8sContext.class));
        }

        @Override
        public String getDisplayName() {
            return "image";
        }
    }
}
