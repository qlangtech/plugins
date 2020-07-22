/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.plugin.incr;

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.k8s.IK8sContext;
import com.qlangtech.tis.coredefine.module.action.IIncrSync;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/*
 * @create: 2020-04-12 11:06
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class DefaultIncrK8sConfig extends IncrStreamFactory {

    public static final String KEY_FIELD_NAME = "k8sName";

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String k8sName;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String namespace;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String // = "docker-registry.default.svc:5000/tis/tis-incr:latest";
            imagePath;

    public String getName() {
        return this.k8sName;
    }

    public ParamsConfig getK8SContext() {
        return (ParamsConfig) ParamsConfig.getItem(this.k8sName, IK8sContext.class);
    }

    private IIncrSync incrSync;

    @Override
    public IIncrSync getIncrSync() {
        if (incrSync != null) {
            return this.incrSync;
        }
        this.incrSync = new K8sIncrSync(this);
        return this.incrSync;
    }

    @TISExtension()
    public static class DescriptorImpl extends Descriptor<IncrStreamFactory> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME, () -> ParamsConfig.getItems(IK8sContext.class));
        }

        @Override
        public String getDisplayName() {
            return "k8s-incr";
        }
    }
}
