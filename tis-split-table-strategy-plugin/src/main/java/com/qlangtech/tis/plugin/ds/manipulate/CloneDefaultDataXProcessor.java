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

package com.qlangtech.tis.plugin.ds.manipulate;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore.AfterPluginSaved;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler.BizLogic;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.IPluginItemsProcessor;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-10 21:04
 **/
public class CloneDefaultDataXProcessor extends DefaultDataXProcessorManipulate implements  IdentityName {

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @Override
    public void manipuldateProcess(IPluginContext pluginContext, Optional<Context> context) {
        if (StringUtils.isEmpty(this.name)) {
            throw new IllegalArgumentException("property name can not be null");
        }
        String[] originId = new String[1];
        /**
         * 校验
         */
        IPluginItemsProcessor itemsProcessor
                = ManipuldateUtils.cloneInstance(pluginContext, context.get(), this.name
                , (meta) -> {
                }, (oldIdentityId) -> {
                    originId[0] = oldIdentityId;
                });
        if (StringUtils.isEmpty(originId[0])) {
            throw new IllegalStateException("originId can not be null");
        }
        if (itemsProcessor == null) {
            return;
        }

        /**
         * 先拷贝所有文件，与下面一步执行前后顺序不能颠倒
         */
        //IPluginWithStore storePlugins = itemsProcessor.getStorePlugins();
        DataxProcessor copyFromPipeline = (DataxProcessor) DataxProcessor.load(null, originId[0]);
        Objects.requireNonNull(copyFromPipeline, "name:" + originId[0] + " relevant pipeline can not be null");
        copyFromPipeline.copy(this.name);
//        List<IAppSource> pipelines = storePlugins.listPlugins();
//        for (IAppSource pipeline : pipelines) {
//            pipeline.copy(this.name);
//        }

        /**
         * 再将新的带有替换后的identityName名的实例保存
         */
        itemsProcessor.save(context.get());

        /**
         * 在数据库中创建
         */
        try {
            pluginContext.executeBizLogic(BizLogic.CREATE_DATA_PIPELINE, context.get(), this.name);
        } catch (Exception e) {
            throw new RuntimeException("create " + this.name + " of type " + StoreResourceType.DataApp + " faild", e);
        }
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    @TISExtension
    public static class DefaultDesc extends DefaultDataXProcessorManipulate.BasicDesc {
        public DefaultDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Clone";
        }
    }
}
