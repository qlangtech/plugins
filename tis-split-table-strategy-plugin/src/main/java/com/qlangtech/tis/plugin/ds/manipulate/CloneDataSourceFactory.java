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
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DBIdentity;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryManipulate;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-09 18:40
 **/
public class CloneDataSourceFactory extends DataSourceFactoryManipulate implements  IdentityName {

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @Override
    public void manipuldateProcess(IPluginContext pluginContext, Optional<Context> context) {
        if (StringUtils.isEmpty(this.name)) {
            throw new IllegalArgumentException("property name can not be null");
        }
        ManipulateItemsProcessor itemsProcessor = ManipuldateUtils.instance(pluginContext, context.get(), this.name, (meta) -> {
            meta.putExtraParams(DBIdentity.KEY_UPDATE, Boolean.FALSE.toString());
            meta.putExtraParams(DBIdentity.KEY_DB_NAME, this.name);
        }, (oldIdentity) -> {

        });
        itemsProcessor.save(context.get());
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    @TISExtension
    public static class DefaultDesc extends BasicDesc {
        public DefaultDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Clone";
        }
    }
}
