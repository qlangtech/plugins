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

package com.qlangtech.tis.plugin.datax.doplinscheduler.export;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-06 08:31
 **/
public abstract class DSTargetTables implements Describable<DSTargetTables> {
    protected static final String KEY_FIELD_TARGET_TABLES = "targetTables";

    @FormField(ordinal = 200, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<IdentityName> targetTables = Lists.newArrayList();

    @Override
    public final Descriptor<DSTargetTables> getDescriptor() {
        Descriptor<DSTargetTables> desc = Describable.super.getDescriptor();
        if (!(desc instanceof BasicDescriptor)) {
            throw new IllegalStateException("desc class" + desc.getClass().getName()
                    + " must inherited from " + BasicDescriptor.class.getName());
        }
        return desc;
    }

    protected static class BasicDescriptor extends Descriptor<DSTargetTables> implements FormFieldType.IMultiSelectValidator {
        public BasicDescriptor() {
        }
    }
}
