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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 对象引用类型参数配置。
 *
 * <p>用于 Action 参数值指向另一个 Ontology 对象的场景，
 * 需要指定目标对象的类型 ID。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/13
 * @see ActionParameterDataType
 */
public class ObjectReferenceDataType extends ActionParameterDataType {

    /**
     * 目标对象类型 ID。
     *
     * <p>指定该参数引用哪种 Ontology 对象类型（如 "Ticket"、"User" 等）。
     * 候选项由 {@code LinkReference.getObjectTypeOpts()} 按当前 ontology 动态提供。
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String objectTypeId;

    @Override
    public String getTypeIdentifier() {
        return "object_reference:" + objectTypeId;
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Object_Reference";
        }

        @Override
        public String shortComment() {
            return "对象引用";
        }
    }
}