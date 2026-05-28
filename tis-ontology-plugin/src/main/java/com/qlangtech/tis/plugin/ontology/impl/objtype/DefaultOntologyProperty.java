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

package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyPropertyTypeRef;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.PropertyRoleType;
import com.qlangtech.tis.plugin.ontology.SemanticRole;
import com.qlangtech.tis.plugin.ontology.impl.typeref.DefaultPropertyTypeRef;
import com.qlangtech.tis.plugin.ontology.impl.typeref.SharedPropertyTypeRef;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.util.Objects;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
@JSONType(serializer = OntologyPropertyJsonSerializer.class)
public class DefaultOntologyProperty extends OntologyProperty {
    public DefaultOntologyProperty() {
    }

    public DefaultOntologyProperty(String name, boolean pk, boolean nullable, String description, OntologyType type) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.typeRef = new DefaultPropertyTypeRef(Objects.requireNonNull(type, "type must not be null").getValue());
        this.pk = pk;
        this.nullable = nullable;
        this.description = description;
    }

    @JSONField(serialize = false)
    @FormField(ordinal = 2, validate = {Validator.require})
    public OntologyPropertyTypeRef typeRef;

    /**
     * ChatBI 语义角色 —— 子类对应 {@link SemanticRole} 中的一项，
     * MeasureRole 还承载 derived property 配置。
     */
    @FormField(ordinal = 5, validate = {Validator.require})
    public PropertyRoleType roleType;

    public SemanticRole getSemanticRole() {
        return this.roleType == null ? SemanticRole.Unknown : this.roleType.kind();
    }

    @Override
    public OntologyType parseOntologyType() {
        return this.typeRef.getOntologyType();
    }

    /**
     * 将属性挂接到已经创建的sharedProperty上
     *
     * @param sharedProp
     */
    @Override
    public void reference2SharedProp(OntologySharedProperty sharedProp) {
        SharedPropertyTypeRef sharedPropertyRef = new SharedPropertyTypeRef();
        sharedPropertyRef.sharedPropRef = Objects.requireNonNull(sharedProp).name;
        this.typeRef = sharedPropertyRef;
    }

    @Override
    public void reference2ValueType(OntologyValueType valueType) {
        Objects.requireNonNull(this.typeRef
                , "property:" + this.getName() + " relevant typeRef can not be null").valueType =
                valueType.identityValue();
    }

    @TISExtension
    public static class DftDesc extends Descriptor<OntologyProperty> {
        public DftDesc() {
            super();
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            OntologyProperty property = postFormVals.newInstance();
            if (property.pk && property.isNullable()) {
                msgHandler.addFieldError(context, FIELD_NULLABLE, "当为该属性为主键时，值不能为空");
                return false;
            }

            return true;
        }
    }
}
