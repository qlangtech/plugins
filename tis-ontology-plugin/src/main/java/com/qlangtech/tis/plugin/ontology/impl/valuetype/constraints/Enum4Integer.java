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

package com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ds.BasicMultiSelectSingleValElementCreatorFactory;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.ValueConstraint;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/4/19
 */
public class Enum4Integer extends ValueConstraint {

    private static final String FIELD_ENUM_VALS = "enumVals";

    /**
     * 选择多个可选的值
     */
    @FormField(ordinal = 0, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<OneOfValueEnum> enumVals;

    @Override
    public List<UDFDesc> getLiteria() {
        return List.of(new UDFDesc("enumVals"
                , this.enumVals.stream().map(BasicMultiSelectSingleValElementCreatorFactory.OneOfMultiElement::getEnumVal)
                .collect(Collectors.joining(","))));
    }

    @TISExtension
    public static class DftDesc extends ValueConstraint.BaseDesc {
        public DftDesc() {
            super();
        }

        @Override
        public Set<IEndTypeGetter.EndType> specializedTypeEnds() {
            return Set.of(IEndTypeGetter.EndType.DataTypeInteger);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            Enum4Integer constraint = postFormVals.newInstance();
            Set<String> seen = new HashSet<>();
            for (OneOfValueEnum val : constraint.enumVals) {
                if (!seen.add(val.getEnumVal())) {
                    msgHandler.addFieldError(context, FIELD_ENUM_VALS, "枚举值不能重复: " + val.getEnumVal());
                    return false;
                }
            }
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Enum";
        }

        @Override
        public String shortComment() {
            return "限定整数属性只能取预设枚举值之一";
        }
    }
}
