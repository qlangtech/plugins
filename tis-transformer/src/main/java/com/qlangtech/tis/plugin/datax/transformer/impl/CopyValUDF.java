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

package com.qlangtech.tis.plugin.datax.transformer.impl;

import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.alibaba.datax.common.element.Record;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 用于使用虚拟列
 */
public class CopyValUDF extends AbstractFromColumnUDFDefinition {

    @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public TargetColType to;


    public static List<TargetColType> getCols() {
        return Lists.newArrayList();
    }

    private TargetColType getTO() {
        return Objects.requireNonNull(this.to, "property to can not be null");
    }

    @Override
    public List<OutputParameter> outParameters() {
        return Collections.singletonList(OutputParameter.create(this.getTO()));
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {

        record.setColumn(this.getTO().getName(), record.getColumn(this.from));
    }

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> literia = super.getLiteria();
        literia.add(new UDFDesc(KEY_TO, this.getTO().getLiteria()));
        return literia;
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<UDFDefinition> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Copy Field";
        }
    }
}
