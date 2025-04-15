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

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-06-20 11:27
 **/
public class SubStrUDF extends CopyValUDF {

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer start;
    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer length;

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> descs = super.getLiteria();
        descs.add(new UDFDesc("start", String.valueOf(this.start)));
        descs.add(new UDFDesc("length", String.valueOf(this.length)));
        return descs;
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {

        final String sourceFieldVal = record.getString(this.from);
        if (StringUtils.isNotEmpty(sourceFieldVal)) {
            record.setString(this.to.getName(), StringUtils.substring(sourceFieldVal, start, length));
        }
    }

    @TISExtension
    public static class DefaultSubStrDescriptor extends CopyValUDF.DefaultDescriptor {
        public DefaultSubStrDescriptor() {
            super();
        }

        @Override
        public EndType getTransformerEndType() {
            return EndType.SubString;
        }

        public boolean validateStart(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {

            Integer start = Integer.parseInt(val);

            if (start < 0) {
                msgHandler.addFieldError(context, fieldName, "不能小于0");
                return false;
            }

            return true;
        }

        public boolean validateLength(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            Integer length = Integer.parseInt(val);

            if (length < 0) {
                msgHandler.addFieldError(context, fieldName, "不能小于0");
                return false;
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return "SubStr";
        }
    }
}
