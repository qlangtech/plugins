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
 * 数据脱敏（数据脱敏是对敏感信息进行变形以保护隐私，如身份证号、手机号、卡号、客户号等。数据库安全技术包括漏扫、加密、防火墙、脱敏、审计系统。数据库安全风险有拖库、刷库、撞库）；
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-29 09:40
 **/
public class DataMaskingUDF extends SubStrUDF {

    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String replaceChar;

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> descs = super.getLiteria();
        descs.add(new UDFDesc("replaceChar", this.replaceChar));
        return descs;
    }


    @Override
    public void evaluate(ColumnAwareRecord record) {
        final String sourceFieldVal = record.getString(this.from);
        if (StringUtils.isNotEmpty(sourceFieldVal)) {
            StringBuffer masking = new StringBuffer();
            int addMaskCharCount = 0;
            for (int idx = 0; idx < sourceFieldVal.length(); idx++) {
                if (idx < start || (addMaskCharCount >= length)) {
                    masking.append(sourceFieldVal.charAt(idx));
                } else {
                    masking.append(replaceChar);
                    addMaskCharCount++;
                }
            }
            record.setString(this.to.getName(), masking.toString());
        }
    }


    @TISExtension
    public static final class DefaultDescriptor extends DefaultSubStrDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public EndType getTransformerEndType() {
            return EndType.Mask;
        }

        public boolean validateReplaceChar(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int length = StringUtils.length(val);
            if (length > 1) {
                msgHandler.addFieldError(context, fieldName, "长度不能大于1");
                return false;
            }
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Data Masking";
        }
    }
}
