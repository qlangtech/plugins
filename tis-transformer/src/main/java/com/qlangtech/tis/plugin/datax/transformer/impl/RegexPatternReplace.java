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
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.jdbcprop.TargetColType;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将目标字段通过正则式转化成新的值，例如要将java的日期，timestamp 将日期去掉，yyyy-MM-dd 转成 yyyy-MM 这样的格式
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-13 13:56
 **/
public class RegexPatternReplace extends AbstractFromColumnUDFDefinition {

    @FormField(ordinal = 10, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String pattern;

    @FormField(ordinal = 11, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String replaceContent;

    @FormField(ordinal = 12, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean throwErrorNotMatch;

    private transient TargetColType _to;
    private transient Pattern _pattern;

    private TargetColType getTO() {
        if (this._to == null) {
            ExistTargetCoumn existTargetCoumn = new ExistTargetCoumn();
            if (StringUtils.isEmpty(this.from)) {
                throw new IllegalArgumentException("param from can not be null");
            }
            existTargetCoumn.name = this.from;
            this._to = new TargetColType();
            this._to.setType(DataType.createVarChar(40));
            this._to.setTarget(existTargetCoumn);
        }
        return this._to;
    }

    private Pattern getPattern() {
        if (this._pattern == null) {
            this._pattern = Pattern.compile(this.pattern);
        }
        return this._pattern;
    }


    @Override
    public List<OutputParameter> outParameters() {
        return Collections.singletonList(TargetColType.create(this.getTO()));
    }

    @Override
    public List<UDFDesc> getLiteria() {
        UDFDesc fromDesc = new UDFDesc(KEY_FROM, this.from);
        fromDesc.addPair("matchPattern", this.pattern);
        fromDesc.addPair("replaceWtih", this.replaceContent);
        return Lists.newArrayList(fromDesc);

    }

    @Override
    public void evaluate(ColumnAwareRecord record) {
        Pattern replacePattern = getPattern();
        String coVal = record.getString(this.from);
        if (coVal == null) {
            return;
        }
        final String strVal = String.valueOf(coVal);
        Matcher matcher = replacePattern.matcher(strVal);
        if (!matcher.find()) {
            if (this.throwErrorNotMatch) {
                throw new IllegalStateException("strVal:" + strVal + " is not match :" + replacePattern);
            } else {
                // 直接跳出
                return;
            }
        }

        record.setString(this.getTO().getName(), matcher.replaceAll(replaceContent));
    }

    @TISExtension
    public static class DefaultDescriptor extends UDFDefinition.BasicUDFDesc {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public EndType getTransformerEndType() {
            return EndType.Replace;
        }

        public boolean validatePattern(IFieldErrorHandler msgHandler
                , Context context, String fieldName, String value) {
            try {
                Pattern.compile(value);
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Regex Replace";
        }
    }
}
