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
package com.qlangtech.tis.plugin.ontology.chatbi.config;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.util.Objects;

/**
 * Trace 追踪配置
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/4
 */
public class TraceConfig implements Describable<TraceConfig> {

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer maxTracesPerDomain;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer retentionDays;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean enableAutoCleanup;

    public int getMaxTracesPerDomain() {
        return Objects.requireNonNull(maxTracesPerDomain, "maxTracesPerDomain can not be null");// != null ? maxTracesPerDomain : 1000;
    }

    public int getRetentionDays() {
        return Objects.requireNonNull(retentionDays, "retentionDays can not be null");// != null ? retentionDays : 7;
    }

    public boolean isEnableAutoCleanup() {
        return Objects.requireNonNull(enableAutoCleanup, "enableAutoCleanup can not be null"); //!= null ? enableAutoCleanup : true;
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<TraceConfig> implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Trace Config";
        }


        public boolean validateMaxTracesPerDomain(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            int maxTraces = Integer.parseInt(value);
            int min = 100;
            int max = 3000;
            if (maxTraces < min) {
                msgHandler.addFieldError(context, fieldName, "不能小于" + min);
                return false;
            }

            if (maxTraces > max) {
                msgHandler.addFieldError(context, fieldName, "不能大于" + max);
                return false;
            }

            return true;
        }

        public boolean validateRetentionDays(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            int retentionDays = Integer.parseInt(value);
            int min = 1;
            int max = 30;
            if (retentionDays < min) {
                msgHandler.addFieldError(context, fieldName, "不能小于" + min);
                return false;
            }

            if (retentionDays > max) {
                msgHandler.addFieldError(context, fieldName, "不能大于" + max);
                return false;
            }

            return true;
        }

        @Override
        public String shortComment() {
            return "记录生成SQL过程日志";
        }
    }
}
