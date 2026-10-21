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

package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 从 Action 参数池中取值。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/12
 */
public class FromParameterValueSource extends ValueSourceConfig {
    public FromParameterValueSource(String parameterRef) {
        this.parameterRef = parameterRef;
    }

    public FromParameterValueSource() {
    }

    /**
     * 参数名。
     *
     * <p>没有下拉候选：参数池在第 3 步声明，而规则在第 2 步配置，规则保存时参数池尚未存在。
     * 行间的「参数是否存在」校验因此做不了，由 {@link DftDescriptor#validateAll} 兜住「必须填参数名」。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String parameterRef;

    @Override
    public Object resolveValue(Map<String, Object> parameters) {
        return parameters.get(parameterRef);
    }

    @TISExtension
    public static class DftDescriptor extends BaseDescriptor {

        @Override
        public String getDisplayName() {
            return "Action 参数";
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals vals) {
            FromParameterValueSource source = vals.newInstance();
            if (StringUtils.isEmpty(source.parameterRef)) {
                msgHandler.addErrorMessage(context, "请填写参数名");
                return false;
            }
            return super.validateAll(msgHandler, context, vals);
        }
    }
}