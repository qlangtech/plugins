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
package com.qlangtech.tis.plugin.ontology.workshop.model.event;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.io.Serializable;

/**
 * stream-llm 事件的载荷，对应前端 {@code StreamLLMConfig} 接口。
 *
 * <p>单独成类而不是把字段平铺到 {@link StreamLlmConfig} 上，是因为前端的联合类型成员是
 * {@code { type: 'stream-llm'; config: StreamLLMConfig }} —— 载荷挂在一个 {@code config} 键下面。
 * 保持这个嵌套形状，事件适配器才可以对所有事件类型使用同一个机械的展开写法
 * {@code { type: 推导类型, ...cfg }}。
 */
public class StreamLlmPayloadConfig implements Describable<StreamLlmPayloadConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String KEY_TARGET_VARIABLE_ID = "targetVariableId";

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String targetVariableId;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String prompt;

    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public LlmModel model = LlmModel.gpt_4;

    /** 没有小数类型的表单字段，故用文本承载，取值范围 0~2，例如 0.7。 */
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT)
    public String temperature;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER)
    public Integer variableUpdateDelay;

    /**
     * 枚举常量名要避开点号与减号，故用下划线；调用大模型时的真实取值（{@code gpt-3.5} 等）
     * 由前端适配器里的 {@code STREAM_LLM_MODEL_API_NAME} 表还原 —— 表单字段的取值就是常量名本身，
     * 后端无法把两者解耦。
     */
    public enum LlmModel implements DescriptorUseableShortComment {
        gpt_3_5("GPT-3.5：速度最快，成本最低"),
        gpt_4("GPT-4：综合能力最强"),
        gpt_4_32k("GPT-4 32K：支持超长上下文");
        public final String label;

        LlmModel(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<StreamLlmPayloadConfig> {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_TARGET_VARIABLE_ID, WidgetOptionHelper::getWorkshopVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "LLM 调用参数";
        }
    }
}
