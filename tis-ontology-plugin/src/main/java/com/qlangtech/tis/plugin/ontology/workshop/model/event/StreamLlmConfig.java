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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 调用大模型并把结果流式写入目标变量（前端 {@code stream-llm}）。
 *
 * <p>类名刻意不带 {@code Event} 后缀：FQCN→kebab 推导（{@code resolveDefinitionKind}）去掉尾部
 * {@code Config} 后得到的就是前端联合类型的 {@code type} 字面量，带上后缀就会推成
 * {@code stream-llm-event}，与前端对不上。所有事件子类都遵循这条命名约定。
 */
public class StreamLlmConfig extends EventConfig {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 1, validate = {Validator.require})
    public StreamLlmPayloadConfig config;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return "Stream_LLM";
        }

        @Override
        public String shortComment() {
            return "调用大模型";
        }
    }
}
