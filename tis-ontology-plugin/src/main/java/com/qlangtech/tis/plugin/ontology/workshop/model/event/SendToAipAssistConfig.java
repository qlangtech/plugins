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
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/** 向 AIP Assist 发送提问（前端 {@code send-to-aip-assist}）。 */
public class SendToAipAssistConfig extends EventConfig {

    private static final long serialVersionUID = 1L;

    /**
     * 前端模型里 prompt 是 {@code string | { variableId: string }} 联合类型，
     * 这里只落文本分支；变量引用分支的钩子尚未实现，待需要时再补一个 SELECTABLE 字段。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String prompt;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT)
    public String defaultChatbot;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return "Send_To_AIP_Assist";
        }

        @Override
        public String shortComment() {
            return "向 AIP Assist 提问";
        }
    }
}
