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

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;

import java.util.List;

/**
 * 即时消息通知渠道（如钉钉、企业微信）。
 *
 * <p>通知目标是 IM 侧的用户 ID 或群组 ID，各 IM 平台的 ID 规则不统一，
 * 无法做统一格式校验，只兜底非空（{@link Validator#require}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/23
 */
public class ImNotifyChannel extends NotifyChannel {

    /**
     * 通知目标：IM 用户 ID 或群组 ID。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String imTarget;

    @Override
    public List<UDFDesc> getLiteria() {
        return List.of(new UDFDesc("imTarget", this.imTarget));
    }

    @TISExtension
    public static class DftDescriptor extends BaseDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "即时消息";
        }

        @Override
        public String shortComment() {
            return "IM 即时消息（如钉钉、企业微信），目标为用户 ID 或群组 ID";
        }
    }
}
