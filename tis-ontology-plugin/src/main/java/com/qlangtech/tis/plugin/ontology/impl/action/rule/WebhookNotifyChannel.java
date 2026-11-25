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
 * Webhook 回调通知渠道。
 *
 * <p>通知地址为回调 URL，走 {@link Validator#url} 格式校验。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/23
 */
public class WebhookNotifyChannel extends NotifyChannel {

    /**
     * Webhook 回调地址，如 {@code https://example.com/hook}。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.url})
    public String webhookUrl;

    @Override
    public List<UDFDesc> getLiteria() {
        return List.of(new UDFDesc("webhookUrl", this.webhookUrl));
    }

    @TISExtension
    public static class DftDescriptor extends BaseDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Webhook";
        }

        @Override
        public String shortComment() {
            return "Webhook 回调，向指定 URL 推送通知";
        }
    }
}
