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
package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 点击后在新窗口/当前窗口打开一个 URL。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class UrlClickAction extends WidgetClickAction {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String url;

    /**
     * 是否在新标签页打开。
     * <p>
     * 前端 {@code window.open(url, target)} 的第二参数由此决定：{@code true} → {@code _blank}，
     * {@code false} → {@code _self}。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public Boolean openInNewTab = true;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return "URL";
        }

        @Override
        public String shortComment() {
            return "跳转链接";
        }
    }
}
