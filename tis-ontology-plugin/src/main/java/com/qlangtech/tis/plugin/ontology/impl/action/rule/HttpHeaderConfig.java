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

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/**
 * 一行 HTTP 请求头，供 {@link WebhookRule} 的 {@code headers} 以
 * {@code MULTI_DESCRIBLE_PLUGIN} 子表单渲染。
 *
 * <p>取代原先 {@code Header-Name: Header-Value\n...} 的扁平文本。结构化顺带修掉一个隐性坑：
 * 旧格式按第一个冒号切分，header 值本身含冒号时（如 {@code X-Url: https://a}）虽然侥幸切对，
 * 但凡是值里出现嵌套 {@code Name: Value} 形态的场景都会切错；现在不存在切分，问题消失。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/11
 */
public class HttpHeaderConfig implements Describable<HttpHeaderConfig>,
        IPluginStore.MultiDescribleElement {

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    /**
     * 值里可以写 {@code ${paramName}}，与 {@code bodyTemplate} 的模板语法一致。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {})
    public String value;

    @Override
    public String identityValue() {
        return this.name;
    }

    @TISExtension
    public static class DftDescriptor extends IPluginStore.MultiDescribleElementDescriptor<HttpHeaderConfig> {

        @Override
        public String getDisplayName() {
            return "Header";
        }

        @Override
        public String shortComment() {
            return "HTTP 请求头";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            return Lists.newArrayList(
                    new ColConfig("name").setClickable(),
                    new ColConfig("value"));
        }
    }
}
