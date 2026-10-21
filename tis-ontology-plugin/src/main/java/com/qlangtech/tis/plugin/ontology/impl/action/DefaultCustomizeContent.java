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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.Optional;

/**
 * 「定制」形态的 {@link CustomizeContent}：用户自己填写要展示的文案。
 *
 * <p>{@link #getDisplayName()} 返回 {@link Descriptor#SWITCH_DEFAULT}（即 {@code "default"}），
 * 与 {@link NoneCustomizeContent} 的 {@code "off"} 一起构成该多态字段的两个选项。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/14
 * @see NoneCustomizeContent
 */
public class DefaultCustomizeContent extends CustomizeContent {

    /**
     * 定制后的文案本体。选定本子类（而不是 {@link NoneCustomizeContent}）即表示启用定制，
     * 所以这里必须填，不能留空 —— 留空与「不定制」在语义上无从区分。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String content;

    /**
     * 去空白后仍为空一律按「没有定制」处理，调用方据此回退到框架缺省文案。
     */
    @Override
    public Optional<String> content() {
        return Optional.ofNullable(this.content).map(String::trim).filter((c) -> !c.isEmpty());
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<DefaultCustomizeContent> {

        @Override
        public String getDisplayName() {
            return SWITCH_DEFAULT;
        }
    }
}
