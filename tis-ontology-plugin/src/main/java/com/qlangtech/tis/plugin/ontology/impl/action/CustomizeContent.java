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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.util.Optional;

/**
 * Action 表单上「可以改文案」的位置的统一载体，对应 Palantir 里
 * <i>Customize submit button</i> / <i>Customize success message</i> 这两个开关。
 *
 * <p>「改不改」由<b>子类类型</b>承担，而不是「一个布尔开关 + 一个文案字段」两个平铺字段：
 * <ul>
 *   <li>{@link NoneCustomizeContent} —— 不改，用框架缺省文案。零 {@code @FormField} 字段，
 *       因此也不写 {@code .json}；{@code getDisplayName()} 返回 {@link Descriptor#SWITCH_OFF}。</li>
 *   <li>{@link DefaultCustomizeContent} —— 改，带上一个 {@code content} 字段。</li>
 * </ul>
 * 这样「关掉了开关却带着一段文案」这种自相矛盾的状态在类型层面即不可表示。
 *
 * <p>多态字段，TIS 自动检测为嵌套子表单；表单默认选哪一项由 {@code .json} 里该字段的
 * {@code dftVal} 决定 —— 它逐字匹配子类描述符的 {@code getDisplayName()}，所以两边
 * 写的是同一个字符串（{@code "off"}），改一边就要改另一边。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/14
 * @see ActionParameters#submitButton
 * @see ActionParameters#successMessage
 */
public abstract class CustomizeContent implements Describable<CustomizeContent> {

//    @Override
//    public Descriptor<CustomizeContent> getDescriptor() {
//        return null; // 框架自动注入
//    }

    /**
     * 定制后的文案；没有启用定制、或启用后内容为空时返回 {@link Optional#empty()}，
     * 调用方据此回退到框架缺省文案。
     */
    public abstract Optional<String> content();
}
