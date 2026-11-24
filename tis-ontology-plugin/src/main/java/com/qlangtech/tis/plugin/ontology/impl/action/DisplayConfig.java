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

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

/**
 * Action 运行时表单的显示配置（Workshop 按钮组打开的模态框长什么样）。
 *
 * <p>「配不配」由<b>子类类型</b>承担（同 {@link CustomizeContent} 的手法）：
 * <ul>
 *   <li>{@link DisplayConfigOff} —— 不配置，前端沿用框架缺省（紧凑布局 + 600px 宽）。
 *       零 {@code @FormField} 字段，因此也不写 {@code .json}。</li>
 *   <li>{@link DisplayConfigOn} —— 配置，携带紧凑布局开关与表单宽度两个必填项。</li>
 * </ul>
 *
 * <p>下发给前端的通道是 {@code DefaultOntologyAction} 的 {@code resolveRuntimeForm}
 * bizresult 顶层 {@code displayConfig} key（{@link #runtimeProps} 扁平化产物），
 * 刻意不走 {@code ActionFormDescriptorBuilder} 的 extractProps/groovy 通道 ——
 * 那条路受 contentHash 缓存约束，displayConfig 变更不会改变 groovy/json 产物哈希，
 * 预览缓存不会失效。
 *
 * <p>多态字段，TIS 自动检测为嵌套子表单；表单默认选哪一项由 {@code ActionParameters.json}
 * 里该字段的 {@code dftVal}（{@code "off"}）逐字匹配子类描述符的 {@code getDisplayName()}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/21
 * @see ActionParameters#displayConfig
 */
public abstract class DisplayConfig implements Describable<DisplayConfig> {

    /** 扁平载荷里「紧凑垂直布局」的 key，前端 {@code ActionDisplayConfig} 逐字对应 */
    public static final String KEY_COMPACT_VERTICAL_LAYOUT = "compactVerticalLayout";

    /** 扁平载荷里「表单宽度」的 key，前端 {@code ActionDisplayConfig} 逐字对应 */
    public static final String KEY_FORM_WIDTH = "width";

    /**
     * 扁平化为前端可直读的运行时载荷；{@code null}（未配置 / off 形态）时返回 {@code null}，
     * 前端据此走框架缺省。
     *
     * <p>静态纯函数：{@code DefaultOntologyAction#resolveRuntimeForm} 调用，
     * 单测无需起 httpProcess 上下文即可覆盖。
     */
    public static JSONObject runtimeProps(DisplayConfig displayConfig) {
        if (displayConfig instanceof DisplayConfigOn on) {
            JSONObject props = new JSONObject();
            props.put(KEY_COMPACT_VERTICAL_LAYOUT, Boolean.TRUE.equals(on.compactVerticalLayout));
            props.put(KEY_FORM_WIDTH, on.formWidth);
            return props;
        }
        return null;
    }
}
