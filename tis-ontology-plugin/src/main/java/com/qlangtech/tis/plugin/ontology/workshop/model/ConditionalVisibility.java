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
package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.io.Serializable;

/**
 * 条件可见性配置：引用一个 Workshop 变量，按变量值的真假控制宿主（Section / Widget）的显示隐藏。
 * <p>
 * 被 <b>Widget 基类</b>
 * （{@link com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetDisplayConfig}）与
 * {@code WorkshopSection} 共同复用，因此与它们同处本模块；
 * 扩展点接口 {@code IWorkshopWidget} 与类目注册 {@code WorkshopWidgetHeteroEnum}
 * 仍留在 tis-plugin 抽象层。
 * <p>
 * 与之相对，{@code VariableBasedVisibility} 支持 equals / notEmpty / greaterThan 等
 * <b>比较表达式</b>，目前仅 Overlay 使用；两者能力不同，尚未收敛。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/13
 */
public class ConditionalVisibility implements Describable<ConditionalVisibility>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String KEY_VARIABLE_ID = "variableId";

    /**
     * 被引用的变量名（模块内唯一，大小写不敏感）。
     *
     * <p>字段名沿用前端读取的 {@code variableId}，但存的是变量<b>名字</b>而非自动生成的 UUID
     * （与 {@code objectSetVar} 等一致：模块内引用一律用名字，UUID 只决定落盘文件名）。
     * 前端 {@code VariableService.getCachedValue(nameOrId)} 两者都认。
     *
     * <p>选项由本类自己的 descriptor 注册（见下），宿主无需各自再注册一遍。
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE)
    public String variableId;

    /**
     * 变量值为假时宿主的呈现方式。
     *
     * <h3>为什么不是布尔</h3>
     * 早先这里是 {@code Boolean hideWhenFalse}，「false」一档的语义是「变量为假时才显示」
     * （一个反转开关）。它既难读，又无法表达前端按钮真正需要的第三种状态 ——
     * {@code disabled}（出现但置灰不可点，提示「这里有个操作，但当前不适用」）。
     * 换成一个描述「条件为假时怎么办」的枚举后，两种需求统一，且与前端
     * {@code button-group.component.ts} 已在读的 {@code stateIfFalse} 逐字一致。
     * 反转型配置无任何使用方（{@code hideWhenFalse=false} 在全仓无赋值点），故直接移除。
     *
     * <p>对 Section / Widget 这类没有「禁用」形态的宿主，{@link StateIfFalse#disabled}
     * 一律按 {@link StateIfFalse#hidden} 处理。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public StateIfFalse stateIfFalse = StateIfFalse.hidden;

    /** 条件为假时的呈现方式；取值固定，硬编码于此（CLAUDE.md 原则二） */
    public enum StateIfFalse implements DescriptorUseableShortComment {
        hidden("隐藏：条件不满足时该元素不出现"),
        disabled("禁用：条件不满足时元素置灰不可交互");

        private final String comment;

        StateIfFalse(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<ConditionalVisibility> {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_VARIABLE_ID, WidgetOptionHelper::getWorkshopVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Conditional Visibility";
        }
    }
}
