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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 按钮组的呈现形态 —— 内联平铺（{@link InlineButtonGroupType}）/ 折叠菜单
 * （{@link MenuButtonGroupType}）/ 主按钮 + 菜单两段式（{@link TwoPartButtonGroupType}）三选一。
 *
 * <p>为什么是多态而不是「一个 {@code buttonType} 枚举 + 一个可空的 {@code layout} 字段」：
 * {@code layout}（水平 / 垂直排列）只在「内联平铺」下有语义 —— 菜单与两段式的按钮
 * 由 Ant Design 的 {@code nz-dropdown-menu} / {@code nz-button-group} 固定排布，根本不读
 * 排列方向。扁平化后「选了 menu 却还配着垂直排列」这类<b>自相矛盾</b>的配置无法被编译器约束，
 * 而框架又只有静态的 {@code advance} 分组、没有条件可见性，矛盾字段只能永远摊在表单上。
 * 多态把「带哪些载荷」交给子类 —— 只有 {@link InlineButtonGroupType} 声明 {@code layout}，
 * 另两个子类零字段。这与 {@link Pagenator}、{@link SizingMode}、{@code VariableDefinitionConfig}
 * 同一处理方式。
 *
 * <p>三个子类的短 key 与前端 {@code ButtonGroupTypeToken} 字面量对齐：
 * {@code inline} / {@code menu} / {@code two-part}（前端 {@code resolveImplKind(impl,
 * "ButtonGroupType")} 剥后缀推导，见各子类 {@code getDisplayName()}）。
 *
 * <p>⚠️ 本类与其 {@code BasicDescriptor} <b>都不得</b>带 {@code @TISExtension}、
 * 也不得有嵌套 Descriptor —— sezpoz 索引不过滤抽象类，加了会让抽象类型静默混进插件面板
 * （同 {@link Pagenator.BasicDescriptor}、{@link SizingMode.BasicDescriptor} 的口径）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/20
 * @see impl.ButtonGroupWidget#buttonType
 */
public abstract class ButtonGroupType implements Describable<ButtonGroupType>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<ButtonGroupType> {
    }
}
