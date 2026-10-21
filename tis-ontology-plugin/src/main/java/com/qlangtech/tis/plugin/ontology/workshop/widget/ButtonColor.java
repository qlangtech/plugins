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
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.button.group.CustomButtonColor;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.button.group.DangerButtonColor;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.button.group.NoneButtonColor;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.button.group.PrimaryButtonColor;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.button.group.SuccessButtonColor;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.button.group.WarningButtonColor;

import java.io.Serializable;

/**
 * 按钮颜色 —— {@link NoneButtonColor} / {@link PrimaryButtonColor} / {@link SuccessButtonColor} /
 * {@link WarningButtonColor} / {@link DangerButtonColor} / {@link CustomButtonColor} 六选一。
 *
 * <p>为什么是多态而不是「一个 {@code color} 枚举 + 一个可空的 {@code customColor} 字段」：
 * {@code customColor}（十六进制色值）只在「自定义」下有语义 —— 其余五档都是固定品牌色，
 * 不该有可填的自定义色值。扁平化后「选了 primary 却还填着 customColor」这类<b>自相矛盾</b>
 * 的配置无法被编译器约束，而框架又只有静态 {@code advance} 分组、没有条件可见性。
 * 多态把「带哪些载荷」交给子类 —— 只有 {@link CustomButtonColor} 声明 {@code customColor}，
 * 其余子类零字段。这与 {@link ButtonGroupType}、{@link Pagenator}、{@link SizingMode}
 * 同一处理方式。
 *
 * <p>六个子类的短 key 与前端 {@code ButtonColor} 值形态的 {@code type} 字面量对齐：
 * {@code none} / {@code primary} / {@code success} / {@code warning} / {@code danger} /
 * {@code custom}（前端 {@code resolveImplKind(impl, "ButtonColor")} 剥后缀推导，
 * 见各子类 {@code getDisplayName()}）。
 *
 * <p>⚠️ 本类与其 {@code BasicDescriptor} <b>都不得</b>带 {@code @TISExtension}、
 * 也不得有嵌套 Descriptor —— sezpoz 索引不过滤抽象类，加了会让抽象类型静默混进插件面板
 * （同 {@link ButtonGroupType.BasicDescriptor}、{@link Pagenator.BasicDescriptor} 的口径）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/20
 * @see WidgetButtonConfig#color
 */
public abstract class ButtonColor implements Describable<ButtonColor>, Serializable {

    private static final long serialVersionUID = 1L;

    public abstract static class BasicDescriptor extends Descriptor<ButtonColor> {
    }
}
