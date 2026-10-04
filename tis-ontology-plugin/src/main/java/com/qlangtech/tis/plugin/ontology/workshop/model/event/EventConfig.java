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
package com.qlangtech.tis.plugin.ontology.workshop.model.event;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;

/**
 * Workshop 事件的多态基类 —— 每个子类对应前端 {@code WorkshopEvent} 联合类型里的一种事件。
 *
 * <h3>类名即协议：FQCN → 短 key 的推导</h3>
 * 子类的类名<b>刻意取名</b>成「剥掉末尾 {@code Config} 后按驼峰边界插连字符、转小写」恰好等于
 * 前端 {@code type} 字面量的形式（推导规则见前端 {@code models/variable.model.ts} 的
 * {@code resolveDefinitionKind}，后端同规则见 {@link #getEventType()}）：
 *
 * <table>
 *   <tr><th>子类</th><th>推导出的 {@code type}</th></tr>
 *   <tr><td>{@link OpenOverlayConfig}</td><td>{@code open-overlay}</td></tr>
 *   <tr><td>{@link SwitchToPageConfig}</td><td>{@code switch-to-page}</td></tr>
 *   <tr><td>{@link RefreshDataInModuleConfig}</td><td>{@code refresh-data-in-module}</td></tr>
 * </table>
 *
 * 因此<b>不要</b>给子类名加 {@code Event} 后缀 —— {@code OpenOverlayEventConfig} 会推导出
 * {@code open-overlay-event}，与前端对不上。前端适配器正是靠这一点做到机械转换：
 * {@code { type: resolveDefinitionKind(cfg), ...cfg }}，无需逐个事件手写映射表。
 *
 * <h3>为什么是多态，而不是「一个 eventType 枚举 + 若干可空载荷字段」</h3>
 * 20 种事件的载荷字段几乎两两不同（{@code pageName} / {@code overlayName} / {@code sectionName}
 * / {@code objectRid} / …）。扁平化后表单会同时渲染出十几个字段、其中绝大多数与当前所选事件无关，
 * 且框架<b>没有任何条件可见性机制</b>（只有静态的 {@code advance} 分组）能按枚举值隐藏它们，
 * 于是「选了 open-overlay 却填了 pageName」这种自相矛盾的状态在表单上完全可达。
 * 同 {@link com.qlangtech.tis.plugin.ontology.workshop.widget.ObjectListDisplay}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public abstract class EventConfig implements Describable<EventConfig>, Serializable, IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    /**
     * 事件名称 —— 子表单行的唯一标识。
     *
     * <p>它是<b>基础字段而不是各子类各自声明</b>的：{@code MultiDescribleElement} 要求
     * descriptor 上恰好有一个 {@code identity = true} 的字段，缺了会在
     * {@code DescriptorsJSON} 构建表单时抛
     * {@code property identityProp can not be null} 让整张表单打不开。
     * 事件本身没有天然的「名字」（{@code switch-to-page} 的载荷是 pageName，
     * 而不是标识），故统一由使用者起一个便于在列表中辨认的短名。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    @Override
    public String identityValue() {
        return this.name;
    }

    /**
     * 事件类型的短 key —— 与前端 {@code WorkshopEvent} 的 {@code type} 字面量逐一相同。
     *
     * <p>后端不需要序列化它（前端从 {@code impl} 自行推导），留作前后端契约的<b>单一事实来源</b>，
     * 供 {@code TestEventConfigParity} 断言与前端字面量集合相等。
     */
    public String getEventType() {
        String simpleName = this.getClass().getSimpleName();
        String name = simpleName.endsWith("Config")
                ? simpleName.substring(0, simpleName.length() - "Config".length()) : simpleName;
        return name
                .replaceAll("([a-z0-9])([A-Z])", "$1-$2")
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2")
                .toLowerCase();
    }

    /**
     * 各子类 descriptor 的共同基类 —— 泛型参数固定为多态基类，见
     * {@link com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetClickAction.BasicDescriptor}
     * 的同类写法。
     */
    public abstract static class BasicDescriptor extends Descriptor<EventConfig> implements DescriptorUseableShortComment {
    }
}
