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
import com.qlangtech.tis.extension.DescriptorUseableShortComment;

import java.io.Serializable;

/**
 * 「点击之后做什么」的多态基类。
 *
 * <p>四个子类一一对应前端 {@code button-group.component.ts} 的 {@code onButtonClick} 里的四个
 * {@code switch} 分支，{@code switch} 判据是<b>短 key</b>
 * （{@link #getShortKeyName()} 的推导规则，与 {@code models/variable.model.ts}
 * 的 {@code resolveDefinitionKind} 同源）：
 *
 * <table>
 *   <tr><th>子类</th><th>短 key</th><th>前端行为</th></tr>
 *   <tr><td>{@link OntologyActionClickAction}</td><td>{@code ontology-action-click-action}</td>
 *       <td>调 {@code OntologyService.executeAction}</td></tr>
 *   <tr><td>{@link EventClickAction}</td><td>{@code event-click-action}</td>
 *       <td>调 {@code EventService.executeEvents}</td></tr>
 *   <tr><td>{@link UrlClickAction}</td><td>{@code url-click-action}</td>
 *       <td>{@code window.open}</td></tr>
 *   <tr><td>{@link ExportClickAction}</td><td>{@code export-click-action}</td>
 *       <td>导出当前对象集为 csv/xlsx</td></tr>
 * </table>
 *
 * <p><b>为什么是多态而不是「type 枚举 + 一堆可空载荷字段」</b>：四型的载荷字段毫无交集
 * （{@code actionId} / {@code events} / {@code url} / {@code format}），扁平化后表单会同时
 * 渲染出 9 个字段、其中 7 个与当前选择无关，且没有任何机制能按枚举值隐藏它们
 * （框架只有静态的 {@code advance} 分组，没有条件可见性）。同 {@link ObjectListDisplay}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 * @see WidgetButtonConfig#onClick
 */
public abstract class WidgetClickAction implements Describable<WidgetClickAction>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 供前端 {@code switch} 派发用的短 key。
     *
     * <p>由实现类的 FQCN 推导，规则与前端 {@code resolveDefinitionKind} 逐字对应：
     * 取简单类名、剥掉末尾 {@code Config}、在「小写/数字 后紧跟大写」与
     * 「连续大写 后紧跟 大写+小写」两个边界插分隔符、转小写。
     * {@code OntologyActionClickAction} → {@code ontology-action-click-action}。
     */
    public String getShortKeyName() {
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
     * {@link ObjectListDisplay.BasicDescriptor} 的同类写法。
     */
    public abstract static class BasicDescriptor extends Descriptor<WidgetClickAction> implements DescriptorUseableShortComment {
    }
}
