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

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;

/**
 * 不支持多选：点对象只写回宿主的 {@code activeObjectVar}，不渲染勾选框。
 *
 * <p>本类<b>零 {@code @FormField} 字段</b> —— 没有字段可配，故同包下<b>没有</b>对应
 * {@code NoObjectSelection.json}（写了是死文件：TIS 只在类上存在表单字段时才去查同名资源）。
 *
 * <h3>为什么 {@code activeObjectVar} 不搬到这里</h3>
 * <p>{@code activeObjectVar}（「当前选中对象」，Palantir 的 <i>Active object</i>）是宿主的
 * <b>基础输出</b>，与多选开关无关：即使开启了多选，点击某一行照样应当更新「当前选中对象」。
 * 换个说法，本类的语义是「多选 = 关」，而<b>不是</b>「单选 = 开」—— 单选在任何子类下都可用。
 * 若把它搬进本类，{@link DefaultObjectSelection} 下就取不到该字段，只能在两个子类里各放一份
 * （字段重复）才能保持行为，且要为此引入 XStream 迁移。Palantir 的文档也是同一口径：
 * 「Active object」是本 Widget 的<b>两个输出变量之一</b>（另一个是 Selected objects，
 * 且「只在 Enable multi-select 打开时才有值」）。
 *
 * <p>因此本类保持零字段，宿主上的 {@code activeObjectVar} 与
 * {@code autoSelectActiveObject} 共同描述单选行为。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/10
 */
public class NoObjectSelection extends ObjectSelection {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean multiSelectEnabled() {
        return false;
    }

    @Override
    public String resolveSelectedObjectsVar() {
        return null;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            // 必须是框架常量 "off"：ObjectListWidget.json 的 objectSelection.dftVal 要逐字对它，
            // 前端 resolveObjectListSelection 也按这个短 key 判态。写成「关闭」不报错，只会静默换子类。
            return SWITCH_OFF;
        }

        @Override
        public String shortComment() {
            return "只能单选，点击对象写回当前选中项变量";
        }
    }
}
