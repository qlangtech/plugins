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
 * 只能单选：点击对象写回宿主的 {@code activeObjectVar}，不渲染勾选框。
 *
 * <p>本类<b>零 {@code @FormField} 字段</b> —— 没有字段可配，故同包下<b>没有</b>对应
 * {@code NoObjectSelection.json}（写了是死文件：TIS 只在类上存在表单字段时才去查同名资源）。
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
