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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

/**
 * 点击后执行一个本体动作（Ontology Action）。
 *
 * <p>对应前端 {@code OntologyService.executeAction(ontologyName, actionId, request)}：
 * 动作名来自 {@link #actionId}，入参对象来自 {@link #selectedObjectsVar} 绑定的对象集变量。
 * 本体域名不需要单独配置 —— 前端从 {@code OntologyService.ontologyDomainId} 取当前模块上下文
 * （由 {@code ModuleStateService.loadModule} 写入）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class OntologyActionClickAction extends WidgetClickAction {

    private static final long serialVersionUID = 1L;

    public static final String KEY_ACTION_ID = "actionId";
    public static final String KEY_SELECTED_OBJECTS_VAR = "selectedObjectsVar";

    /**
     * 要执行的本体动作（当前本体域中已定义的 Action 名）
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String actionId;

    /**
     * 作为动作入参的对象来源 —— 该变量中的对象 rid 列表会被送进
     * {@code ActionExecutionRequest.objectRids}。
     */
    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE)
    public String selectedObjectsVar;

    /**
     * 执行前是否二次确认
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public Boolean confirmBeforeExecute = false;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_ACTION_ID, WidgetOptionHelper::getOntologyActionOptions);
            this.registerSelectOptions(KEY_SELECTED_OBJECTS_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
        }

        @Override
        public String shortComment() {
            return "执行本体动作";
        }

        @Override
        public String getDisplayName() {
            return "Ontology_Action";
        }
    }
}
