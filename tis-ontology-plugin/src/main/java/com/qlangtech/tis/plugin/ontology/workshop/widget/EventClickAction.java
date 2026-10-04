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
import com.qlangtech.tis.plugin.ontology.workshop.model.event.EventConfig;

import java.util.List;

/**
 * 点击后触发一串 Workshop 事件（切换页面、开关浮层、写变量、刷新数据…）。
 *
 * <p>{@link EventConfig} 是多态体系，其每个子类的短 key（由 FQCN 推导，见
 * {@link WidgetClickAction#getShortKeyName()}）与前端 {@code WorkshopEvent} 联合类型的
 * {@code type} 字面量<b>逐一相同</b> —— 前端适配器因此是机械的：
 * {@code { type: resolveDefinitionKind(cfg), ...cfg }}，无需逐个事件写映射表。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class EventClickAction extends WidgetClickAction {

    private static final long serialVersionUID = 1L;

    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = EventConfig.class,
            ordinal = 0, validate = {Validator.require})
    public List<EventConfig> events;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return "Event";
        }

        @Override
        public String shortComment() {
            return "触发事件";
        }
    }
}
