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
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.event.EventConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

import java.io.Serializable;

/**
 * {@link com.qlangtech.tis.plugin.ontology.workshop.widget.impl.TabsWidget} 里的一个标签页。
 *
 * <p>{@link #targetPage} 与 {@link #onSelect} 分别服务两种模式，由
 * {@code TabsWidget.location} 决定用哪个：
 * <ul>
 *   <li>{@code location = header} —— 切换 Page，读 {@link #targetPage}，
 *       调用前端 {@code LayoutService.navigateToPage}；</li>
 *   <li>{@code location = section} —— 原地触发事件，读 {@link #onSelect}，
 *       调用前端 {@code EventService.executeEvents}。</li>
 * </ul>
 * 两者都留空时该标签页只是个静态标题。之所以不做成「二选一的多态」，是因为这两个字段
 * 并非互斥的两种配置形态，而是<b>同一份配置在两种落位下的两种用途</b> —— 同一个
 * {@code TabsWidget} 挪动位置时无需重配。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class WidgetTabConfig implements Describable<WidgetTabConfig>, Serializable, IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    public static final String KEY_TARGET_PAGE = "targetPage";

    /** 标签名，同时作为子表单行的唯一标识 */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String title;

    /** header 模式下切换到的页面（选项为当前域下的全部页面） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 1, advance = false)
    public String targetPage;

    /** section 模式下选中该标签时触发的事件 */
    @FormField(ordinal = 2, advance = false)
    public EventConfig onSelect;

    @Override
    public String identityValue() {
        return this.title;
    }

    @TISExtension
    public static class DftDescriptor extends Descriptor<WidgetTabConfig> {

        public DftDescriptor() {
            super();
            this.registerSelectOptions(KEY_TARGET_PAGE, WidgetOptionHelper::getWorkshopPageOptions);
        }

        @Override
        public String getDisplayName() {
            return "Tab";
        }
    }
}
