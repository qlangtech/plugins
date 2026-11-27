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

package com.qlangtech.tis.plugin.ontology.impl.action.widget;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.EnumFieldMode;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.MultiDescribleElementSetSelector;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * 下拉控件的单个候选项（`val` + `label`）。
 *
 * <p>供 {@code SelectableWidget} / {@code MultiSelectableWidget} 以 {@code @SubForm} 列表的方式收集。
 * 按工程约定，列表化结构对象不得让用户手写 JSON 字符串，必须走子表单。
 *
 * <p>本类实现 {@link IPluginStore.MultiDescribleElement}（该接口本身即继承 {@link IdentityName}）且
 * {@code val} 标 {@code identity = true}，这是两种列表收集方式的共同硬性要求（本工程约定与 create-plugin skill 中
 * 「聚合属性插件不实现 IdentityName」相反，以本工程既有实现为准）：缺 {@code IdentityName} 会在下钻编辑明细时
 * ClassCastException，缺 {@code identity = true} 字段则在构造期直接抛异常；而 {@code MULTI_DESCRIBLE_PLUGIN}
 * 这条路（如 {@code EnumWidget.options}）还要求元素类可赋值给 {@code MultiDescribleElement}，
 * 否则 {@code PropertyType} 构造期就抛 IllegalStateException。
 *
 * <p>同理，descriptor 也必须继承 {@link MultiDescribleElementSetSelector.MultiDescribleElementDescriptor}：子列表的展现形态
 * （{@code viewStyle} / {@code colsCfg} / {@code maxHeight}）是由<b>元素自己的</b> descriptor 在
 * {@code getExtractProps} 里下发的，而这个方法只在该基类上实现 —— 挂一个普通 {@code Descriptor} 编译期不报错，
 * 但前端拿不到这三项，子表格按默认形态渲染（列标题、列宽全丢）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/15
 */
public class WidgetOptionConfig implements Describable<WidgetOptionConfig>, IPluginStore.MultiDescribleElement {

    /**
     * 选项值，即最终提交给业务的值。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String val;

    /**
     * 选项显示文本；留空则前端回落显示 {@link #val}。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {})
    public String label;

    /**
     * 选项内容说明信息；当显示模式使用 {@link EnumFieldMode#FLATTED_SINGLE_CHOICE} 或者 {@link EnumFieldMode#MODEL_ASSIST} 可以作为选项的辅助说明信息使用
     */
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {})
    public String description;

    @Override
    public String identityValue() {
        return this.val;
    }

    /**
     * 转成候选项线格式 {@link Option}：label 留空（含纯空白）回落 {@link #val}；
     * {@link #description} 经 {@code Option#description()} 由 {@code Option.toJson} 写进 help 键，
     * FLATTED_SINGLE_CHOICE / MODEL_ASSIST / 缺省下拉三种显示模式的前端渲染器都已有 help 槽位，
     * 无需前端适配。EnumWidget 与 ManualOpsSource 两条选项下发链共用本方法，避免各拼各的。
     */
    public Option toOption() {
        final String desc = this.description;
        return new Option(StringUtils.defaultIfBlank(label, val), val) {
            @Override
            public String description() {
                return desc;
            }
        };
    }

    @TISExtension
    public static class DftDescriptor extends MultiDescribleElementSetSelector.MultiDescribleElementDescriptor<WidgetOptionConfig> {

        @Override
        public String getDisplayName() {
            return "Widget_Option";
        }

        @Override
        public int maxHeight() {
            return 400;
        }

        /**
         * 选项子表格按表格展现。注意缺省 {@code ViewStyle.List} 分支完全不消费 {@code colsCfg}
         * （每行只渲染 identity 键），不切 Table 的话 {@code colsConfig()} 就是死配置。
         */
        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            return Lists.newArrayList( //
                    new ColConfig("val", 25).setClickable()
                    , new ColConfig("label", 25)
                    , new ColConfig("description"));
        }

        /**
         * 本接口在 {@code Descriptor} 上没有默认实现（父类只做 instanceof 探测），必须给出。
         */
        @Override
        public String shortComment() {
            return "控件的候选项：值 + 显示文本";
        }
    }
}
