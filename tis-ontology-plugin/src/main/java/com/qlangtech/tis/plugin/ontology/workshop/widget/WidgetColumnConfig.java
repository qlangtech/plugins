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

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 列/行的结构化配置，供 @SubForm 驱动子表单渲染。
 * 用于 ObjectTable、PropertyList 等 Widget 的列/行定义，
 * 不再让用户手写 JSON 数组字符串。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class WidgetColumnConfig implements Describable<WidgetColumnConfig>, IPluginStore.MultiDescribleElement {

    /**
     * 自动生成的列宽，与 {@code WidgetColumnConfig.json} 里 {@code width} 的 dftVal 保持一致，
     * 使「由对象集变量自动填充的行」与「用户手工新增的行」完全一致。
     */
    public static final int DEFAULT_WIDTH = 120;

    /**
     * 数据属性名（对应对象上的 property key），作为 SubForm 表行的唯一标识
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String prop;

    /**
     * 列/行在 UI 上的显示标签
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String label;

    /**
     * 列宽（像素），仅 ObjectTable 使用
     */
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER)
    public Integer width;

    /**
     * 格式化方式
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM, advance = false)
    public ColumnFormat format;

    /**
     * 文本对齐方式
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM, advance = false)
    public ColumnAlign align;

    /**
     * 是否可排序
     */
    @FormField(ordinal = 5, type = FormFieldType.ENUM, advance = false)
    public Boolean sortable;

    @Override
    public String identityValue() {
        return this.prop;
    }

    // ===============================================================
    //  OntologyProperty → WidgetColumnConfig
    // ===============================================================

    /**
     * 由一个 Ontology 属性生成一行列/行配置。
     *
     * <p>字段取值口径：
     * <ul>
     *   <li>{@code prop} 取 {@link OntologyProperty#getName()} —— 它是 Ontology 侧唯一稳定的标识，
     *       且带 {@code Validator.db_col_name}，天然满足 IdentityName 的可用性要求。
     *       本字段刻意不加 {@code Validator.identity}：那会拒绝存量手工行，且来源已保证是合法列名。</li>
     *   <li>{@code label} 取 {@code description}（人写的显示文本），为空回落到 {@code name}。
     *       {@code description} 是<b>可空</b>的，必须 defaultIfBlank，否则自动生成的列会缺显示文本。</li>
     *   <li>{@code format} / {@code align} 由属性类型推导，见
     *       {@link #formatOf(OntologyType)} 与 {@link #alignOf(ColumnFormat)}。</li>
     * </ul>
     */
    public static WidgetColumnConfig createFrom(OntologyProperty prop) {
        Objects.requireNonNull(prop, "prop can not be null");
        WidgetColumnConfig cfg = new WidgetColumnConfig();
        cfg.prop = prop.getName();
        cfg.label = StringUtils.defaultIfBlank(prop.getDescription(), prop.getName());
        cfg.width = DEFAULT_WIDTH;
        cfg.format = formatOf(prop.parseOntologyType());
        cfg.align = alignOf(cfg.format);
        cfg.sortable = Boolean.FALSE;
        return cfg;
    }

    /**
     * 批量转换，供 valueChangePipe 的 render 与 getEnumableCandidateSet 共用。
     */
    public static List<WidgetColumnConfig> createFrom(List<OntologyProperty> props) {
        if (props == null || props.isEmpty()) {
            return Collections.emptyList();
        }
        List<WidgetColumnConfig> cols = new ArrayList<>(props.size());
        for (OntologyProperty prop : props) {
            cols.add(createFrom(prop));
        }
        return cols;
    }

    /**
     * Ontology 属性类型 → 单元格格式化方式。
     *
     * <p>刻意写成<b>穷尽式 switch expression 且不写 default</b>：Java 会强制覆盖枚举全部常量，
     * 因此 tis-plugin 里新增一个 {@link OntologyType} 常量会让本处编译失败，
     * 而不是静默地落到 text 上。
     */
    public static ColumnFormat formatOf(OntologyType type) {
        return switch (Objects.requireNonNull(type, "type can not be null")) {
            case STRING, VECTOR, ARRAY, STRUCT, GEOPOINT, GEOSHAPE, MEDIA_REFERENCE -> ColumnFormat.text;
            case INTEGER, SHORT, LONG, BYTE, FLOAT, DOUBLE, DECIMAL -> ColumnFormat.number;
            case BOOLEAN -> ColumnFormat.boolean_;
            case DATE, TIMESTAMP -> ColumnFormat.date;
        };
    }

    /**
     * 由格式化方式推导对齐方式：数值右对齐、布尔居中、其余左对齐。
     * 比 {@code .json} 里一律 left 的默认值更符合表格阅读习惯。
     */
    public static ColumnAlign alignOf(ColumnFormat format) {
        return switch (Objects.requireNonNull(format, "format can not be null")) {
            case number -> ColumnAlign.right;
            case boolean_ -> ColumnAlign.center;
            case text, date -> ColumnAlign.left;
        };
    }

    /**
     * 单元格格式化方式
     */
    public enum ColumnFormat implements DescriptorUseableShortComment {
        text("纯文本"), number("数值"), date("日期"), boolean_("布尔");
        public final String label;

        ColumnFormat(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    /**
     * 文本对齐方式
     */
    public enum ColumnAlign implements DescriptorUseableShortComment, IEndTypeGetter {
        left("左对齐", EndType.AlignLeft), center("居中", EndType.AlignCenter), right("右对齐", EndType.AlignRight);
        public final String label;
        private final EndType endType;

        ColumnAlign(String label, EndType endType) {
            this.label = label;
            this.endType = endType;
        }

        @Override
        public String shortComment() {
            return this.label;
        }

        @Override
        public EndType getEndType() {
            return endType;
        }
    }

    @TISExtension
    public static class DftDescriptor extends IPluginStore.MultiDescribleElementDescriptor<WidgetColumnConfig> {
        @Override
        public String getDisplayName() {
            return "Column";
        }

        @Override
        public String shortComment() {
            return "设置显示列配置";
        }

        @Override
        public boolean isEnumableSet() {
            return true;
        }

        /**
         * 候选列 = 宿主所绑定对象集变量的全部 Ontology 属性。
         *
         * <p>与 {@code TestChild.DftDescriptor} 保持同一形状：{@code Pair.of(Option, 元素)}，
         * 其中 {@code Option.setChecked()} 决定既有列在「可枚举集合」选择器里是否预先勾选。
         */
        @Override
        protected List<Pair<Option, IPluginStore.MultiDescribleElement>> getEnumableCandidateSet(ParseDescribable<?> describable) {
            List<Pair<Option, IPluginStore.MultiDescribleElement>> target = Lists.newArrayList();
            Object hostInstance = describable.getInstance();
            if (!(hostInstance instanceof IWidgetColumnHost host)) {
                throw new IllegalStateException("host plugin "
                        + (hostInstance == null ? "null" : hostInstance.getClass().getName())
                        + " must implement " + IWidgetColumnHost.class.getName());
            }
            List<WidgetColumnConfig> currentCols = host.getColumnConfigs();
            Set<String> selected = currentCols == null
                    ? Collections.<String>emptySet()
                    : currentCols.stream().map(WidgetColumnConfig::identityValue).collect(Collectors.toSet());
            // getObjectPropertyMetas 对 FunctionConfig 型对象集会抛出可执行错误，此处不吞
            for (WidgetColumnConfig cfg : WidgetColumnConfig.createFrom(
                    WidgetOptionHelper.getObjectPropertyMetas(host.getBoundObjectSetVar()))) {
                target.add(Pair.of(new Option(cfg.identityValue()).setChecked(selected.contains(cfg.identityValue())), cfg));
            }
            return target;
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少有一列被标记为 clickable：点击该列可打开该条记录的编辑对话框。
            // 列出的键必须都是 WidgetColumnConfig 上真实存在的 @FormField。
            // width 刻意不列：它「仅 ObjectTable 使用」，对 cardFields / properties 是无意义的一列。
            return Lists.newArrayList(new ColConfig("prop").setClickable(), new ColConfig("label"),
                    new ColConfig("format", 12), new ColConfig("align", 12), new ColConfig("sortable", 12));
        }
    }
}
