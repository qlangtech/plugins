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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.lang.model.SourceVersion;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Action 参数池中的一行参数定义，供 {@code MULTI_DESCRIBLE_PLUGIN} 子表单渲染。
 *
 * <p>替代原先 {@link ActionParameters#parameters} 里让用户手写 JSON 数组字符串的做法
 * （违反工程 CLAUDE.md 原则一）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/11
 * @see ActionParameters
 * @see com.qlangtech.tis.plugin.ontology.impl.action.rule.PropertyMappingConfig
 */
@SuppressWarnings("all")
public class ActionParameterConfig implements Describable<ActionParameterConfig>,
        IPluginStore.MultiDescribleElement {

    static final String KEY_ONTOLOGY_ACTION_PARAMS = "params";
    /**
     * 参数唯一标识。规则里的映射行以该名字引用参数，同时它也是本行的记录键，
     * 并直接用作 Action 扁平表单的字段名（见 {@code ActionFormDescriptorBuilder}）。
     *
     * <p>因此这里用 {@code identity_strict}（不允许连字符）而非 {@code identity}：
     * 连字符不是合法 Java 字段名，会让生成的 groovy 编译失败。唯一性要求仍由
     * 「一行一个键」的表格语义保证。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity_strict})
    public String parameterId;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String displayName;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer order;
    /**
     * 数据类型配置（多态字段）。
     *
     * <p>可选子类：
     * <ul>
     *   <li>{@link PrimitiveDataType} - 基本类型（STRING/INTEGER/DATE 等）
     *   <li>{@link ObjectReferenceDataType} - 对象引用类型
     * </ul>
     *
     * <p>采用多态设计替代原先的枚举 + 条件字段耦合，消除字段间的条件依赖。
     */
    @FormField(ordinal = 3, validate = {Validator.require})
    public ActionParameterDataType dataType;

    /**
     * Groovy 在 Java 关键字之外额外保留的词，parameterId 撞上这些同样不能作字段名。
     */
    private static final Set<String> GROOVY_RESERVED = Set.of("as", "def", "in", "trait");

    /**
     * 该参数在生成的 groovy 里用作字段名。
     *
     * <p>{@code parameterId} 已由 {@code identity_strict} 约束成 {@code [A-Z\da-z_]+} 且不以数字开头，
     * 唯一剩余的非法形态是 Java/Groovy 保留字（如 {@code class}、{@code int}、{@code def}），这里兜底拦截。
     */
    public String requireFieldName() {
        if (StringUtils.isBlank(this.parameterId)) {
            throw new IllegalArgumentException("parameterId 不能为空，无法作为表单字段名");
        }
        if (SourceVersion.isKeyword(this.parameterId) || GROOVY_RESERVED.contains(this.parameterId)) {
            throw new IllegalArgumentException(
                    "parameterId '" + this.parameterId + "' 是 Java/Groovy 保留字，不能用作表单字段名");
        }
        return this.parameterId;
    }

    /**
     * 该参数在最终 Action 表单里用什么控件收集；对象引用（{@link ObjectReferenceDataType}）无 widget，
     * MVP 退化为 {@link FormFieldType#INPUTTEXT}，真实对象选择器留作后续。
     */
    public FormFieldType formFieldType() {
        if (dataType instanceof PrimitiveDataType primitive && primitive.widgetType != null) {
            return primitive.widgetType.getFormFieldType();
        }
        return FormFieldType.INPUTTEXT;
    }

    /**
     * {@link #formFieldType()} → groovy 字段的 Java 类型。
     */
    public String javaType() {
        return switch (formFieldType()) {
            case INT_NUMBER -> "Integer";
            case DECIMAL_NUMBER -> "Double";
            case MULTI_SELECTABLE -> "List<String>";
            case DateTime, DATE -> java.util.Date.class.getName();
            case DURATION_OF_SECOND, DURATION_OF_MINUTE, DURATION_OF_HOUR -> Duration.class.getName();
            case MEMORY_SIZE_OF_BYTE, MEMORY_SIZE_OF_KIBI, MEMORY_SIZE_OF_MEGA -> "Long";
            default -> "String";
        };
    }

    /**
     * 该参数是否必填；对象引用类型无 {@code required} 概念，返回 false。
     */
    public boolean required() {
        return dataType instanceof PrimitiveDataType primitive && Boolean.TRUE.equals(primitive.required);
    }

    /**
     * 显示名，缺省退化为 {@link #parameterId}。
     */
    public String label() {
        return StringUtils.defaultIfBlank(this.displayName, this.parameterId);
    }

    /**
     * 帮助文案，仅基本类型（{@link PrimitiveDataType}）有。
     */
    public String help() {
        return dataType instanceof PrimitiveDataType primitive ? primitive.help : null;
    }

//    /**
//     * 默认值：取 widget 的 {@code defaultValue} 并做 Date 归一化，无 widget 或无值返回 null。
//     */
//    public Object dftVal() {
//        if (dataType instanceof PrimitiveDataType primitive && primitive.widgetType != null) {
//            return normalizeDftVal(readPublicField(primitive.widgetType, "defaultValue"));
//        }
//        return null;
//    }

    /**
     * 把本参数的 extra-props 追加到 {@code fieldJson}。
     *
     * <p>{@code label}/{@code help}/{@code dftVal} 这些宿主级属性由本类负责；控件专属属性
     * （如下拉控件的 {@code enum}）委托给 {@code UserInputWidgetType#appendExtraPropsJson} 追加。
     * 这么拆是因为「enum 候选表」只有带 {@code options} 的少数控件才有，不该让每个参数都走一遍探测。
     */
    public JSONObject createExtraPropsJson() {
        JSONObject fieldJson = new JSONObject();
        if (StringUtils.isNotEmpty(label())) {
            fieldJson.put(Option.KEY_LABEL, label());
        }
        if (StringUtils.isNotEmpty(help())) {
            fieldJson.put(Option.KEY_HELP, help());
        }
        // Object defaultVal = dftVal();
//        if (defaultVal != null) {
//            fieldJson.put(com.qlangtech.tis.extension.util.PluginExtraProps.KEY_DFTVAL_PROP, defaultVal);
//        }
        if (dataType instanceof PrimitiveDataType primitive && primitive.widgetType != null) {
            primitive.widgetType.setExtraPropsJson(fieldJson);
        }
        return fieldJson;
    }

    /**
     * 反射读取 widget 上的 public 字段（{@code defaultValue}），不存在返回 null。
     */
    private static Object readPublicField(Object target, String fieldName) {
        try {
            Field field = target.getClass().getField(fieldName);
            return field.get(target);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return null;
        }
    }

    /**
     * Date 默认值序列化成 ISO 字符串，避免 fastjson 对 Date 的默认形态不定。
     */
    private static Object normalizeDftVal(Object dftVal) {
        if (dftVal instanceof Date date) {
            return DateTimeFormatter.ISO_DATE_TIME.format(date.toInstant().atZone(ZoneOffset.UTC));
        }
        return dftVal;
    }

//    /**
//     * 参数约束配置，决定在表单预览中如何渲染。
//     *
//     * <p>多态字段，可选值：
//     * <ul>
//     *   <li>{@link UserInputConstraint} - 文本输入框</li>
//     *   <li>{@link MultipleChoiceConstraint} - 下拉选项</li>
//     *   <li>{@link DatePickerConstraint} - 日期选择器</li>
//     * </ul>
//     */
//    @FormField(ordinal = 3)
//    public ActionParameterConstraints constraints;

    /**
     * 参数是否在表单中隐藏（用于 Environment variable 场景，如 Current object）。
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM)
    public Boolean hidden = false;


//    /**
//     * 持久化一行参数配置。
//     *
//     * <p>落盘路径：{@code <cfgRoot>/ontology/<domain>/action-type/params/<parameterId>/<pluginIdVal>.xml}。
//     * 参数名只作为目录名，文件名取自 {@code meta.getPluginIdVal()}（详情页链路里就是 Action 名）——
//     * 这样同一 domain 下不同 Action 的同名参数互不覆盖。调用方必须传入一个
//     * {@code pluginIdVal} 非空的 meta，且读写两条链路共用同一个 meta 实例。
//     *
//     * @param pluginContext 由调用方（httpProcess）显式传入，不要依赖线程上下文
//     */
//    public static void save(IPluginContext pluginContext, OntologyPluginMeta meta, ActionParameterConfig paramConfig) {
//        getOntologyActionTypeParamStore(paramConfig.identityValue(), meta).setPlugins(pluginContext
//                , Optional.empty(), Collections.singletonList(new Descriptor.ParseDescribable(paramConfig)));
//    }


//    /**
//     * @param param 参数名，允许带 {@code .xml} 后缀，这里会剥掉
//     * @see #save(IPluginContext, OntologyPluginMeta, ActionParameterConfig)
//     */
//    public static IPluginStore<?> getOntologyActionTypeParamStore(String param, OntologyPluginMeta pluginMeta) {
//        // 刻意不改写 pluginMeta 的 pluginIdVal：读写链路共用调用方传入的同一个 meta 实例，
//        // 在这里改写会让「文件名」随调用位置而变。
//        IPluginStore<?> paramStore
//                = ACTION_PARAM_STORE_GETTER.getPluginStore(pluginMeta
//                , Optional.of(KEY_ONTOLOGY_ACTION_PARAMS + File.separator + StringUtils.removeEnd(param, XmlFile.KEY_XML_DOT_EXTENSION)));
//        return paramStore;
//    }

    @Override
    public String identityValue() {
        return this.parameterId;
    }

    @TISExtension
    public static class DftDescriptor extends IPluginStore.MultiDescribleElementDescriptor<ActionParameterConfig> {

        @Override
        public String getDisplayName() {
            return "Parameter";
        }

        @Override
        public String shortComment() {
            return "Action 的输入参数";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        protected RowDragDropCfg isEnableRowDragDrop() {
            return new RowDragDropCfg(true, "order");
        }

        @Override
        public EnumableSetCfg getEnumableSetCfg() {
            return new EnumableSetCfg(true);
        }

        @Override
        protected List<Pair<Option, IPluginStore.MultiDescribleElement>> getEnumableCandidateSet(ParseDescribable<?> describable) {
            List<Pair<Option, IPluginStore.MultiDescribleElement>> target = Lists.newArrayList();
            OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
            OntologyAction action = Ontology.loadActionDetail(pluginMeta.getDomain(), pluginMeta.getPluginIdVal());
            if (!(action instanceof DefaultOntologyAction ontologyAction)) {
                throw new IllegalStateException("action must be type:" + DefaultOntologyAction.class);
            }
            // 因为 EnumableSetCfg(true, 1) 设置了跳过 1 层，所以这里拿到的是 ObjectActionType
            ActionType actionType = ontologyAction.getActionType();

            List<Pair<Option, ActionParameterConfig>> enumsCandidateSet = actionType.getEnumableCandidateSet(describable.getInstance());

            return enumsCandidateSet.stream()
                    .map((p) -> Pair.of(p.getLeft(), (IPluginStore.MultiDescribleElement) p.getRight())).toList();
            // ObjectActionType actionType = describable.getInstance();
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少有一列被标记为 clickable，否则 getExtractProps 抛异常
            // （IPluginStore.MultiDescribleElementDescriptor#getExtractProps）。
            // 列出的键必须都是本类上真实存在的 @FormField。
            return Lists.newArrayList( //
                    new ColConfig("parameterId", 15).setClickable()
                    , new ColConfig("displayName", 15)
                    // , new ColConfig("order", 5)
                    , new ColConfig("dataType"));
        }

        @Override
        public int maxHeight() {
            return 300;
        }
    }
}
