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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.extension.impl.EnumFieldMode;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.UserInputWidgetType;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.WidgetOptionConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DateTimeWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DateWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.DecimalNumberWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.EnumWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.FileWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.InputTextWidget;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.IntNumberWidget;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.ValueConstraint;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4Integer;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.OneOfValueEnum;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Range4Decimal;
import com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Range4Integer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 把一条本体属性映射成 Action 参数配置的默认值约定，全部收在这一个类里。
 *
 * <p>被 {@code ObjectActionType#getEnumableCandidateSet} 用来给「把该属性加成 Action 参数」这一动作生成初始配置，
 * 使生成的参数开箱即用（数据类型、控件、控件选项、必填态都对），用户只需微调。
 *
 * <p>两个层级的映射，前一层命中就用前一层：
 * <ol>
 *     <li>属性引用的值类型上的值约束（{@link ValueConstraint}）—— 约束是比本体类型更具体的信息，例如同样一个
 *     String 属性，限定了枚举取值的应该用下拉控件并要求带上候选项；</li>
 *     <li>本体类型（{@link OntologyType}）—— 没有值类型引用、或约束不改变控件形态时的默认控件。</li>
 * </ol>
 *
 * <p>三条降级路径都刻意「不抛异常、也不留空」：值类型名称为空、名字在本域内查不到（值类型被删或跨域引用）、
 * 枚举约束没有任何取值时，都退回按本体类型取默认控件。原因是本方法的调用方在构建候选集，抛异常会让整个 Action
 * 详情页打不开（同 {@code DefaultOntologyAction.collectRuleRefs} 的既有原则）；而 {@code widgetType} 带
 * {@code Validator.require}，留空则等于让每一个生成的参数都卡在表单校验上。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/16
 * @see com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType#getEnumableCandidateSet(ActionParameters)
 */
public class ActionParameterMapper {

    private ActionParameterMapper() {
    }

    /**
     * 按名字加载本体域下全部值类型。
     *
     * <p>值类型与属性是分开存储的、属性上只有值类型的名字，所以查一次表是必要的；而一次候选集构建会遍历对象类型的
     * 全部属性，因此这张表要在循环外加载一次、循环内复用，不要逐属性去查 store。
     *
     * @see Ontology#loadAllValueTypes(String)
     */
    public static Map<String, OntologyValueType> loadValueTypes(String ontologyName) {
        Map<String, OntologyValueType> result = Maps.newHashMap();
        for (OntologyValueType valType : Ontology.loadAllValueTypes(ontologyName)) {
            result.put(valType.identityValue(), valType);
        }
        return result;
    }

    /**
     * 把一条本体属性映射成 Action 参数配置。
     *
     * @param col        对象类型上的一条属性
     * @param valueTypes {@link #loadValueTypes(String)} 的结果，可为 null
     */
    public static ActionParameterConfig create(OntologyProperty col,
                                               Map<String, OntologyValueType> valueTypes) {
        Objects.requireNonNull(col, "col can not be null");
        // 本体类型是属性的核心身份，解析失败说明属性本身坏了，保持抛（改造前这里也在解析）
        OntologyType ontologyType = col.parseOntologyType();

        ActionParameterConfig config = new ActionParameterConfig();
        // 属性没有独立的 displayName，name 兼任；description 在 ActionParameterConfig 上没有落点
        config.parameterId = col.getName();
        config.displayName = col.getName();
        config.hidden = Boolean.FALSE;

        PrimitiveDataType dataType = new PrimitiveDataType();
        // nullable 是「值可以为空」，正好是 required 的反面；未设置时按必填处理
        dataType.required = !Boolean.TRUE.equals(col.isNullable());
        dataType.widgetType = resolveWidget(ontologyType, constraintOf(col, valueTypes));
        config.dataType = dataType;
        return config;
    }

    /**
     * 取属性所引用的值类型上的那一个值约束。
     *
     * <p>属性侧只有值类型的<b>名字</b>（{@code BasicOntologyPropertyTypeRef.valueType}），所以这里要走一次查表：
     * 值类型 → 约束步骤 → 那唯一一个 {@link ValueConstraint}（{@link ConstraintsOfValueType} 只持有单个约束）。
     */
    private static Optional<ValueConstraint> constraintOf(OntologyProperty col,
                                                          Map<String, OntologyValueType> valueTypes) {
        Optional<String> valueTypeRef = col.getPropertyTypeRef().getValueTypeRef();
        // 表单里枚举未选时会交出 Optional.of("")，空串一律当「没引用值类型」处理
        if (valueTypeRef.isEmpty() || StringUtils.isBlank(valueTypeRef.get())) {
            return Optional.empty();
        }
        OntologyValueType valType = (valueTypes == null) ? null : valueTypes.get(valueTypeRef.get());
        if (!(valType instanceof DefaultOntologyValueType defaultValType)) {
            // 值类型被删或跨域引用：降级，不抛
            return Optional.empty();
        }
        ConstraintsOfValueType constraintsStep = defaultValType.getConstraintsStep();
        if (constraintsStep == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(constraintsStep.constraint);
    }

    /**
     * 第一层（值约束）优先，落到第二层（本体类型）兜底。
     *
     * @param constraint 为 empty 表示该属性没有引用值类型、或引用的值类型上没配约束
     */
    static UserInputWidgetType resolveWidget(OntologyType ontologyType, Optional<ValueConstraint> constraint) {
        if (constraint.isPresent()) {
            UserInputWidgetType fromConstraint = widgetOfConstraint(constraint.get());
            if (fromConstraint != null) {
                return fromConstraint;
            }
        }
        return defaultWidget(ontologyType);
    }

    /**
     * 值约束 → 控件。返回 null 表示该约束不改变控件形态（或暂时无处安放），交给
     * {@link #defaultWidget(OntologyType)} 兜底。
     *
     * <p>目前没能落地的约束内容有：{@code Regex.pattern}（{@code InputTextWidget.validators} 装的是
     * {@code Validator} 枚举，塞不下正则）、{@code Range4String} 的 {@code min}/{@code max}（它们是<b>长度</b>
     * 上下界，而 {@code InputTextWidget} 没有长度字段）、{@code Range4TimeStamp} 的时间区间
     * （{@code DateTimeWidget} 只有 defaultValue/displayFormat/showTime）。这三种只保证类型与控件正确，
     * 约束内容留给用户手工补 —— 与其把它们塞进语义相悖的控件字段，不如在界面上显式缺着。
     */
    private static UserInputWidgetType widgetOfConstraint(ValueConstraint constraint) {
        if (constraint instanceof Enum4String enumOfString) {
            return enumWidget(enumOfString.enumVals);
        } else if (constraint instanceof Enum4Integer enumOfInteger) {
            return enumWidget(enumOfInteger.enumVals);
        } else if (constraint instanceof Range4Integer intRange) {
            IntNumberWidget widget = new IntNumberWidget();
            widget.min = intRange.min;
            widget.max = intRange.max;
            return widget;
        } else if (constraint instanceof Range4Decimal decimalRange) {
            DecimalNumberWidget widget = new DecimalNumberWidget();
            // 约束侧是 Float，控件侧是 Double
            widget.min = toDouble(decimalRange.min);
            widget.max = toDouble(decimalRange.max);
            return widget;
        }
        return null;
    }

    /**
     * 枚举约束 → 带候选项的 {@link EnumWidget}。
     *
     * <p>没有可用候选项时返回 null 让调用方回落：{@code EnumWidget.options} 带 {@code Validator.require}，
     * 产一个没有任何候选项的下拉等于产一个提交不了的表单。这里有两种「没有可用候选项」——{@code enumVals}
     * 为空/为 null，以及它非空但每一条的 {@code enumVal} 都是空白（{@code WidgetOptionConfig.val} 同样带
     * {@code Validator.require}，逐条搬过去就是逐条校验失败）。
     */
    private static UserInputWidgetType enumWidget(List<OneOfValueEnum> enumVals) {
        if (CollectionUtils.isEmpty(enumVals)) {
            return null;
        }
        List<WidgetOptionConfig> options = Lists.newArrayList();
        for (OneOfValueEnum oneOf : enumVals) {
            if (StringUtils.isBlank(oneOf.getEnumVal())) {
                continue;
            }
            WidgetOptionConfig option = new WidgetOptionConfig();
            // OneOfValueEnum 只有值没有显示文本，label 取同一个值（留空则前端自行回落显示 val）
            option.val = oneOf.getEnumVal();
            option.label = oneOf.getEnumVal();
            options.add(option);
        }
        if (options.isEmpty()) {
            return null;
        }
        EnumWidget widget = new EnumWidget();
        widget.options = options;
        // showStyle 带 Validator.require，不设会卡在表单校验上
        widget.showStyle = EnumFieldMode.DEFAULT;
        return widget;
    }

    /**
     * 第二层：本体类型 → 默认控件。
     *
     * <p>这里是 switch 表达式且**刻意不写 default**：{@link OntologyType} 新增常量时编译期就会失败，
     * 逼着人来补映射，而不是静默地让新类型掉进某个兜底分支。
     *
     * @see OntologyType
     */
    private static UserInputWidgetType defaultWidget(OntologyType ontologyType) {
        return switch (ontologyType) {
            // BOOLEAN 无布尔控件；VECTOR / GEOPOINT / GEOSHAPE 无任何控件，一律回落单行文本
            case STRING, BOOLEAN, VECTOR, GEOPOINT, GEOSHAPE, ARRAY, STRUCT -> new InputTextWidget();
            case INTEGER, SHORT, LONG -> new IntNumberWidget();
            case FLOAT, DOUBLE, DECIMAL -> new DecimalNumberWidget();
            case DATE -> new DateWidget();
            case TIMESTAMP -> new DateTimeWidget();
            case BYTE, MEDIA_REFERENCE -> new FileWidget();
//            case ARRAY -> new MultiSelectableWidget();
//            case STRUCT -> new MultiDescriblePluginWidget();
        };
    }

    private static Double toDouble(Float val) {
        return val == null ? null : val.doubleValue();
    }
}
