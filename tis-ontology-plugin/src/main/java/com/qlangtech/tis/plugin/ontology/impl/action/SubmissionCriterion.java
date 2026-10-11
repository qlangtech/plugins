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
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * 提交条件中的一行，供 {@code MULTI_DESCRIBLE_PLUGIN} 子表单渲染。
 *
 * <p>多条条件之间是「且(AND)」关系；整个列表为空表示不限制提交。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/11
 * @see ActionSubmissionCriteria
 */
public class SubmissionCriterion implements Describable<SubmissionCriterion>,
        IPluginStore.MultiDescribleElement {

    public static final String KEY_LABEL = "label";
    public static final String KEY_SUBJECT_KIND = "subjectKind";
    public static final String KEY_SUBJECT_REF = "subjectRef";
    public static final String KEY_OPERATOR = "operator";
    public static final String KEY_VALUE = "value";

    /**
     * 条件名，同时是本行的记录键（identity）。
     *
     * <p>为什么必须另设一个 identity 字段而不是拿 {@link #subjectRef} 顶替：前端以 identity
     * 字段的<b>值</b>作 {@code descVal.vals} 的 key（tis-console 的
     * {@code tis.plugin.ts} 里 {@code descVal.vals[pk.primary] = item}），同值即<b>静默覆盖</b>。
     * 若拿 subjectRef 当 identity，「金额下限 amount&gt;0」与「金额上限 amount&lt;100」
     * 这类同字段多条件会被合并成一行。
     *
     * <p>刻意不加 {@code Validator.identity}：该校验器只允许 {@code [A-Z\da-z_\-]+}，
     * 会拒绝「金额下限」这种天然是中文的条件名。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String label;

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public SubjectKind subjectKind = SubjectKind.OBJECT_PROPERTY;

    /**
     * 左侧字段；候选 = Action 参数池 ∪ 目标对象类型属性，见 {@link #availableSubjects()}。
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String subjectRef;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public ComparisonOperator operator = ComparisonOperator.EQUALS;

    /**
     * 比较值；{@link ComparisonOperator#isNullary()} 为真的操作符留空。
     */
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {})
    public String value;

    @Override
    public String identityValue() {
        return this.label;
    }

    public enum SubjectKind {
        OBJECT_PROPERTY, PARAMETER
    }

    /**
     * 比较操作符。八项取自被取代的 {@code ActionRules.json} 里 submissionCriteria 的 help 文本，
     * 一项不多一项不少，中文标签写在 {@code SubmissionCriterion.json} 的 enum 数组里。
     */
    public enum ComparisonOperator {
        EQUALS, NOT_EQUALS, IN, NOT_IN, GREATER_THAN, LESS_THAN, IS_NULL, IS_NOT_NULL;

        /**
         * 一元操作符：不需要比较值。
         */
        public boolean isNullary() {
            return this == IS_NULL || this == IS_NOT_NULL;
        }
    }

    /**
     * 左侧字段的候选池：Action 参数池 ∪ 目标对象类型的属性。
     *
     * <p>由 {@code SubmissionCriterion.json} 的 {@code enum} 脚本调用，带 {@code :uncache_true}
     * 后缀，因此每次渲染表单都会重新求值（{@code PropertyType#getEnumPropOptions}）。
     *
     * <p>为什么用 ENUM + json 脚本而不是 SELECTABLE：SELECTABLE 要求 Callable 返回
     * {@code List<? extends IdentityName>}，框架会 {@code new SelectOption(r.identityValue(), ...)}，
     * <b>丢掉 Option 的 label 与分组 comment</b>；而 ENUM 脚本路径保留 {@link OptionWithEndType}
     * 的分组角标。
     *
     * <p>两条渲染路径下都能拿到完整的候选集：向导首次渲染第 4 步时，第 3 步的
     * {@code processPreSaved} 已顺手把第 2 步也发布了（见 {@link ActionParameters#processPreSaved}）；
     * 编辑已保存 Action 时走 {@code getInstancePropsJson()}，所有步骤依次发布自己。
     *
     * @see com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference#getObjectTypeOpts()
     */
    public static List<OptionWithEndType> availableSubjects() {
        List<OptionWithEndType> result = Lists.newArrayList();

        // ① Action 参数池
        ActionParameters params = OneStepOfMultiSteps.getPreviousStepInstance(ActionParameters.class, false);
        if (params != null) {
            for (ActionParameterConfig p : params.getParameters()) {
                result.add(new OptionWithEndType(p.displayName, p.parameterId,
                        IEndTypeGetter.EndType.OntologyObjectType)
                        .setComment(new Option.OptionComment(Option.OptionCommentColor.Blue, "Action 参数")));
            }
        }

        // ② 目标对象类型的属性 —— 取自第 2 步 ObjectActionType.objectTypeReference
        try {
            ActionRuleSetter setter = OneStepOfMultiSteps.getPreviousStepInstance(ActionRuleSetter.class, false);
            String objTypeName = setter == null ? null : setter.getActionTypes().stream()
                    .filter(ObjectActionType.class::isInstance)
                    .map(ObjectActionType.class::cast)
                    .map((t) -> t.objectTypeReference)
                    .filter(StringUtils::isNotEmpty)
                    .findFirst().orElse(null);
            if (StringUtils.isNotEmpty(objTypeName)) {
                OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta(
                        IPluginContext.getThreadLocalInstance().getContext());
                OntologyObjectType objType = Ontology.loadObjectTypeDetail(meta.getDomain(), objTypeName);
                if (objType != null) {
                    for (OptionWithEndType col : objType.getColOpts()) {
                        result.add(col.setComment(
                                new Option.OptionComment(Option.OptionCommentColor.Geekblue, "对象属性")));
                    }
                }
            }
        } catch (Exception e) {
            // 渲染期降级：本体域/对象类型取不到时只丢这一组候选，不能让整张表单打不开。
            // 与 LinkReference#getObjectTypeOpts 的宽 catch 是同一取舍。
        }
        return result;
    }

    @TISExtension
    public static class DftDescriptor extends IPluginStore.MultiDescribleElementDescriptor<SubmissionCriterion> {

        @Override
        public String getDisplayName() {
            return "Criterion";
        }

        @Override
        public String shortComment() {
            return "提交条件（多条之间为「且」关系）";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // Table 视图要求至少一列 clickable，否则 getExtractProps 抛异常。
            // subjectKind 不列成列：候选池已把两类来源合并并打了分组角标，单列信息量低。
            return Lists.newArrayList(
                    new ColConfig(KEY_LABEL).setClickable(),
                    new ColConfig(KEY_SUBJECT_REF),
                    new ColConfig(KEY_OPERATOR),
                    new ColConfig(KEY_VALUE));
        }
    }
}
