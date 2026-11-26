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

package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.MultiDescribleElementLiteria;
import com.qlangtech.tis.plugin.MultiDescribleElementSetSelector;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameters;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * Action Type 插件基类
 * <p>
 * 代表 Ontology Action 的规则类别，每个具体子类对应一类规则操作：
 * - ObjectActionType: 对象操作（创建/修改/删除对象）
 * - LinkActionType: 链接操作（创建/删除链接）
 * - FunctionActionType: 函数操作
 * - InterfaceActionType: 接口操作
 * - SideEffectActionType: 副作用操作（通知/Webhook）
 * - ScheduleActionType: 数据构建
 * - ApplyScenarioActionType: 场景合并
 *
 * <p><b>一个 Action 可以组合多条规则</b>（Palantir: "Action types can include combinations of
 * Ontology rules."），因此本类按 {@code MULTI_DESCRIBLE_PLUGIN} 的元素类来建模：
 * 由 {@code ActionRuleSetter#actionTypes} 的列表渲染成一张表，<b>列表顺序即规则执行顺序</b>。
 * 唯一例外是函数规则 —— 存在时不允许配置任何其它规则（组合约束见
 * {@code RuleCombinationValidator}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 * @see RuleType
 * @see RuleGroup
 * @see ObjectActionType
 */
public abstract class ActionType
        implements Describable<ActionType>, IPluginStore.MultiDescribleElement, MultiDescribleElementLiteria {

    /**
     * 本行的标识字段名（同时是前端 {@code descVal.vals} 的 key）。
     */
    public static final String KEY_RULE_NAME = "ruleName";
    
    /**
     * 规则名称，本行的记录键（identity）。
     *
     * <p>为什么必须让用户手填而不是自动生成：前端以 identity 字段的<b>值</b>作
     * {@code descVal.vals} 的 key（tis-console 的 {@code tis.plugin.ts} 里
     * {@code descVal.vals[pk.primary] = item}），同值即<b>静默覆盖</b>；而自动生成
     * （构造期 {@code System.currentTimeMillis()}）在「服务端反序列化 / 前端新增行」两条链路
     * 上都拿不到稳定值 —— 新增行的初值只认 json 的 {@code dftVal}，所有行会拿到同一个值。
     *
     * <p>刻意不加 {@code Validator.identity}：该校验器只允许 {@code [A-Z\da-z_\-]+}，
     * 会拒绝「创建工单」这种天然是中文的规则名。
     *
     * <p>{@code ordinal = -1}：排在各子类自己的字段（0 起）之前，避免与子类 ordinal 撞区间。
     */
    @FormField(identity = true, ordinal = -1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String ruleName;

    public static List<BaseActionTypeDescriptor> descFilter(List<BaseActionTypeDescriptor> descs, String ruleType) {
        final RuleGroup ruleGroup = RuleGroup.valueOf(ruleType);
        return descs.stream().filter((desc) -> {
            return RuleGroup.OFF == desc.getRuleGroup() || ruleGroup == desc.getRuleGroup();
        }).toList();
    }

    /**
     * 按规则类型标识过滤 ActionType 描述符
     * 适用于同一组内有多个 ActionType 的场景（如 ADVANCED 组下的 SCHEDULE 和 APPLY_SCENARIO）
     */
    public static List<BaseActionTypeDescriptor> descFilterByRuleType(List<BaseActionTypeDescriptor> descs, String ruleTypeFilter) {
        return descs.stream().filter((desc) -> {
            return RuleGroup.OFF == desc.getRuleGroup() || ruleTypeFilter.equals(desc.getRuleTypeFilter());
        }).toList();
    }

    @Override
    public String identityValue() {
        return this.ruleName;
    }

    /**
     * 获取该 ActionType 所属的规则类别
     */
    public abstract RuleGroup getRuleGroup();

    /**
     * 获取该 ActionType 允许的 RuleType 列表
     * 用于在 UI 中筛选该类别下可选的规则实现
     */
    public abstract List<RuleType> getAllowedRuleTypes();

    /**
     * 获取配置的规则实例
     */
    public abstract OntologyActionRule getRule();


    /**
     * 验证规则配置是否有效
     */
    public boolean validate() {
        return true;
    }

    public List<Pair<Option, ActionParameterConfig>> getEnumableCandidateSet(ActionParameters actParams) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    /**
     * 带「本 Action 全部对象规则引用的对象类型」的候选集推导。
     *
     * <p>参数池是全 Action 共用的，而对象规则的参数按属性名生成。当同一个 Action 里的规则
     * 指向<b>不同的对象类型</b>时，两者会生成同名参数（例如都叫 {@code id}），在参数池里被
     * 合并成一行、执行时互相覆盖。默认实现原样委派，只有对象规则需要据此加命名空间前缀，
     * 见 {@code ObjectActionType#getEnumableCandidateSet(ActionParameters, List)}。
     *
     * @param objectTypeRefs 见 {@code ObjectActionType#objectTypeReferences(List)}
     */
    public List<Pair<Option, ActionParameterConfig>> getEnumableCandidateSet(ActionParameters actParams,
                                                                             List<String> objectTypeRefs) {
        return getEnumableCandidateSet(actParams);
    }

    /**
     * 执行用户提交的运行时表单。未实现的动作类别在调用时显式报错，
     * 与相邻的 {@link #getEnumableCandidateSet(ActionParameters)} 同风格。
     */
    public void executeActionRule(Describable<?> plugin) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    /**
     * 执行运行时表单，额外携带「删除已确认」标记。
     *
     * <p>该标记来自 Workshop 模态框的危险按钮，属于「用户在界面上确认过」的证明，
     * 无法放进运行时表单（表单字段由参数池推导，不含它）。默认忽略该标记，
     * 只有对象删除规则需要它，见
     * {@code ObjectActionType#executeActionRule(Describable, boolean)}。
     */
    public void executeActionRule(Describable<?> plugin, boolean deleteConfirmed) {
        executeActionRule(plugin);
    }

    /**
     * 执行运行时表单，额外携带本 Action 全部对象类型引用（用于还原参数命名空间）。
     *
     * <p>多对象类型 Action 的表单字段名做了 {@code <objectTypeReference>_} 前缀隔离，
     * 见 {@code ObjectActionType#namespacePrefix(String, List)}；不关心该上下文的规则类别
     * 原样委派即可。
     */
    public void executeActionRule(Describable<?> plugin, boolean deleteConfirmed, List<String> objectTypeRefs) {
        executeActionRule(plugin, deleteConfirmed);
    }

    /**
     * ActionType 基础 Descriptor
     *
     * <p>继承 {@link MultiDescribleElementSetSelector.MultiDescribleElementDescriptor} 而不是裸 {@link Descriptor}：
     * 作为 {@code MULTI_DESCRIBLE_PLUGIN} 的元素类，框架在表单初始化时会强制校验这一点
     * （见 {@code PropertyType#createFieldListElementClazz}），写错只会在运行期抛。
     */
    public static abstract class BaseActionTypeDescriptor
            extends MultiDescribleElementSetSelector.MultiDescribleElementDescriptor<ActionType> {

        /**
         * 获取该 ActionType 所属的规则类别
         */
        public abstract RuleGroup getRuleGroup();

        @Override
        public final String getDisplayName() {
            return getRuleGroup().getDisplayName();
        }

        /**
         * 获取该 ActionType 的过滤标识
         * 默认返回 RuleGroup 名称，子类可重写以提供更精确的过滤
         */
        public String getRuleTypeFilter() {
            return getRuleGroup().name();
        }

        @Override
        public final String shortComment() {
            return getRuleGroup().getDescription();
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        /**
         * 规则表格只列「规则名称」一列：其余字段都是各子类自己的多态载荷
         * （{@code objectRule} / {@code linkRule} / ...），{@code cellText} 读不到嵌套值，
         * 列出来只会是永远空白的列。列名必须是本类（含父类）上真实存在的
         * {@code @FormField} 字段名，并且至少一列 clickable，否则 {@code getExtractProps} 抛异常。
         */
        @Override
        public List<ColConfig> colsConfig() {
            return Lists.newArrayList(
                    new ColConfig(KEY_RULE_NAME, 30).setClickable()
                    , ColConfig.literiaDescCol());
        }

        @Override
        public int maxHeight() {
            return 800;
        }
    }
}
