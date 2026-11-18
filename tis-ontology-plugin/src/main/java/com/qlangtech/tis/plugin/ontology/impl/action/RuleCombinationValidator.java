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

import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.FunctionActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 规则组合合法性校验。
 *
 * <p>来源：Palantir <i>Action types</i> 文档的 "Invalid combinations" 一节 ——
 * 一个 action type 可以组合多条 Ontology 规则，后端会把规则编译成「每个对象一次编辑」，
 * 因此以下组合不被支持：
 * <ol>
 *   <li>Objects cannot be deleted before they are added or modified.</li>
 *   <li>Objects cannot be modified before they are added.</li>
 *   <li>Objects cannot be created twice in one form submission.</li>
 * </ol>
 * 另外函数规则是唯一例外：存在时不允许配置其它任何规则 ——
 * "When this rule is present, no other rule may be configured since function code alone is
 * capable of handling everything that other rules can do."
 *
 * <p><b>粒度说明</b>：Palantir 的约束是「同一个对象」，而当前实现里规则的定位参数
 * （{@code ridParameterName} / 属性映射）在配置期不可判定指向哪个实例，因此只能退化为
 * 「同一个对象类型」这一级。为避免误伤正常配置，顺序类检查<b>只在同一对象类型上同时存在
 * 两类操作时</b>才比较先后 —— 单独一条「删除对象」或「修改对象」动作永远合法。
 *
 * <p>接口规则（{@code InterfaceActionType}）不参与顺序检查：它们以接口为分组键，
 * 与对象类型不是同一坐标系，混比会产生误报。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/17
 * @see ActionType
 * @see RuleType
 */
public final class RuleCombinationValidator {

    private RuleCombinationValidator() {
    }

    /**
     * @param rules 按用户配置顺序排列的规则（列表位置即执行顺序）
     * @return 空表示合法；否则为一条可直接展示给用户的错误说明
     */
    public static Optional<String> validate(List<ActionType> rules) {
        if (rules == null || rules.isEmpty()) {
            return Optional.of("至少需要配置一条规则");
        }
        Optional<String> functionErr = validateFunctionRuleExclusive(rules);
        if (functionErr.isPresent()) {
            return functionErr;
        }
        return validateObjectOperationOrder(rules);
    }

    /**
     * 函数规则必须单独使用。
     */
    private static Optional<String> validateFunctionRuleExclusive(List<ActionType> rules) {
        if (rules.size() <= 1) {
            return Optional.empty();
        }
        return rules.stream()
                .filter(FunctionActionType.class::isInstance)
                .findFirst()
                .map((f) -> "函数规则必须单独使用：函数代码本身就能实现其它所有规则的能力，"
                        + "存在函数规则时不能再配置其它规则");
    }

    /**
     * 对象类型维度的顺序检查。
     *
     * <p>一趟扫描收集「每个对象类型上各操作的首个出现位置」，扫描结束后再比较 ——
     * 边扫边比会漏掉「删除在前、创建在后」这类违例（扫描到删除时创建位置还未知）。
     */
    private static Optional<String> validateObjectOperationOrder(List<ActionType> rules) {
        Map<String, OperationIndices> byObjectType = new LinkedHashMap<>();
        for (int i = 0; i < rules.size(); i++) {
            if (!(rules.get(i) instanceof ObjectActionType objectAction)) {
                continue;
            }
            OntologyActionRule rule = objectAction.getRule();
            if (rule == null) {
                continue;
            }
            byObjectType
                    .computeIfAbsent(objectAction.objectTypeReference, (k) -> new OperationIndices())
                    .record(rule.getRuleType(), i);
        }

        for (Map.Entry<String, OperationIndices> entry : byObjectType.entrySet()) {
            Optional<String> err = entry.getValue().validate(entry.getKey());
            if (err.isPresent()) {
                return err;
            }
        }
        return Optional.empty();
    }

    /**
     * 单个对象类型上各类操作的首个出现位置（规则在列表中的下标，从 0 起）。
     */
    private static final class OperationIndices {
        /**
         * 创建类操作：CREATE_OBJECT 与 CREATE_OR_MODIFY_OBJECT。
         * 后者在当前实现里没有独立规则类，但枚举值存在，按「创建」参与约束。
         */
        private int createIdx = -1;
        private int createCount = 0;
        private int modifyIdx = -1;
        private int deleteIdx = -1;

        void record(RuleType ruleType, int index) {
            switch (ruleType) {
                case CREATE_OBJECT, CREATE_OR_MODIFY_OBJECT -> {
                    if (createIdx < 0) {
                        createIdx = index;
                    }
                    createCount++;
                }
                case MODIFY_OBJECT -> {
                    if (modifyIdx < 0) {
                        modifyIdx = index;
                    }
                }
                case DELETE_OBJECT -> {
                    if (deleteIdx < 0) {
                        deleteIdx = index;
                    }
                }
                default -> {
                    // 其它规则类型（链接 / 接口 / 副作用）不参与对象维度的顺序约束
                }
            }
        }

        Optional<String> validate(String objectType) {
            if (createCount > 1) {
                return Optional.of("对象类型「" + objectType + "」不能在一次提交中创建两次");
            }
            if (createIdx >= 0 && modifyIdx >= 0 && modifyIdx < createIdx) {
                return Optional.of("对象类型「" + objectType + "」不能先修改后创建："
                        + "请把「创建对象」排在「修改对象」之前");
            }
            if (createIdx >= 0 && deleteIdx >= 0 && deleteIdx < createIdx) {
                return Optional.of("对象类型「" + objectType + "」不能先删除后创建："
                        + "请把「创建对象」排在「删除对象」之前");
            }
            if (modifyIdx >= 0 && deleteIdx >= 0 && deleteIdx < modifyIdx) {
                return Optional.of("对象类型「" + objectType + "」不能先删除后修改："
                        + "请把「修改对象」排在「删除对象」之前");
            }
            return Optional.empty();
        }
    }
}
