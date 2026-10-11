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

package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;

import java.util.List;
import java.util.Map;

/**
 * Ontology Action Rule 插件接口
 * <p>
 * 定义 Action 的执行规则，包括：
 * - Ontology Rules: CREATE_OBJECT, MODIFY_OBJECT, DELETE_OBJECT, CREATE_LINK, DELETE_LINK
 * - Function Rules: FUNCTION_BACKED
 * - Side Effect Rules: NOTIFICATION, WEBHOOK, WRITEBACK_WEBHOOK
 * - Advanced Rules: SCHEDULE, APPLY_SCENARIO
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public abstract class OntologyActionRule implements Describable<OntologyActionRule> {

    public static List<BaseRuleDescriptor> descFilter(List<BaseRuleDescriptor> descs, String ruleType) {
        final RuleGroup ruleGroup = RuleGroup.valueOf(ruleType);
        return descs.stream().filter((desc) -> {
            return ruleGroup == desc.getRuleGroup();
        }).toList();
    }

    /**
     * 获取规则类型
     */
    public abstract RuleType getRuleType();

    /**
     * 获取唯一标识值
     */
    public abstract String identityValue();

    /**
     * 是否启用该规则
     */
    public boolean isEnabled() {
        return true;
    }

    /**
     * 执行规则
     *
     * @param context 执行上下文
     * @return 执行结果
     */
    public abstract RuleExecutionResult execute(RuleExecutionContext context) throws Exception;

    /**
     * 验证规则配置是否有效
     *
     * @return 验证通过返回 true
     */
    public boolean validate() {
        return true;
    }

    /**
     * 把结构化的映射行解析成「目标字段 → 取值」。
     *
     * <p>原先 {@link CreateObjectRule}、{@link ModifyObjectRule}、{@link FunctionBackedRule}
     * 各有一份逐字重复的 parse + build 两段实现，这里收敛成一处。
     *
     * @param mappings   映射行，可为 null（函数可以无入参）
     * @param parameters Action 参数池的运行时取值
     * @return 目标字段到取值的映射；{@code FROM_PARAMETER} 指向的参数不存在时该行被跳过
     */
    protected Map<String, Object> buildMappedValues(List<PropertyMappingConfig> mappings,
                                                    Map<String, Object> parameters) {
        Map<String, Object> result = new java.util.HashMap<>();
        if (mappings == null) {
            return result;
        }
        for (PropertyMappingConfig m : mappings) {
            switch (m.source) {
                case FROM_PARAMETER -> {
                    Object paramValue = parameters.get(m.parameterRef);
                    if (paramValue != null) {
                        result.put(m.target, paramValue);
                    }
                }
                case CURRENT_TIME -> result.put(m.target, System.currentTimeMillis());
                case LITERAL -> result.put(m.target, m.literalValue);
            }
        }
        return result;
    }

    /**
     * 规则基础 Descriptor
     */
    public static abstract class BaseRuleDescriptor extends Descriptor<OntologyActionRule> implements DescriptorUseableShortComment {

        /**
         * 获取规则类型
         */
        public abstract RuleType getRuleType();

        /**
         * 获取规则分组
         */
        public RuleGroup getRuleGroup() {
            return getRuleType().getGroup();
        }

        @Override
        public final String getDisplayName() {
            return getRuleType().name();
        }

        @Override
        public final String shortComment() {
            return getRuleType().getDescription();
        }
    }
}