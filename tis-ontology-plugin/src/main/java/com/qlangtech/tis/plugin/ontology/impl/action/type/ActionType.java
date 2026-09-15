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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;

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
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 * @see RuleType
 * @see RuleGroup
 */
public abstract class ActionType implements Describable<ActionType> {

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

    /**
     * ActionType 基础 Descriptor
     */
    public static abstract class BaseActionTypeDescriptor extends Descriptor<ActionType>
            implements DescriptorUseableShortComment {

        /**
         * 获取该 ActionType 所属的规则类别
         */
        public abstract RuleGroup getRuleGroup();

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
    }
}