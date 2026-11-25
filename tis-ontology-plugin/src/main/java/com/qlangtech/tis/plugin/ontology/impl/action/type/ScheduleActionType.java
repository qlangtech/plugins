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
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;

import java.util.List;

/**
 * Schedule Action Type - 调度操作
 * <p>
 * 对应 SCHEDULE 规则：触发 Ontology 数据的重新构建或刷新
 * <p>
 * TODO: 待实现 ScheduleRule 后，添加 @FormField 字段引用
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 */
public class ScheduleActionType extends ActionType {

    @Override
    public RuleGroup getRuleGroup() {
        return RuleGroup.ADVANCED;
    }

    @Override
    public List<RuleType> getAllowedRuleTypes() {
        return List.of(RuleType.SCHEDULE);
    }

    @Override
    public OntologyActionRule getRule() {
        // TODO: 待实现 ScheduleRule 后返回具体规则
        return null;
    }

    /**
     * 尚无可配置字段、规则也未实现（{@code getRule()} 恒为 null），返回空列表占位；
     * ScheduleRule 落地后在此追加其规则 literia。
     */
    @Override
    public List<UDFDesc> getLiteria() {
        return Lists.newArrayList();
    }

    // @TISExtension
    public static class DefaultDescriptor extends BaseActionTypeDescriptor {

        @Override
        public RuleGroup getRuleGroup() {
            return RuleGroup.ADVANCED;
        }

        @Override
        public String getRuleTypeFilter() {
            return "SCHEDULE";
        }
//
//        @Override
//        public String getDisplayName() {
//            return "Schedule";
//        }
    }
}