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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 删除链接规则
 * <p>
 * 删除两个对象之间的链接关系。
 *
 * <p><b>执行入口</b>是 {@code LinkActionType#executeActionRule}：链接实例没有独立存储，
 * 需要按 {@code RelationshipType} 翻译成对对象表 / 连接表的 DML，规则本身不持有数据源。
 *
 * <p>{@link #linkTypeName} 是兼容字段：链接类型以 Action 级的 {@code linkTypeReference}
 * 为准，只有旧 Action 未配置它时才会回退到这里。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class DeleteLinkRule extends OntologyActionRule {

    /**
     * 兼容字段：使用 {@code LinkActionType#linkTypeReference}；仅当 Action 级链接类型为空时回退到这里。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {})
    public String linkTypeName;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String sourceObjectRidParam = "sourceRid";

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String targetObjectRidParam = "targetRid";

    /**
     * 双向删除开关。TIS 没有链接实例存储，不存在「反向链接」，取值 true 会被显式拒绝
     * （见 {@code LinkActionExecutor#rejectBidirectional}）。
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {})
    public Boolean deleteBidirectional = false;

    @Override
    public RuleType getRuleType() {
        return RuleType.DELETE_LINK;
    }

    @Override
    public String identityValue() {
        return "delete_link";
    }


    @Override
    public void accept(AdapterRuleVisitor visitor) {
        visitor.visit(this);
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.DELETE_LINK;
        }
    }
}
