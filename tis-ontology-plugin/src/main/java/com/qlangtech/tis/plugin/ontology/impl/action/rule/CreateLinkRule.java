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
 * 创建链接规则
 *
 * 在两个对象之间创建链接关系
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public class CreateLinkRule extends OntologyActionRule {

    @FormField( ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String linkTypeName;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String sourceObjectRidParam = "sourceRid";

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String targetObjectRidParam = "targetRid";

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {})
    public Boolean bidirectional = false;

    @Override
    public RuleType getRuleType() {
        return RuleType.CREATE_LINK;
    }

    @Override
    public String identityValue() {
        return "create_link";
    }

    @Override
    public RuleExecutionResult execute(RuleExecutionContext context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            // 1. 获取源对象 RID
            String sourceRid = context.getParameter(sourceObjectRidParam, String.class);
            if (sourceRid == null) {
                return RuleExecutionResult.failure("未找到参数: " + sourceObjectRidParam);
            }

            // 2. 获取目标对象 RID
            String targetRid = context.getParameter(targetObjectRidParam, String.class);
            if (targetRid == null) {
                return RuleExecutionResult.failure("未找到参数: " + targetObjectRidParam);
            }

            // 3. 创建链接（实际实现需要调用 Ontology 服务）
            // TODO: 集成 Ontology 链接创建服务
            // ontologyLinkService.createLink(
            //     context.getOntologyDomain(),
            //     linkTypeName,
            //     sourceRid,
            //     targetRid
            // );

            // 4. 如果是双向链接，创建反向链接
            // if (bidirectional) {
            //     ontologyLinkService.createLink(
            //         context.getOntologyDomain(),
            //         linkTypeName + "_reverse",
            //         targetRid,
            //         sourceRid
            //     );
            // }

            // 5. 将创建的链接信息存入上下文
            context.setContextData("createdLinkType", linkTypeName);
            context.setContextData("createdLinkSource", sourceRid);
            context.setContextData("createdLinkTarget", targetRid);

            long executionTime = System.currentTimeMillis() - startTime;

            RuleExecutionResult.Builder builder = RuleExecutionResult.builder()
                .success(true)
                .addAffectedObjectRid(sourceRid)
                .addAffectedObjectRid(targetRid)
                .putResultData("linkType", linkTypeName)
                .putResultData("sourceRid", sourceRid)
                .putResultData("targetRid", targetRid)
                .putResultData("bidirectional", bidirectional)
                .executionTimeMs(executionTime);

            return builder.build();

        } catch (Exception e) {
            return RuleExecutionResult.failure(e);
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseRuleDescriptor {

        @Override
        public RuleType getRuleType() {
            return RuleType.CREATE_LINK;
        }
    }
}
