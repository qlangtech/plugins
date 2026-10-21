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

/**
 * {@link OntologyActionRule} 的访问者。
 *
 * <p>配合各规则子类的 {@code accept(RuleVisitor)} 完成双分发：外部按规则子类型做差异化处理时，
 * 实现本接口即可，不必写 instanceof 链。
 *
 * <p>新增 {@link OntologyActionRule} 子类时须同步添加对应的 {@code visit} 重载，
 * 并在子类实现 {@code accept} —— 否则编译不通过，这是刻意保留的穷尽性。
 *
 * @see OntologyActionRule#accept(RuleVisitor)
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/18
 */
public interface RuleVisitor {

    public void visit(CreateObjectRule rule);

    public void visit(ModifyObjectRule rule);

    public void visit(DeleteObjectRule rule);

    public void visit(CreateLinkRule rule);

    public void visit(DeleteLinkRule rule);

    public void visit(FunctionBackedRule rule);

    public void visit(NotificationRule rule);

    public void visit(WebhookRule rule);
}
