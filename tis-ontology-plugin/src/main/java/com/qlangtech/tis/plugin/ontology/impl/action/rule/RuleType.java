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
 * Action Rule 类型枚举
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public enum RuleType {

    // ========== Ontology Rules - Object ==========
    CREATE_OBJECT("创建对象", "创建新的对象实例", RuleGroup.ONTOLOGY_OBJECT),
    MODIFY_OBJECT("修改对象", "修改现有对象的属性值", RuleGroup.ONTOLOGY_OBJECT),
    CREATE_OR_MODIFY_OBJECT("创建或修改对象", "如果对象存在则修改，否则创建", RuleGroup.ONTOLOGY_OBJECT),
    DELETE_OBJECT("删除对象", "删除指定的对象实例", RuleGroup.ONTOLOGY_OBJECT),

    // ========== Ontology Rules - Link ==========
    CREATE_LINK("创建链接", "在两个对象之间创建链接关系", RuleGroup.ONTOLOGY_LINK),
    DELETE_LINK("删除链接", "删除两个对象之间的链接关系", RuleGroup.ONTOLOGY_LINK),

    // ========== Ontology Rules - Function ==========
    FUNCTION_BACKED("函数支持", "通过自定义函数执行复杂业务逻辑", RuleGroup.ONTOLOGY_FUNCTION),

    // ========== Ontology Rules - Interface ==========
    CREATE_OBJECT_OF_INTERFACE("创建接口对象", "创建实现特定接口的对象", RuleGroup.ONTOLOGY_INTERFACE),
    MODIFY_OBJECT_OF_INTERFACE("修改接口对象", "修改实现特定接口的对象", RuleGroup.ONTOLOGY_INTERFACE),
    DELETE_OBJECT_OF_INTERFACE("删除接口对象", "删除实现特定接口的对象", RuleGroup.ONTOLOGY_INTERFACE),
    CREATE_LINK_OF_INTERFACE("创建接口链接", "为接口对象创建链接", RuleGroup.ONTOLOGY_INTERFACE),
    DELETE_LINK_OF_INTERFACE("删除接口链接", "删除接口对象的链接", RuleGroup.ONTOLOGY_INTERFACE),

    // ========== Side Effect Rules ==========
    NOTIFICATION("发送通知", "执行 Action 后发送通知给用户", RuleGroup.SIDE_EFFECT),
    WEBHOOK("调用外部 API", "Action 执行后调用外部 Webhook", RuleGroup.SIDE_EFFECT),
    WRITEBACK_WEBHOOK("编辑前调用外部 API", "Action 执行前调用外部 Webhook 进行验证或准备", RuleGroup.SIDE_EFFECT),

    // ========== Advanced Rules ==========
    SCHEDULE("触发数据构建", "触发 Ontology 数据的重新构建或刷新", RuleGroup.ADVANCED),
    APPLY_SCENARIO("合并场景编辑", "将场景中的编辑合并到主数据", RuleGroup.ADVANCED);

    private final String displayName;
    private final String description;
    private final RuleGroup group;

    RuleType(String displayName, String description, RuleGroup group) {
        this.displayName = displayName;
        this.description = description;
        this.group = group;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }

    public RuleGroup getGroup() {
        return group;
    }

    /**
     * 是否为写操作规则（会修改数据）
     */
    public boolean isWriteOperation() {
        return group == RuleGroup.ONTOLOGY_OBJECT
            || group == RuleGroup.ONTOLOGY_LINK
            || group == RuleGroup.ONTOLOGY_INTERFACE;
    }

    /**
     * 是否为副作用规则（不修改主数据，但有外部影响）
     */
    public boolean isSideEffect() {
        return group == RuleGroup.SIDE_EFFECT;
    }
}