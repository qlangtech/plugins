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

import com.qlangtech.tis.extension.Descriptor;

/**
 * Action Rule 分组
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/6
 */
public enum RuleGroup {
    OFF(Descriptor.SWITCH_OFF, "不启用"),
    ONTOLOGY_OBJECT("对象操作", "创建、修改、删除对象"),
    ONTOLOGY_LINK("链接操作", "创建、删除对象之间的链接关系"),
    ONTOLOGY_FUNCTION("函数操作", "通过自定义函数执行复杂逻辑"),
    ONTOLOGY_INTERFACE("接口操作", "操作实现特定接口的对象"),
    SIDE_EFFECT("副作用操作", "通知、Webhook 等外部调用"),
    ADVANCED("高级操作", "数据构建、场景合并等");

    private final String displayName;
    private final String description;

    RuleGroup(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }
}