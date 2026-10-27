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
package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

/**
 * 内联平铺：按钮逐个横 / 竖排开。唯一带 {@code layout} 载荷的子类 —— 见 {@link ButtonGroupType}
 * 的多态理由。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/20
 */
public class InlineButtonGroupType extends ButtonGroupType {

    private static final long serialVersionUID = 1L;

    /** 按钮排列方向（仅内联平铺下有意义） */
    @FormField(ordinal = 0,type = FormFieldType.ENUM)
    public GroupLayout layout = GroupLayout.horizontal;

    /** 按钮排列方向；取值固定，硬编码于此（CLAUDE.md 原则二：固定取值用 ENUM） */
    public enum GroupLayout implements DescriptorUseableShortComment {
        horizontal("水平排列"), vertical("垂直排列");
        public final String label;

        GroupLayout(String label) {
            this.label = label;
        }

        @Override
        public String shortComment() {
            return this.label;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            // 必须是短 key "inline"：ButtonGroupWidget.json 的 buttonType.dftVal 要逐字对它，
            // 前端 resolveButtonGroupType 也按这个短 key 判态。写成中文标签不报错，只会静默换子类。
            return "inline";
        }

        @Override
        public String shortComment() {
            return "内联：按钮平铺展示";
        }
    }
}
