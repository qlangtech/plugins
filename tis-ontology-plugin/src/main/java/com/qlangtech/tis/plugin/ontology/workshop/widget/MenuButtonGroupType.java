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

/**
 * 折叠菜单：触发按钮 + 下拉菜单。按钮排布由 {@code nz-dropdown-menu} 固定，故<b>零字段</b>。
 *
 * <p>本类没有 {@code @FormField} 字段，同包下<b>没有</b>对应 {@code MenuButtonGroupType.json}
 * （TIS 只在类上存在表单字段时才去查同名资源，写了是死文件 —— 同 {@link NoPagenator}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/20
 */
public class MenuButtonGroupType extends ButtonGroupType {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "menu";
        }

        @Override
        public String shortComment() {
            return "菜单：折叠为下拉菜单";
        }
    }
}
