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
package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.button.group;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ButtonColor;

/**
 * 默认颜色：跟随按钮形态（即不指定意图色）。零字段 —— 见 {@link ButtonColor} 的多态理由。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/20
 */
public class NoneButtonColor extends ButtonColor {

    private static final long serialVersionUID = 1L;

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            // 必须是短 key "none"：WidgetButtonConfig.json 的 color.dftVal 要逐字对它，
            // 前端 resolveButtonColor 也按这个短 key 判态。
            return "none";
        }

        @Override
        public String shortComment() {
            // 该颜色类型被按钮、指标卡文字色 / 背景色复用，措辞不能只提「按钮」。
            return "默认：不指定颜色";
        }
    }
}
