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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * 「自定义」形态的 {@link DisplayConfig}：由 Action 定义方决定运行时表单的
 * 紧凑布局开关与模态框宽度。
 *
 * <p>两个字段都 required；{@link #formWidth} 用 {@code INT_NUMBER}（构造器第三参自动附
 * {@code Validator.integer}）再叠加 {@code Validator.positive_integer} 拦住 0 与负数；
 * 上限 {@link #MAX_FORM_WIDTH} 由 {@code DefaultDescriptor#validateFormWidth} 在字段级校验中强制。
 * 字段级取值描述见同包资源 {@code DisplayConfigOn.json}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/21
 * @see DisplayConfigOff
 */
public class DisplayConfigOn extends DisplayConfig {

    /**
     * {@link #formWidth} 的取值上限（像素）。前端 nz-modal 按 viewport 等比留白，
     * 超宽模态会被挤压或溢出；前端在输入框上有同一上限的软约束
     * （{@code item-prop-val} 的 {@code nzMax}，由 json 的 {@code max} 驱动），
     * 但输入约束拦不住存量数据，所以保存期硬校验才是唯一闸门。
     */
    public static final int MAX_FORM_WIDTH = 900;

    /**
     * 表单 label 与控件是否采用紧凑垂直布局（label 在上、控件在下、间距最小化）。
     *
     * <p> {@code Boolean} + {@code ENUM} 是全工程布尔字段的唯一写法（无 BOOLEAN 类型），
     * 前端按 bool_mode 自动渲染成「是/否」开关。缺省 {@code true} —— 与 displayConfig
     * 引入前运行时模态的硬编码行为一致，用户翻到「on」不动其他项时外观不变。
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean compactVerticalLayout = true;

    /**
     * 运行时表单模态框的显示宽度（像素），必须是正整数且不能大于 {@link #MAX_FORM_WIDTH}；
     * 前端 {@code nz-modal} 先按缺省 600 打开、配置到货后经 {@code NzModalRef.updateConfig} 覆盖。
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER,
            validate = {Validator.require, Validator.positive_integer})
    public Integer formWidth = 600;

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<DisplayConfigOn>
            implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }

        @Override
        public String shortComment() {
            return "自定义表单布局与宽度";
        }

        /**
         * {@code formWidth} 单字段校验（框架按 {@code validate} + 字段名反射挂接，见
         * {@code Descriptor#createValidateMap}）：值不得超过 {@link DisplayConfigOn#MAX_FORM_WIDTH}。
         *
         * <p>不做成 {@code Validator} 枚举新成员 —— 上限是本插件私有语义，进公共枚举会
         * 污染全局校验词表；标准 {@code Validator} 是纯正则规则，也表达不了带参比较。
         *
         * <p>挂接点在标准 {@code @FormField(validate)} 校验<b>之后</b>（同为字段级，
         * {@code Descriptor#isValid} 内先跑枚举校验器再调本方法）。注意枚举校验失败并不会
         * 跳过本方法，所以非整数值（如 {@code "abc"}）也会进来 —— 此时 {@code Validator.integer}
         * 已报过错，这里吞掉解析异常直接放行，避免同一字段叠两条错误。
         *
         * @param value 表单原始值（字符串）；空白值在挂接点已被过滤，不会进来
         * @return false 校验失败，中止保存
         */
        public boolean validateFormWidth(IFieldErrorHandler msgHandler, Context context
                , String fieldName, String value) {
            try {
                int width = Integer.parseInt(value);
                if (width > DisplayConfigOn.MAX_FORM_WIDTH) {
                    msgHandler.addFieldError(context, fieldName
                            , "表单宽度不能大于" + DisplayConfigOn.MAX_FORM_WIDTH + "像素");
                    return false;
                }
            } catch (NumberFormatException e) {
                // 非整数交由字段级 Validator.integer / positive_integer 报错，这里不重复报
            }
            return true;
        }
    }
}
