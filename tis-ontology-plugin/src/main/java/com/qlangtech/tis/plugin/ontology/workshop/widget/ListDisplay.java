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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * 列表形态：每个对象占一行，属性在其后铺开。
 * <p>
 * 官方文档里 List 是默认形态，但在本工程中默认仍给了 {@link GridDisplay}
 * —— 前端 {@code object-list.component.ts} 两种形态均已渲染，保持默认 Grid
 * 只为避免存量 widget 的视觉回退。
 *
 * <p>属性的排布由前端 {@code nz-descriptions} 渲染，本类只提供「一行放几列」
 * （{@link #columnCount}）。原先还有一个「内联 / 对齐」的 {@code PropertyStyle}
 * 枚举，改用描述列表后这两种排布是同一种表格的不同列数，已删除。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/19
 */
public class ListDisplay extends ObjectListDisplay {

    private static final long serialVersionUID = 1L;

    /**
     * 一行放几组「属性名：属性值」，与 {@code ListDisplay.json} 的 {@code dftVal} 保持一致
     */
    public static final int DEFAULT_COLUMN_COUNT = 2;

    /**
     * 列数下限：1 列即「名称与值逐行竖排」
     */
    public static final int MIN_COLUMN_COUNT = 1;

    /**
     * 列数上限：超过 6 列名称与值都挤在一起
     */
    public static final int MAX_COLUMN_COUNT = 6;

    /**
     * 每行的列数（前端 {@code nz-descriptions} 的 {@code nzColumn}）。
     *
     * <p>合法区间 {@code [MIN_COLUMN_COUNT, MAX_COLUMN_COUNT]}，由
     * {@link DefaultDescriptor#validateColumnCount} 在保存时校验。
     */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer columnCount = DEFAULT_COLUMN_COUNT;

    /**
     * 是否给属性描述表画边框（前端 {@code nz-descriptions} 的 {@code nzBordered}）。
     *
     * <p>默认关闭：列表形态下每个对象一行，逐行画表格线视觉噪音太大；需要对齐感时可打开。
     * 与 {@code boolean} 字段一样走 {@code ENUM_BOOLEAN}（是 / 否）下拉，故 json 里不写 enum。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public Boolean showBorder = false;

    /**
     * XStream 反序列化不走构造器、也不执行字段初值：本字段上线前落盘的 {@code ListDisplay}
     * 没有 {@code <columnCount>} 元素，不兜底会得到 {@code null}，前端表单与画布都要处理空值。
     */
    private Object readResolve() {
        if (this.columnCount == null) {
            this.columnCount = DEFAULT_COLUMN_COUNT;
        }
        if (this.showBorder == null) {
            this.showBorder = Boolean.FALSE;
        }
        return this;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "List";
        }

        @Override
        public String shortComment() {
            return "列表：每个对象一行，属性在其后铺开";
        }

        /**
         * 校验 {@link #columnCount} 落在 1~6。
         *
         * <p>方法名 {@code validate<字段名首字母大写>} 是框架约定（见
         * {@code Descriptor#createValidateMap}），签名必须是
         * {@code (IFieldErrorHandler, Context, String, String)} 且返回 {@code boolean}；
         * 改名或改签名都会让校验<b>静默失效</b>。
         *
         * <p>{@code Validator.integer} 已经在前面挡掉了非整数，这里只管区间。
         */
        public boolean validateColumnCount(IFieldErrorHandler msgHandler, Context context,
                                           String fieldName, String value) {

            int count = Integer.parseInt(value.trim());

            if (count < MIN_COLUMN_COUNT || count > MAX_COLUMN_COUNT) {
                msgHandler.addFieldError(context, fieldName
                        , "列数必须在 " + MIN_COLUMN_COUNT + "~" + MAX_COLUMN_COUNT + " 之间，当前为 " + count);
                return false;
            }
            return true;
        }
    }
}