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
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 默认分页器：服务端分页（后端先 count 出总数、再取该页）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/9
 */
public class DefaultPagenator extends Pagenator {

    private static final long serialVersionUID = 1L;

    /**
     * 每页显示条数
     */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer pageSize;

    /**
     * 是否显示「总记录数 + 可随机跳转的页码」。
     *
     * <p>{@code true}：分页条给出总数与页码，用户可任意跳页；
     * {@code false}：只保留上一页 / 下一页。后者是给 ElasticSearch 这类<b>不支持深翻页</b>的
     * 数据源留的口子 —— 在那种数据源上渲染出可点击的页码，点下去只会得到一个错误或一段
     * 无法接受的耗时。
     *
     * <p>无论取值如何，后端都照常回总数：{@code nz-table} 在 {@code nzFrontPagination=false}
     * 下要靠总数算「下一页」是否可点。
     *
     * <p>本字段无需显式 {@code type} —— {@code boolean} 会被自动挂上
     * {@code OverwriteProps.ENUM_BOOLEAN}（是 / 否）下拉，故 {@code .json} 里也不必手写 enum。
     */
    @FormField(ordinal = 1,type = FormFieldType.ENUM)
    public boolean showPageNumber = true;

    @Override
    public boolean pagingEnabled() {
        return true;
    }

    @Override
    public int resolvePageSize() {
        return pageSize != null ? pageSize : DEFAULT_PAGE_SIZE;
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Default";
        }

        @Override
        public String shortComment() {
            return "开启服务端分页";
        }
    }
}
