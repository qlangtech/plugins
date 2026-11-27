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
package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetColumnConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.IWidgetColumnHost;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.List;

/**
 * P1 Widget：属性列表（Property List）
 * <p>
 * 分组标题直接用基类的 {@code title} 字段（前端读的也是 {@code config.title}），
 * 这里原先又声明了一个同名 {@code title} 把基类字段遮蔽掉 —— 表单反射整条继承链的字段，
 * 同名会让字段集合出现两个 {@code title}，配置值到底落在哪一个不确定。
 *
 * <h3>两个对象变量各有分工，不要合并</h3>
 * <table>
 *   <tr><th>字段</th><th>作用</th><th>能选什么</th></tr>
 *   <tr><td>{@link #objectVar}</td>
 *       <td><b>属性来源</b>：只用于级联生成 {@link #properties} 的候选行</td>
 *       <td>只有「对象集合定义」型变量有 schema（{@code ObjectSetVariable#getColsMeta()}）</td></tr>
 *   <tr><td>{@link #activeObjectVar}</td>
 *       <td><b>运行时展示对象</b>：组件订阅它的当前值</td>
 *       <td>任意 OBJECT_SET，含「外部写入」型 —— 那正是接收行选中项的典型用法</td></tr>
 * </table>
 *
 * <p>运行时取值<b>优先 {@code activeObjectVar}、回退 {@code objectVar}</b>（前端组件同口径）。
 * {@code activeObjectVar} 不加 {@link Validator#require}：存量 widget 只有 {@code objectVar}，
 * 加了会让所有老配置保存失败。
 *
 * <p>本类吸收了已删除的 {@code ObjectViewWidget}（原 P0 对象详情视图）—— 那个实现与本类
 * 同为 nz-descriptions 属性描述列表，唯一差异就是 {@code activeObjectVar}，现已并入。
 * 典型链路（详见 {@code ObjectListWidget#onActiveObjectSelection}）：ObjectList 行选中 →
 * 写 {@code activeObjectVar} → open-overlay 浮层里的本 Widget 实例化后首发即拿到该对象
 * （浮层只按名字寻址、没有参数通道，共享变量是唯一的传导方式）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/9
 */
public class PropertyListWidget extends WorkshopWidget implements IWidgetColumnHost {

    public static final String KEY_OBJECT_VAR = "objectVar";
    public static final String KEY_PROPERTIES = "properties";
    public static final String KEY_ACTIVE_OBJECT_VAR = "activeObjectVar";

    /** 展示对象来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 10)
    public String objectVar;

    @FormField(type = FormFieldType.INT_NUMBER,ordinal = 12,validate = {Validator.require,Validator.integer})
    public Integer columnsCount;

    /**
     * 要展示的属性列表，由子表单结构化管理（非手写 JSON）。
     * <p>
     * 原先声明为 {@code MULTI_SELECTABLE} 且没有登记行编辑器，前端拿到的是一个填不进去的
     * 多选下拉；改为 {@code MULTI_DESCRIBLE_PLUGIN} 后由 {@link WidgetColumnConfig} 承担行结构，
     * 前端读的是同一批 {@code prop}/{@code label}/{@code format} 字段。
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,desClazz = WidgetColumnConfig.class,ordinal = 11,validate = {Validator.require})
    public List<WidgetColumnConfig> properties;

    /**
     * 运行时展示对象：通常指向 ObjectList 的 activeObjectVar
     * （「外部写入」型 OBJECT_SET 变量），行选中后自动刷新。
     * <p>
     * 不加 {@link Validator#require}：存量 widget 只有 {@code objectVar}，加了会让所有老配置保存失败。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 13)
    public String activeObjectVar;

    @Override
    public String getBoundObjectSetVar() {
        // 只服务于「列/行候选从哪个对象集取 schema」（IWidgetColumnHost 契约），
        // 因此永远是属性来源 objectVar，不读 activeObjectVar —— 后者多半是没有 schema 的外部写入型。
        return this.objectVar;
    }

    @Override
    public List<WidgetColumnConfig> getColumnConfigs() {
        return this.properties;
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_VAR, WidgetOptionHelper.getObjectSetVariableOptions());
            // 运行时对象可以是任意 OBJECT_SET（含「外部写入」型），与 ObjectList 的 activeObjectVar
            // 用同一个供货方 —— 那两个字段指的是同一批变量。用 getObjectSetSourceVariableOptions
            // 会把唯一该选的类型（外部写入）排除掉。
            this.registerSelectOptions(KEY_ACTIVE_OBJECT_VAR, WidgetOptionHelper.getObjectSetVariableOptions());

            // === 级联管道：objectVar 变化时，按该对象集变量的属性自动生成候选属性行 ===
            this.valueChangePipe(KEY_OBJECT_VAR, KEY_PROPERTIES).render((pluginMeta, param) ->
                    WidgetColumnConfig.createFrom(
                            WidgetOptionHelper.getObjectPropertyMetas(param.getString(KEY_OBJECT_VAR))));
        }

        @Override
        public String getDisplayName() {
            return "Property List";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.PROPERTY_LIST;
        }
    }
}