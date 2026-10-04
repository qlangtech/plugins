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

import com.qlangtech.tis.plugin.annotation.FormField;

/**
 * <b>单行表单控件</b>类 Widget 的基类：显示配置只有宽度与条件可见性。
 *
 * <h3>两个显示配置基类的分工</h3>
 * <table border="1">
 *   <caption>分组</caption>
 *   <tr><th>基类</th><th>displayConfig 类型</th><th>适用</th></tr>
 *   <tr><td>{@link CompactDisplayWidget}（本类）</td>
 *       <td>{@link CompactDisplayConfig}（width + conditionalVisibility）</td>
 *       <td>自身就是<b>单行表单控件</b>的 Widget —— 输入框、复选框、下拉、日期选择器、按钮、标题文本。<br>
 *           它们的「高度」就是控件高度，也没有挂载 / 卸载策略可言。</td></tr>
 *   <tr><td>{@link FullDisplayWidget}</td>
 *       <td>{@link WidgetDisplayConfig}（宽度 + 高度 + 条件可见性 + 展示优化）</td>
 *       <td>自身有<b>内容区</b>的 Widget —— 表格、列表、图表、看板、时间线、Markdown、
 *           Tabs、过滤条件列表、Groovy 脚本。它们的内容长短不定，高度与挂载策略都说得通。</td></tr>
 * </table>
 *
 * <h3>判定口径</h3>
 * 分组<b>不看</b> {@code WorkShopWidgetCategory}。分类是调色板的展示分组，与「这个 Widget
 * 说不说得上高度」无关 —— 例如 {@code FilterListWidget} 属于 FILTERING 分类，但它的
 * {@code filters} 是可增删的条件元组列表、行数会长，因此它在 {@link FullDisplayWidget} 组。
 * 判据只有一条：<b>自身是不是单行表单控件</b>。
 *
 * <h3>为什么用继承而不是「一个类 + 若干字段」</h3>
 * 本仓已有同类先例：{@code DataxReader} / {@code DataxWriter} 各有两条抽象分支
 * （{@code BasicDataXRdbmsReader} vs {@code AbstractDFSReader}、
 * {@code BasicDataXRdbmsWriter} vs {@code BasicFSWriter}），存在的目的就是让不同子类
 * 拿到不同的表单。表单构建按祖先链逐层 {@code getDeclaredFields()} 收集
 * （{@code PluginExtraProps.visitAncestorsClass}），所以两个分支各自声明一个同名的
 * {@code displayConfig} 不会冲突 —— 任何一条继承链上只会出现其中一个。
 *
 * <h3>⚠️ 本类及 {@link FullDisplayWidget} 不得加 {@code @TISExtension}</h3>
 * 插件枚举走 sezpoz 注解索引，加载路径（{@code ExtensionFinder.Sezpoz}）<b>没有抽象类过滤</b>；
 * 唯一的守卫就是「中间抽象类不带 {@code @TISExtension}、也没有嵌套 Descriptor」这一事实。
 * 一旦加上，本类会以「可创建 Widget」的身份混进调色板，而且<b>运行期不会报任何错</b>。
 * 这与本仓既有约定一致（{@code AxisConfig.BasicDescriptor}、
 * {@code VariableDefinitionConfig.BasicDescriptor} 同样是不带注解的抽象中间类）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/20
 * @see FullDisplayWidget
 * @see CompactDisplayConfig
 */
public abstract class CompactDisplayWidget extends WorkshopWidget {

    private static final long serialVersionUID = 1L;

    /** 实例级显示配置（宽度 / 条件可见性）—— 字段名与 {@link FullDisplayWidget} 一致，前端读取路径两组通用 */
    @FormField(ordinal = 4)
    public CompactDisplayConfig displayConfig;
}
