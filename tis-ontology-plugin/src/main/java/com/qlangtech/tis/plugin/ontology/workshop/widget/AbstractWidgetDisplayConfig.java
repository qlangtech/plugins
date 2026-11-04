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

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.BaseWidgetDescriptor;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidgetDisplay;

import java.io.Serializable;

/**
 * Widget 显示配置的抽象基类 —— 独立插件实体，一个 Widget 至多一份，落盘到
 * {@code widgets/{widgetId}_display.xml}（见 {@code WorkshopWidgetDisplayStore}）。
 *
 * <h3>为什么是独立实体</h3>
 * 原先显示配置是 Widget 基类（{@code FullDisplayWidget} / {@code CompactDisplayWidget}）上的
 * 一个字段，随 Widget 一起写进 {@code {widgetId}.xml}。这带来两个问题：编辑「显示」也要整份
 * 重写 Widget 表单；「哪些 Widget 需要高度 / 挂载策略」这条语义被固化进 Widget 的继承树。
 * 现在它成为独立扩展点 {@link IWorkshopWidgetDisplay} 的一个实现，Widget 侧只剩一个
 * transient 的 {@code display} 引用（加载时回填）。
 *
 * <h3>身份字段 {@link #widgetId}</h3>
 * 它既是<b>所属 Widget 的 id</b>，也是本实体的 identity —— 两者是同一个值，不是两份数据。
 * 落盘文件名 {@code {widgetId}_display.xml} 由 {@code WorkshopWidgetDisplayStore.getStorageKey}
 * 从它生成；反向读取时也能直接从实例问「你属于哪个 Widget」，不必再从文件名反解
 * （差集删除、批量索引两条路径都受益）。
 *
 * <p>服务端在 {@code WorkshopModule.parseWidgetDisplay} 解析后按<b>当前 Widget 的 id</b>
 * 覆写它，不采信前端提交的值；表单里它是 {@code readonly} 的展示项 —— 改了等于把这份配置
 * 挪到别的 Widget 名下。
 *
 * <p>为什么不干脆去掉它（2026-10-31 试过，2026-11-01 回退）：删掉它就<b>必然</b>连带删掉
 * {@link IdentityName} —— 框架的 {@code Descriptor.getPropertyTypes()} 对 identity 字段数量
 * 做双向校验，实现了该接口就必须恰好 1 个 {@code @FormField(identity = true)} 字段，0 个直接抛
 * {@code IllegalStateException}。而失去 {@link IdentityName} 的代价蔓延到整条存储链路：
 * {@code AbstractWorkshopEntityStore} 的泛型上界要放宽、{@code save} 要加「身份由调用方显式给出」
 * 的重载、{@code deleteNotInStorageKeys} 要回落到文件名反解。冗余一份 id 换来的是所有这些
 * 调用点都不必知道「这个实体的身份不在实例上」。
 *
 * <h3>用哪个 impl —— 由 Widget 类型决定</h3>
 * 「完整 / 精简」是 Widget 类型的固有属性（原来就是两条继承分支），不是用户的选项。
 * 判据单点在 {@link #configClazzOf(IWorkshopWidget)}，前端的选择只是它的镜像
 * （读 {@code extractProps.fullDisplay}）。
 *
 * <p>⚠️ 本类<b>不带</b> {@code @TISExtension} —— sezpoz 不区分抽象类，带上会让它混进
 * 候选实现列表，前端会多出一个无法实例化的 impl。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/30
 * @see WidgetDisplayConfig
 * @see CompactDisplayConfig
 */
public abstract class AbstractWidgetDisplayConfig
        implements IWorkshopWidgetDisplay, IdentityName, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 所属 Widget 的 id —— 同时是本实体的 identity，决定落盘文件名
     * {@code {widgetId}_display.xml}。
     *
     * <p>表单里以 {@code readonly} 展示：值由服务端按当前 Widget 覆写，用户不能改
     * （改了等于把这份配置挪到别的 Widget 名下）。
     *
     * <p><b>刻意不加 {@link Validator#identity}</b>：它含「不能以数字开头」的限制，而
     * Widget 的 id 是前端生成的 UUID，有 10/16 的概率以数字开头 —— 加上会让保存随机失败
     * （与 {@code WorkshopWidget.id} 同一口径）。{@link Validator#require} 足够。
     *
     * <p><b>存量数据</b>：2026-10-31 曾把本字段删掉，那一版写出的 {@code _display.xml}
     * 没有 {@code <widgetId>} 元素。XStream 反序列化后该字段为 {@code null}，
     * 由 {@code WorkshopWidgetDisplayStore} 在加载时按文件名补回（见
     * {@code WorkshopWidgetDisplayStoreGetter} 的加载路径），差集删除不会因此抛异常。
     */
    @FormField(type = FormFieldType.INPUTTEXT, ordinal = 0, identity = true,
            validate = {Validator.require})
    public String widgetId;

    @Override
    public String identityValue() {
        if (this.widgetId == null) {
            throw new IllegalStateException(
                    getClass().getSimpleName() + " widgetId can not be null");
        }
        return this.widgetId;
    }

    /**
     * 该 Widget 使用哪一份显示配置 —— <b>唯一判据</b>：表格、图表、看板卡片、Markdown 正文这类
     * 有独立内容区的 Widget 用 {@link WidgetDisplayConfig}（宽度 / 高度 / 展示优化），
     * 单行表单控件与页头文字、按钮组用 {@link CompactDisplayConfig}（只有宽度）。
     *
     * <p>服务端在 {@code WorkshopModule.parseWidgetDisplay} 与持久化路径上都调本方法，
     * 前端表单提交上来的 {@code impl} <b>不采信</b>（表单只显示宽高等业务字段）。
     *
     * <p>{@code GroovyWorkshopWidget} 走默认值：它的 descriptor（{@code GroovyWidgetDescriptor}）
     * 不是 {@link BaseWidgetDescriptor} 的子类、没有 {@code WorkShopWidgetType}，
     * 而它原本就是 {@code FullDisplayWidget} 的分支。
     */
    public static Class<? extends AbstractWidgetDisplayConfig> configClazzOf(IWorkshopWidget widget) {
        if (widget == null) {
            return WidgetDisplayConfig.class;
        }
        Descriptor<?> desc = widget.getDescriptor();
        if (desc instanceof BaseWidgetDescriptor<?> base && !base.getWidgetType().fullDisplayWidget) {
            return CompactDisplayConfig.class;
        }
        return WidgetDisplayConfig.class;
    }
}
