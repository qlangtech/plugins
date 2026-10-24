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
import com.qlangtech.tis.extension.IStoreSelfManagement;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.ontology.workshop.store.AbstractWorkshopEntityStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopWidgetStore;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Optional;

/**
 * Workshop Widget 抽象基类 —— 既是插件扩展点的实例载体，也是布局实体的字段宿主。
 *
 * <h3>为什么只有一个类</h3>
 * 原先存在两个平行概念：抽象层的 {@code WorkshopWidgetDescribable}（定位为「运行时行为」）
 * 与 ontology 插件里的 {@code WorkshopWidget} 实体（定位为「布局 / 定位」），二者靠
 * {@code setupConfig} 字段做组合，而 {@code setupConfig} 声明的是 {@code WidgetSetupConfig} ——
 * 一个只有 3 个零引用子类的类型，因此该字段恒为 {@code null}，组合从未真正成立。
 * <p>
 * TIS 的插件本来就是充血模型，「运行时行为」与「布局」不需要拆成两个类型：具体 Widget 子类
 * 通过 {@code @FormField} 声明自己的配置字段，通用的布局字段由本基类提供。
 * 因此删除了 {@code WorkshopWidgetDescribable}（本类即其更名版本）、ontology 侧的
 * {@code WorkshopWidget} 实体、以及 {@code WidgetSetupConfig} 及其三个子类。
 *
 * <h3>类型身份</h3>
 * 「我是哪一种 Widget」由<b>子类的 Java 类型</b>承担，<b>没有</b>与之平行的类型枚举或
 * 类型字段。原先的 {@code WidgetType} 枚举（20 个常量，中文标签与各子类描述符
 * {@code shortComment()} 逐字重复）属于重复分类体系，已删除。
 * 面向用户的短 key（{@code object-table}、{@code chart-xy} …）由各子类描述符经
 * {@code WorkShopWidgetType} + {@code getExtractProps()} 下发。
 * <p>
 * 实例 JSON 上具体类以扁平 {@code impl} 键（FQCN）出现，前端读
 * {@code setupConfig.impl} 即得类型，无需额外字段。
 *
 * <h3>字段序位</h3>
 * 基类占用 {@code 0}、{@code 1}（{@code 4} 让给了显示配置，见下）；各子类字段从 {@code 10} 起编排，
 * 避免 ordinal 撞车 —— TIS 的表单排序是对 {@code formField.ordinal()} 做稳定排序，
 * 而字段集合来自 HashMap，相同 ordinal 的相对顺序不确定。{@code 2}、{@code 3} 原为变量绑定字段
 * 所占用，两字段已删除（见下），序位留空无害，不必重排。
 *
 * <h3>显示配置为何不在基类</h3>
 * 本类原先声明 {@code WidgetDisplayConfig displayConfig}（宽度 / 高度 / 条件可见性 / 展示优化），
 * 于是<b>每个</b> Widget 的表单都渲染这四项。但对单行表单控件（日期选择器、输入框、复选框…
 * 见 {@link CompactDisplayWidget}）而言，「高度」与「展示优化」没有语义 —— 用户填了不会有任何
 * 效果，只会困惑。
 * <p>
 * 因此把显示配置下移到两条抽象分支，各自声明<b>同名的</b> {@code displayConfig} 字段：
 * <ul>
 *   <li>{@link CompactDisplayWidget} → {@link CompactDisplayConfig}（宽度 + 条件可见性）</li>
 *   <li>{@link FullDisplayWidget} → {@link WidgetDisplayConfig}（完整四项）</li>
 * </ul>
 * 具体 Widget 必须继承<b>两者之一</b>。⚠️ 直接继承本类虽能编译通过，但会<b>静默丢失</b>
 * 显示配置（表单上少一整块，且无任何报错）—— 这条已由
 * {@code TestWidgetVariableContract#everyWidgetInheritsADisplayWidgetBase} 兜住。
 *
 * <h3>变量绑定字段为何不在基类</h3>
 * 基类原先声明 {@code List<String> inputVariables} / {@code outputVariables}，作为
 * 「角色 → 变量」的绑定容器。该设计不成立：其一，它把变量<b>角色</b>（objectSet / dataSource /
 * activeObject …）压成了一个无角色的字符串列表，而每个 Widget 的角色各不相同，基类无法表达；
 * 其二，{@code MULTI_SELECTABLE} 需要 {@code enum} 或 {@code elementCreator} 才能渲染，
 * 这两字段两样都没有，导致前端 {@code buildMultiSelectedAttr} 在取不到 {@code eprops.enum}
 * 时直接抛错 —— <b>20 个 Widget 的表单全部无法渲染</b>；其三，全工程没有任何代码读写它们
 * （无依赖图、无执行引擎、无序列化、无校验）。
 * 因此删除，改由各子类声明自己的<b>类型化</b>绑定字段
 * （如 {@code ObjectTableWidget.objectSetVar}、{@code ObjectListWidget.activeObjectVar}），
 * 用 {@code FormFieldType.SELECTABLE} + {@code WidgetOptionHelper} 按变量类型供给选项。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/13
 */
public abstract class WorkshopWidget implements IWorkshopWidget, Serializable, IdentityName,
        IPluginStore.BeforePluginSaved, IStoreSelfManagement {
    
    /**
     * 组件 UUID（identity 字段），由前端创建 widget 时生成（{@code crypto.randomUUID()}），
     * <b>生命周期内不可变</b>；后端把它当不透明值原样持久化。表单里以 {@code readonly} 展示。
     *
     * <p>与 {@code WorkshopPage.id} / {@code AbstractWorkshopSection.id} 同口径：
     * 不提供默认值、也不在服务端兜底生成 —— id 的生成方是实体的创建者；
     * {@link #identityValue()} 在为空时直接抛异常，避免掩盖「id 在链路上丢失」。
     * <p>
     * {@link IdentityName} 不是可选的装饰：框架的 {@code Descriptor.getPropertyTypes()}
     * 对 identity 字段数量做<b>双向</b>校验 —— 实现了本接口就必须有且仅有 1 个
     * {@code identity = true} 字段，不实现则必须一个也没有，数量不对直接抛
     * {@code IllegalStateException}，而编译期毫无提示。
     * <p>
     * widget 的独立落盘走 {@code IStoreSelfManagement}（见
     * {@code WorkshopModule.persistEntities}），文件名即本字段；{@link #identityValue()}
     * 是差集删除的键。
     * <p>
     * <b>刻意不加 {@link Validator#identity}</b>：它含「不能以数字开头」的限制，而 UUID
     * 有 10/16 的概率以数字开头（同 {@code WorkshopPage.id}）。id 是前端生成的不透明值，
     * 不是用户输入的名称；{@link Validator#require} 足够。
     * <p>
     * <b>存量数据兼容</b>：本字段原名 {@code name}，旧 widget XML 的元素名是 {@code <name>}。
     * 改名后若不处理，反序列化拿不到值（id 为 null），存量 widget 会被跳过或保存失败。
     * 兼容是在 {@link WorkshopWidgetStore} 里对 TIS 共享的 XStream 实例注册字段别名
     * （{@code aliasField("name", WorkshopWidget.class, "id")}）实现的 ——
     * <b>不能用 {@code @XStreamAlias}</b>：TIS 的 {@code XStream2} 没有开启注解自动处理，
     * 该注解虽在 classpath 上却不生效（已由 {@code WorkshopLayoutStoreTest} 实测）。
     * 别名读写双向生效，故新写出的 XML 元素名仍是 {@code <name>}（内部存储形态，
     * 不影响表单 / 接口口径）。存量值可能是人读名称（如 {@code Widget 1}），
     * 按「不迁移」策略原样保留。
     */
    @FormField(type = FormFieldType.INPUTTEXT, ordinal = 0, identity = true, advance = false,
            validate = {Validator.require})
    public String id;

    /**
     * Widget 标题（画布与配置面板通用，面向用户展示）
     */
    @FormField(type = FormFieldType.INPUTTEXT, ordinal = 1, advance = false, validate = {Validator.require})
    public String title;

    /**
     * 所属 Section 的主键 —— 对应 {@code AbstractWorkshopSection.id}（UUID）。
     *
     * <p>持久化采用「子实体独立落盘 + 子侧外键归属」：widget 是独立的插件实例，
     * 与 section 之间不靠 XML 嵌套表达（{@code AbstractWorkshopSection.widgets} 是
     * transient），父子关系由本字段承担。加载某个 section 的全部 widget =
     * 按 {@code sectionId} 过滤 widget 存储；同一 section 内再用 {@link #sortOrder} 排序。
     *
     * <p><b>不是表单字段</b>（没有 {@code @FormField}）：归属关系由服务端在保存 widget 时
     * 从请求参数赋值，不由用户编辑；XStream 会照常持久化普通字段。
     * 前端模型 {@code WorkshopWidget.sectionId} 与本字段逐字对应。
     */
    public String sectionId;

    /**
     * 同一 Section 内的显示顺序（只在所属 section 内有意义）
     */
    // @FormField(type = FormFieldType.INT_NUMBER, ordinal = 5)
    public Integer sortOrder = 0;


    @Override
    public void beforeSaved(IPluginContext pluginContext, Optional<Context> context) {
        if (StringUtils.isEmpty(this.sectionId)) {
            throw new IllegalStateException("sectionId can not be empty");
        }
    }

    /**
     * 实例自管理落盘 —— 标准 {@code plugin_action} 链路与编辑器保存
     * （{@code module-apply} 增量 / {@code module-save} 兜底快照）共用本方法。
     *
     * <p>归属字段 {@link #sectionId} 与 {@code sortOrder} 不是表单字段：保存链路里由
     * {@code module-apply} / {@code module-save} 从信封字段回填，本方法只负责
     * 「拿到实例就写下去」。
     */
    @Override
    public void manipuldateProcess(IPluginContext pluginContext, UploadPluginMeta meta,
                                   Optional<Context> context) {
        WorkshopWidgetStore.create(
                        AbstractWorkshopEntityStore.getOntologyDomainId(meta),
                        AbstractWorkshopEntityStore.getModuleName(meta))
                .save(pluginContext, context, this);
    }

    @Override
    public String identityValue() {
        return this.id;
    }
}
