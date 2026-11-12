package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IStoreSelfManagement;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.WorkshopCptType;
import com.qlangtech.tis.plugin.ontology.workshop.enums.WorkshopCptTypeGetter;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.NoneVariableBasedVisibility;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableBasedVisibilityConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.overlay.OverlayTypeConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;
import com.qlangtech.tis.plugin.ontology.workshop.store.AbstractWorkshopEntityStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopOverlayStore;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Workshop Overlay 实体
 */
public final class WorkshopOverlay implements Describable<WorkshopOverlay>, Serializable, IdentityName,
        IStoreSelfManagement {

    private static final long serialVersionUID = 1L;

    /**
     * identity 字段：浮层 UUID。前端创建 overlay 时生成，后端把它当不透明值原样持久化。
     * 表单里以 {@code readonly} 展示。
     * <p>
     * <b>不提供默认值</b>：生成方是实体的创建者，后端只透传与持久化；
     * {@link #identityValue()} 在 id 为空时直接抛异常，不兜底生成。
     * <p>
     * <b>刻意不加 {@link Validator#identity}</b>：它含「不能以数字开头」的限制，而
     * {@code crypto.randomUUID()} 生成的 UUID 有 10/16 的概率以数字开头（同
     * {@link WorkshopPage#id}）。id 是前端生成的不透明值，只需非空。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT,
            validate = {Validator.require})
    public String id;

    /**
     * 浮层显示名称，<b>可编辑</b>；同时是运行态 open / close 事件的寻址键
     * （{@code LayoutService#openOverlays} 是按 name 存的集合、{@code open-overlay} 事件的
     * {@code option.value} 也是它）。树节点、配置面板等编辑期寻址走的仍是 {@link #id}。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    /**
     * 浮层布局模板：决定浮层里直接子 Section 的排布（与 {@link WorkshopPage#template}
     * 是同一套插件）。
     *
     * <p><b>⚠️ 聚合属性插件字段不得指定 {@code type}</b>：写成
     * {@code type = FormFieldType.SELECTABLE} 会让 {@code DescriptorsJSON} 走 SELECTABLE 分支去要
     * {@code getSelectOptions}，而多态 Describable 的类型信息本就由 {@code @TISExtension} 派生类提供，
     * 结果在取 descriptor 时直接抛 {@code IllegalStateException}，并连坐整个 module 的表单下发。
     *
     * <p><b>不提供 Java 字段初值</b>：XStream 反序列化不走构造器，初值对落盘数据无效；
     * 缺失时的兜底放在 {@link #readResolve()}（补 {@link PageTemplate#createDefault()}）。
     */
    @FormField(ordinal = 2, validate = {Validator.require})
    public PageTemplate template;

    @FormField(ordinal = 3, validate = {Validator.require})
    public OverlayTypeConfig typeConfig;

    @FormField(ordinal = 4, type = FormFieldType.ENUM)
    public Boolean showHeader = true;

    @FormField(ordinal = 6, type = FormFieldType.ENUM)
    public IEndTypeGetter.EndType icon;

    /**
     * 基于变量的可见性。声明为抽象父类，让「不启用」（{@link NoneVariableBasedVisibility}）
     * 在类型层面可表示；不加 {@code Validator.require} —— 该字段线上可缺省，
     * off 子类本身就是合法的「未启用」值。
     */
    @FormField(ordinal = 7)
    public VariableBasedVisibilityConfig variableBasedVisibility = new NoneVariableBasedVisibility();

    @FormField(ordinal = 8, type = FormFieldType.ENUM)
    public Boolean closeOnBackdropClick = true;

    @FormField(ordinal = 9, type = FormFieldType.ENUM)
    public Boolean addBackgroundBehindOverlay = true;

    @FormField(ordinal = 10, type = FormFieldType.INT_NUMBER)
    public Integer sortOrder = 0;

    // @FormField(ordinal = 11, type = FormFieldType.MULTI_SELECTABLE)
    // 非 final：加载时整体赋值（见 WorkshopModule#loadLayout）；XStream 反序列化后可能为 null
    public transient List<WorkshopSection> sections = new ArrayList<>();

    /**
     * 把 section 挂到本浮层下，并同步维护子侧外键
     * {@link AbstractWorkshopSection#parent}（OVERLAY + overlay id）—— 浮层是 section 的
     * 另一种父容器，与 {@code WorkshopPage#addSection} 对称。
     *
     * @throws IllegalStateException 本 overlay 还没有 id（见 {@link #identityValue()}）
     */
    public void addSection(WorkshopSection section) {
        if (sections == null) {
            sections = new ArrayList<>();
        }
        section.parent = WorkshopCptParentId.overlayParent(identityValue());
        sections.add(section);
    }

    /**
     * 本浮层顶层 section 的序号归一化 —— {@code sortOrder} 是「父容器内序号」，
     * 与页面内的 section 序号互不相干。
     */
    public void normalizeSectionOrder() {
        if (sections == null) {
            return;
        }
        AbstractWorkshopSection.normalizeOrder(sections,
                s -> s.sortOrder == null ? 0 : s.sortOrder,
                (s, index) -> s.sortOrder = index);
    }

    /**
     * XStream 反序列化后的补偿钩子：兜底 {@link #variableBasedVisibility} 与 {@link #template} 缺失。
     *
     * <p>字段上的 Java 初值对落盘数据无效 —— XStream 不走构造器。
     * 存量 overlay XML 里可能没有这两个元素，缺失时分别补「不启用」与
     * {@link PageTemplate#createDefault()}（与 {@link WorkshopPage#readResolve()} 同构）。
     *
     * @return 本实例（XStream 约定）
     */
    private Object readResolve() {
        if (variableBasedVisibility == null) {
            variableBasedVisibility = new NoneVariableBasedVisibility();
        }
        if (template == null) {
            template = PageTemplate.createDefault();
        }
        return this;
    }

    /**
     * @throws IllegalStateException id 为空 —— 说明创建方（前端建 overlay）没有下发 id，
     *                               属于链路上的缺陷，此处不兜底生成
     */
    @Override
    public String identityValue() {
        if (this.id == null) {
            throw new IllegalStateException("WorkshopOverlay id can not be null");
        }
        return this.id;
    }

    public String getId() {
        return identityValue();
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * 实例自管理落盘 —— 标准 {@code plugin_action} 链路与编辑器保存
     * （{@code module-apply} 增量 / {@code module-save} 兜底快照）共用本方法。
     */
    @Override
    public void manipuldateProcess(IPluginContext pluginContext, UploadPluginMeta meta,
                                   Optional<Context> context) {
        WorkshopOverlayStore.create(
                        AbstractWorkshopEntityStore.getOntologyDomainId(meta),
                        AbstractWorkshopEntityStore.getModuleName(meta))
                .save(pluginContext, context, this);
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopOverlay> implements WorkshopCptTypeGetter {
        @Override
        public String getDisplayName() {
            return "Workshop Overlay";
        }

        @Override
        public WorkshopCptType getWorkshopCptType() {
            return WorkshopCptType.Overlay;
        }
    }
}
