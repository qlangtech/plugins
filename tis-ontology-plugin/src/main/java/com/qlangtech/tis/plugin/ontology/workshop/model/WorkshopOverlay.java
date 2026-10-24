package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IStoreSelfManagement;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.OverlayType;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableBasedVisibility;
import com.qlangtech.tis.plugin.ontology.workshop.model.overlay.OverlayTypeConfig;
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

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public OverlayType type;

    @FormField(ordinal = 3, validate = {Validator.require})
    public OverlayTypeConfig typeConfig;

    @FormField(ordinal = 4, type = FormFieldType.ENUM)
    public Boolean showHeader = true;

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT)
    public String title;

    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT)
    public String icon;

    @FormField(ordinal = 7)
    public VariableBasedVisibility variableBasedVisibility;

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
    public static class DefaultDescriptor extends Descriptor<WorkshopOverlay> {
        @Override
        public String getDisplayName() {
            return "Workshop Overlay";
        }
    }
}
