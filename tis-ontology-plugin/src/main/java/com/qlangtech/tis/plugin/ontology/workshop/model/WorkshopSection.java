package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IStoreSelfManagement;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.DropHandling;
import com.qlangtech.tis.plugin.ontology.workshop.store.AbstractWorkshopEntityStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopSectionStore;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Workshop Section 实体（可含子分区的<b>容器</b>），共享字段见基类 {@link AbstractWorkshopSection}。
 * <p>
 * {@link #childSections} 由原来的 {@code List<WorkshopSection>} 自引用收敛为有界一层
 * {@code List<SectionChild>} —— {@link SectionChild} 不含 {@code childSections}，从而断掉
 * {@code WorkshopSection → childSections → WorkshopSection → …} 的自引用，规避
 * {@code DescriptorsJSON.getDescriptorsJSON()} 无环检测/无深度上限导致的 StackOverflowError，
 * 并按 {@code MULTI_DESCRIBLE_PLUGIN} 契约做成表单字段（可在配置面板添加子分区）。
 * <p>
 * 类上不再声明「不实现 IdentityName」：identity 因共享基类 {@link AbstractWorkshopSection} 而上收
 * （{@code id} 标 {@code identity=true}）。id 为 UUID，由前端创建 section 时生成、随模块 JSON 落到后端，
 * 事件载荷与前端渲染都以它寻址；name 仅作展示 / 引用键。
 */
public final class WorkshopSection extends AbstractWorkshopSection
        implements Describable<WorkshopSection>, IStoreSelfManagement {

    private static final long serialVersionUID = 1L;
    @FormField(ordinal = 6, validate = {Validator.require})
    public DropHandling dropHandling;
    /**
     * 子分区列表。有界一层：{@link SectionChild} 无 {@code childSections}，故不再有孙级。
     */
    @FormField(ordinal = 7, desClazz = SectionChild.class, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN)
    public List<SectionChild> childSections = new ArrayList<>();

    /**
     * 把子分区挂到本 section 下，并维护子侧外键：子分区是本节点的
     * <p>
     * 子分区当前随本 section 嵌在同一个 XML 里（{@code childSections} 不是 transient），
     * 外键只是把归属关系显式化 —— 将来若改为独立落盘，加载语义不需要再迁移。
     */
    public void addChildSection(SectionChild child) {
        if (childSections == null) {
            childSections = new ArrayList<>();
        }
        child.parent = WorkshopCptParentId.sectionParent(identityValue());
        childSections.add(child);
    }

    /**
     * 子分区在本 section 内的序号归一化（{@code sortOrder} 是父容器内序号，
     * 与页面 / 浮层层级上的 section 序号互不相干）。
     */
    public void normalizeChildSectionOrder() {
        AbstractWorkshopSection.normalizeOrder(childSections,
                s -> s.sortOrder == null ? 0 : s.sortOrder,
                (s, index) -> s.sortOrder = index);
    }

    /**
     * 子分区的归属由父节点唯一决定：它们嵌在本 section 的表单里，
     * 前端拿不到也不该编辑 {@code parentId} / {@code parentType}，
     * 因此在这里统一回填，避免表单回传的 {@code null} 触发基类的必填校验。
     */
    @Override
    public void beforeSaved(IPluginContext pluginContext, Optional<Context> context) {
        super.beforeSaved(pluginContext, context);
        if (this.childSections != null) {
            for (SectionChild child : this.childSections) {
                child.parent = WorkshopCptParentId.sectionParent(identityValue());
            }
            // 子分区随父 section 落盘：写之前按数组顺序压实序号，避免带空洞或重号的
            // sortOrder 被原样持久化。加载侧同样会归一化，两边口径保持一致。
            normalizeChildSectionOrder();
        }
    }

    /**
     * 实例自管理落盘 —— 标准 {@code plugin_action} 链路（{@code IPluginStore#noSaveStore}）
     * 与编辑器保存（{@code module-apply} 增量 / {@code module-save} 兜底快照）共用本方法。
     *
     * <p>归属字段 {@link #parent} 不是表单字段，表单链路里由父节点
     * （{@code WorkshopPage#addSection} / {@code WorkshopOverlay#addSection} /
     * {@link #addChildSection}）赋值；保存链路里由 {@code module-apply} /
     * {@code module-save} 从信封字段 {@code parentId} / {@code parentType} 回填。
     * 本方法只负责「拿到实例就写下去」。
     */
    @Override
    public void manipuldateProcess(IPluginContext pluginContext, UploadPluginMeta meta,
                                   Optional<Context> context) {
        WorkshopSectionStore.create(
                        AbstractWorkshopEntityStore.getOntologyDomainId(meta),
                        AbstractWorkshopEntityStore.getModuleName(meta))
                .save(pluginContext, context, this);
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopSection> {
        @Override
        public String getDisplayName() {
            return "Workshop Section";
        }
    }
}
