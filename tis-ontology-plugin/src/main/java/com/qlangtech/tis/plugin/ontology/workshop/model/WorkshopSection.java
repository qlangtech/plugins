package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.DropHandling;

import java.util.ArrayList;
import java.util.List;

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
 * （{@code name} 标 {@code identity=true}）。section 不落盘、无 store 消费者，identity 只在框架双向校验
 * 与表单里作行键存在，无副作用。
 * <p>
 * 原先还有一个私有的 {@code id}（构造器生成 UUID、无 {@code @FormField}、非 identity），
 * 全工程零调用点，已删除。注意<b>前端</b>的 section 模型另有自己的 id 且是活的
 * （{@code section.parentId}、CDK drop list id、树节点 key 都在用），与本类无关。
 */
public final class WorkshopSection extends AbstractWorkshopSection implements Describable<WorkshopSection> {

    private static final long serialVersionUID = 1L;
    @FormField(ordinal = 4)
    public DropHandling dropHandling;
    /**
     * 子分区列表。有界一层：{@link SectionChild} 无 {@code childSections}，故不再有孙级。
     */
    @FormField(ordinal = 6, desClazz = SectionChild.class, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN)
    public List<SectionChild> childSections = new ArrayList<>();

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopSection> {
        @Override
        public String getDisplayName() {
            return "Workshop Section";
        }
    }
}
