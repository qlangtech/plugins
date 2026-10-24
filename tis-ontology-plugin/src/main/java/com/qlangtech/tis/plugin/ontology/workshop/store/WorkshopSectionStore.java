package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopSection;

import java.util.Comparator;
import java.util.List;

/**
 * Workshop Section 的独立落盘存储（一个 section 一个文件）。
 *
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop/{moduleName}/sections/{sectionId}.xml
 * </pre>
 *
 * <p>文件名即 {@link WorkshopSection} 的 identity（前端生成的 UUID）。子分区
 * {@code SectionChild} 不单独落盘 —— 它们嵌在父 section 的 {@code childSections} 字段里，
 * 随同一个 XML 一起写。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class WorkshopSectionStore extends AbstractWorkshopEntityStore<WorkshopSection> {

    public static final String SECTIONS_DIR_NAME = "sections";

    private static final WorkshopSectionStoreGetter storeGetter = new WorkshopSectionStoreGetter();

    private WorkshopSectionStore(String ontologyDomainId, String moduleName) {
        super(ontologyDomainId, moduleName);
    }

    public static WorkshopSectionStore create(String ontologyDomainId, String moduleName) {
        return new WorkshopSectionStore(ontologyDomainId, moduleName);
    }

    @Override
    protected String getStoreDirName() {
        return SECTIONS_DIR_NAME;
    }

    @Override
    protected BasicWorkshopStoreGetter<WorkshopSection> getStoreGetter() {
        return storeGetter;
    }

    /**
     * 按「父容器 → 父容器内序号 → id」排序。跨父容器比较没有意义，
     * 但稳定的全序能让测试与日志可复现。
     */
    @Override
    public List<WorkshopSection> listAll() {
        List<WorkshopSection> sections = super.listAll();
        sections.sort(Comparator
                .comparing((WorkshopSection s) -> s.parent == null ? "" : s.parent.parentType.name())
                .thenComparing(s -> s.parent == null ? "" : s.parent.parentId)
                .thenComparingInt(s -> s.sortOrder == null ? 0 : s.sortOrder)
                .thenComparing(WorkshopSection::identityValue));
        return sections;
    }

    public static class WorkshopSectionStoreGetter extends BasicWorkshopStoreGetter<WorkshopSection> {

        @Override
        protected Class<WorkshopSection> getPluginType() {
            return WorkshopSection.class;
        }

        @Override
        public java.io.File getAssistRootDir(String ontologyName, java.util.Optional<String> submodule) {
            return new java.io.File(super.getAssistRootDir(ontologyName, submodule), SECTIONS_DIR_NAME);
        }
    }
}
