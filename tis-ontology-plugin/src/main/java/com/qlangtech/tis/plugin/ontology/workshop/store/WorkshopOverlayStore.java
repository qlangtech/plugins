package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Workshop Overlay 的独立落盘存储（一个 overlay 一个文件）。
 *
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop/{moduleName}/overlays/{overlayId}.xml
 * </pre>
 *
 * <p>文件名即 {@link WorkshopOverlay} 的 identity（前端生成的 UUID）。overlay 下的
 * section 不随 overlay XML 走（{@code sections} 是 transient），而是由
 * {@link WorkshopSectionStore} 按 {@code parent.parentType = OVERLAY} 独立落盘。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class WorkshopOverlayStore extends AbstractWorkshopEntityStore<WorkshopOverlay> {

    public static final String OVERLAYS_DIR_NAME = "overlays";

    private static final WorkshopOverlayStoreGetter storeGetter = new WorkshopOverlayStoreGetter();

    private WorkshopOverlayStore(String ontologyDomainId, String moduleName) {
        super(ontologyDomainId, moduleName);
    }

    public static WorkshopOverlayStore create(String ontologyDomainId, String moduleName) {
        return new WorkshopOverlayStore(ontologyDomainId, moduleName);
    }

    @Override
    protected String getStoreDirName() {
        return OVERLAYS_DIR_NAME;
    }

    @Override
    protected BasicWorkshopStoreGetter<WorkshopOverlay> getStoreGetter() {
        return storeGetter;
    }

    @Override
    public List<WorkshopOverlay> listAll() {
        List<WorkshopOverlay> overlays = super.listAll();
        overlays.sort(Comparator
                .comparingInt((WorkshopOverlay o) -> o.sortOrder == null ? 0 : o.sortOrder)
                .thenComparing(WorkshopOverlay::identityValue));
        return overlays;
    }

    public static class WorkshopOverlayStoreGetter extends BasicWorkshopStoreGetter<WorkshopOverlay> {

        @Override
        protected Class<WorkshopOverlay> getPluginType() {
            return WorkshopOverlay.class;
        }

        @Override
        public File getAssistRootDir(String ontologyName, Optional<String> submodule) {
            return new File(super.getAssistRootDir(ontologyName, submodule), OVERLAYS_DIR_NAME);
        }
    }
}
