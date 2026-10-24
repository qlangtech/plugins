package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Workshop Widget 的独立落盘存储（一个 widget 一个文件）。
 *
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop/{moduleName}/widgets/{encodedName}.xml
 * </pre>
 *
 * <p>widget 的 identity 现在是前端创建时生成的 UUID {@code id}（与 page / section /
 * overlay 同口径）。文件名仍走 URL 编码（见 {@link #getStorageKey(String)}）：
 * UUID 编码后原样不变；保留编码是为了兼容存量数据 —— 旧版 widget 的 identity 是
 * 用户可编辑的 {@code name}，可能含中文、空格、括号（如 {@code Widget 1}、
 * {@code xxx (副本)}），既不能直接拼进文件路径，也必须映射回原来的文件名。
 *
 * <p>widget 与 section 的归属由 {@link WorkshopWidget#sectionId} 表达，
 * 加载后由模块重建树时按该字段挂回。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public class WorkshopWidgetStore extends AbstractWorkshopEntityStore<WorkshopWidget> {

    public static final String WIDGETS_DIR_NAME = "widgets";

    static {
        // 存量 XML 兼容：WorkshopWidget 的 id 字段原名 name，旧 widget 文件的元素名是 <name>。
        // TIS 的 XStream2 未开启注解自动处理，@XStreamAlias 不生效，只能在共享实例上显式注册。
        // FieldAliasingMapper 沿父类链查找别名，注册一次即覆盖全部具体 Widget 子类；读写双向生效。
        XmlFile.DEFAULT_XSTREAM.aliasField("name", WorkshopWidget.class, "id");
    }

    private static final WorkshopWidgetStoreGetter storeGetter = new WorkshopWidgetStoreGetter();

    private WorkshopWidgetStore(String ontologyDomainId, String moduleName) {
        super(ontologyDomainId, moduleName);
    }

    public static WorkshopWidgetStore create(String ontologyDomainId, String moduleName) {
        return new WorkshopWidgetStore(ontologyDomainId, moduleName);
    }

    @Override
    protected String getStoreDirName() {
        return WIDGETS_DIR_NAME;
    }

    @Override
    protected BasicWorkshopStoreGetter<WorkshopWidget> getStoreGetter() {
        return storeGetter;
    }

    /**
     * 新数据是 UUID，编码后原样不变；存量 identity 可能含中文 / 空格 / 括号，
     * 不能直接当文件名 —— 统一编码成安全且单射的 key。
     */
    @Override
    protected String getStorageKey(String identity) {
        return encodeIdentity(identity);
    }

    /**
     * 按「所属 section → section 内序号 → name」排序
     */
    @Override
    public List<WorkshopWidget> listAll() {
        List<WorkshopWidget> widgets = super.listAll();
        widgets.sort(Comparator
                .comparing((WorkshopWidget w) -> w.sectionId == null ? "" : w.sectionId)
                .thenComparingInt(w -> w.sortOrder == null ? 0 : w.sortOrder)
                .thenComparing(WorkshopWidget::identityValue));
        return widgets;
    }

    public static class WorkshopWidgetStoreGetter extends BasicWorkshopStoreGetter<WorkshopWidget> {

        @Override
        protected Class<WorkshopWidget> getPluginType() {
            return WorkshopWidget.class;
        }

        @Override
        public File getAssistRootDir(String ontologyName, Optional<String> submodule) {
            return new File(super.getAssistRootDir(ontologyName, submodule), WIDGETS_DIR_NAME);
        }
    }
}
