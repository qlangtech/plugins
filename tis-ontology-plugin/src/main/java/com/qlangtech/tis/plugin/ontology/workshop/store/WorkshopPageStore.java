package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.SetPluginsResult;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.qlangtech.tis.plugin.ontology.Ontology.ONTOLOGY;
import static com.qlangtech.tis.plugin.ontology.OntologyDomain.NAME_ONTOLOGY_DOMAIN;
import static com.qlangtech.tis.plugin.ontology.OntologyValueType.KEY_START_PERSISTENCE;

/**
 * Workshop Page 的独立持久化存储（一个页面一个文件）。
 *
 * <h3>存储路径</h3>
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop/{moduleName}/pages/{pageId}.xml
 * </pre>
 * 目录按 <b>module</b> 隔离 —— 页面是模块的聚合子实体，不能跨模块共享。
 * 文件名即 {@link WorkshopPage#id}（前端建页时生成的 UUID），因此改名不影响落盘位置。
 *
 * <h3>为什么不用旧的 Key.keyVal 方案</h3>
 * 旧实现（已注释的 {@code WorkshopPageStore}）把 pageId 只编进文件路径、keyVal 恒为
 * domain 级目录，而 {@code TIS.getPluginStore(Key)} 是按 {@code keyVal + pluginClass}
 * 的 hashCode 缓存的 —— 同 domain 下所有页面会命中同一个 store，文件被锁死在首次构造时
 * 的 pageId 上。这里改走 {@link BasicWorkshopStoreGetter}：keyVal 里的 pluginId 就是
 * pageId（见 {@code BaiscAssistStoreGetter#getPluginStore}），每个页面一个独立 store。
 *
 * <h3>写入语义</h3>
 * 编辑器的主保存路径是<b>增量提交</b>（{@code module-apply}）：只对变化的页面调
 * {@link #save}、对显式删除的页面调 {@link #delete}，未变化的页面文件原样保留。
 * {@link #replaceAll} 仍保留给「前端没有增量基准」的兜底整对象快照路径 —— 它按快照
 * 数组顺序归一化 {@code sortOrder} 并做差集删除。
 * <p>
 * 两个路径都不做单字段 patch 合并 —— patch 流需要服务端 read-modify-write，
 * 会把合并顺序、并发和校验问题引进来。
 */
public class WorkshopPageStore {

    /**
     * 页面存储目录名，位于 {@code ontology/{domain}/workshop/{moduleName}} 之下
     */
    public static final String PAGES_DIR_NAME = "pages";

    private static final WorkshopPageStoreGetter storeGetter = new WorkshopPageStoreGetter();

    private final String ontologyDomainId;
    private final String moduleName;

    private WorkshopPageStore(String ontologyDomainId, String moduleName) {
        this.ontologyDomainId = ontologyDomainId;
        this.moduleName = moduleName;
    }

    public static WorkshopPageStore create(String ontologyDomainId, String moduleName) {
        if (StringUtils.isEmpty(ontologyDomainId)) {
            throw new IllegalArgumentException("param ontologyDomainId can not be empty");
        }
        if (StringUtils.isEmpty(moduleName)) {
            throw new IllegalArgumentException("param moduleName can not be empty");
        }
        return new WorkshopPageStore(ontologyDomainId, moduleName);
    }

    /**
     * 存储根目录：{@code ontology/{domain}/workshop/{moduleName}/pages}
     */
    public File getStoreDir() {
        return new File(WorkshopModule.getWorkshopDir(ontologyDomainId, moduleName), PAGES_DIR_NAME);
    }

    /**
     * 单个页面的配置文件
     */
    public File getPageFile(String pageId) {
        if (StringUtils.isEmpty(pageId)) {
            throw new IllegalArgumentException("param pageId can not be empty");
        }
        // pageId 直接拼进文件名：与 @FormField(validate = identity) 同一口径，
        // 顺带挡住 "../" 这类路径穿越，避免保存接口写到模块目录之外
        if (!ValidatorCommons.pattern_identity.matcher(pageId).matches()) {
            throw new IllegalArgumentException("illegal pageId: " + pageId);
        }
        return new File(getStoreDir(), pageId + XmlFile.KEY_XML_DOT_EXTENSION);
    }

    /**
     * 加载单个页面，不存在返回 {@code null}
     */
    public WorkshopPage load(String pageId) {
        if (StringUtils.isEmpty(pageId) || !getPageFile(pageId).isFile()) {
            return null;
        }
        return storeGetter.load(ontologyDomainId, Optional.of(moduleName), pageId);
    }

    /**
     * 列出本模块下的全部页面，按 {@code sortOrder} 升序（再按 id 稳定排序）
     */
    public List<WorkshopPage> listAll() {
        File storeDir = getStoreDir();
        if (!storeDir.isDirectory()) {
            return Collections.emptyList();
        }
        List<WorkshopPage> pages = new ArrayList<>(
                storeGetter.loadAll(ontologyDomainId, Optional.of(moduleName)));
        pages.sort((a, b) -> {
            int ao = a.sortOrder != null ? a.sortOrder : 0;
            int bo = b.sortOrder != null ? b.sortOrder : 0;
            if (ao != bo) {
                return Integer.compare(ao, bo);
            }
            return String.valueOf(a.id).compareTo(String.valueOf(b.id));
        });
        return pages;
    }

    /**
     * 保存单个页面。是新增还是更新由文件是否已存在决定 —— 调用方无需跟踪状态。
     */
    public void save(IPluginContext pluginContext, Optional<Context> context, WorkshopPage page) {
        Objects.requireNonNull(page, "param page can not be null");
        String pageId = page.identityValue();
        boolean update = getPageFile(pageId).isFile();
        IPluginStore<WorkshopPage> store = getStore(pageId);
        SetPluginsResult result = store.setPlugins(pluginContext, context,
                Collections.singletonList(new Descriptor.ParseDescribable<>(page)), update);
        if (!result.success) {
            // 具体的字段校验错误已由插件框架写入 context，这里只负责中止整批快照保存
            throw new IllegalStateException("save workshop page faild, pageId:" + pageId);
        }
        // 模块实例缓存的是页面/区块/组件的内存树，落盘后必须让它下次读取时整体重建
        WorkshopModule.invalidateWorkshopData(ontologyDomainId, moduleName);
    }

    /**
     * 以给定快照覆盖本模块的全部页面：逐页按 id 覆盖写，然后删除快照中不存在的文件。
     *
     * <p>这是编辑器「保存」的唯一写入口。相比逐字段 patch 提交，快照写是幂等的：
     * 重复保存同一份快照结果相同，中途失败重试也不会把字段合并错。
     *
     * @return 被删除的页面 id（快照里已不存在的那些）
     */
    public Set<String> replaceAll(IPluginContext pluginContext, Optional<Context> context,
                                  List<WorkshopPage> pages) {
        Objects.requireNonNull(pages, "param pages can not be null");

        Set<String> incomingIds = new HashSet<>();
        for (int i = 0; i < pages.size(); i++) {
            WorkshopPage page = pages.get(i);
            Objects.requireNonNull(page, "page at index " + i + " can not be null");
            // sortOrder 以快照数组顺序为准，服务端归一化，避免前端传来空洞或重复序号
            page.sortOrder = i;
            String pageId = page.identityValue();
            if (!incomingIds.add(pageId)) {
                throw new IllegalStateException("duplicate page id in snapshot: " + pageId);
            }
        }

        // 先全部写入，再删差集：写失败时直接抛出，不会留下「已删旧页、新页没写进去」的中间态
        for (WorkshopPage page : pages) {
            save(pluginContext, context, page);
        }

        Set<String> removed = new HashSet<>();
        File storeDir = getStoreDir();
        if (storeDir.isDirectory()) {
            for (File file : FileUtils.listFiles(storeDir, new String[]{XmlFile.KEY_XML_EXTENSION}, false)) {
                String pageId = StringUtils.removeEnd(file.getName(), XmlFile.KEY_XML_DOT_EXTENSION);
                if (!incomingIds.contains(pageId)) {
                    delete(pluginContext, context, pageId);
                    removed.add(pageId);
                }
            }
        }
        return removed;
    }

    /**
     * 删除页面，返回是否真的删掉了文件。
     */
    public boolean delete(IPluginContext pluginContext, Optional<Context> context, String pageId) {
        File pageFile = getPageFile(pageId);
        boolean deleted = false;
        if (pageFile.isFile()) {
            try {
                FileUtils.forceDelete(pageFile);
                deleted = true;
            } catch (IOException e) {
                throw new IllegalStateException("delete page file:" + pageFile.getAbsolutePath() + " faild", e);
            }
        }
        // 清掉进程内缓存，否则后续 getPlugin() 还会返回已删除的实例
        getStore(pageId).cleanPlugins();
        WorkshopModule.invalidateWorkshopData(ontologyDomainId, moduleName);
        return deleted;
    }

    private IPluginStore<WorkshopPage> getStore(String pageId) {
        if (StringUtils.isEmpty(pageId)) {
            throw new IllegalArgumentException("param pageId can not be empty");
        }
        UploadPluginMeta pluginMeta = UploadPluginMeta.create(ONTOLOGY)
                .putExtraParams(KEY_START_PERSISTENCE, Boolean.TRUE.toString())
                .putExtraParams(NAME_ONTOLOGY_DOMAIN, ontologyDomainId)
                .putExtraParams(IdentityName.PLUGIN_IDENTITY_NAME, pageId);
        return storeGetter.getPluginStore(
                OntologyPluginMeta.createPluginMeta(pluginMeta), Optional.of(moduleName));
    }

    /**
     * {@link BasicWorkshopStoreGetter} 的页面版：目录追加 {@value #PAGES_DIR_NAME} 子目录，
     * {@code pluginClass} 换成 {@link WorkshopPage}（父类默认是 Ontology，会导致
     * {@code setPlugins} 的类型校验失败）。
     */
    public static class WorkshopPageStoreGetter extends BasicWorkshopStoreGetter<WorkshopPage> {

        @Override
        protected Class<WorkshopPage> getPluginType() {
            return WorkshopPage.class;
        }

        @Override
        public File getAssistRootDir(String ontologyName, Optional<String> submodule) {
            return new File(super.getAssistRootDir(ontologyName, submodule), PAGES_DIR_NAME);
        }
    }
}
