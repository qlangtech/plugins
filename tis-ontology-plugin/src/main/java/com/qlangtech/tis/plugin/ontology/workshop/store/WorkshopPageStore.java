package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Workshop Page 的独立持久化存储。
 *
 * <h3>为什么页面需要独立存储</h3>
 * 同 {@link WorkshopVariableStore}：{@code WorkshopPage.sections} 是 transient，
 * 页面与模块分开落盘。页面按 <b>domain</b> 归属（不按模块），故本类没有 moduleName 维度。
 *
 * <h3>存储路径</h3>
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop_pages/{pageId}.xml
 * </pre>
 *
 * <h3>为什么 pageId 必须放进 Key.keyVal（本类的主要存在理由）</h3>
 * 本类取代了 {@code WorkshopModule.DefaultDescriptor} 里原先的 4 个私有页面方法。那些方法
 * 把 pageId <b>只编进了文件路径</b>（覆盖 {@code getSerializeFileRelativePath()}），
 * 而 keyVal 恒为 {@code domain + "/workshop_pages"}，于是：
 *
 * <ol>
 *   <li>{@link KeyedPluginStore.Key#hashCode()} 只由 {@code keyVal} 与 {@code pluginClass}
 *       决定，与 pageId 无关；</li>
 *   <li>{@link TIS#getPluginStore(KeyedPluginStore.Key)} 按该 hashCode 缓存 store 实例，
 *       而 store 的目标文件在<b>构造时</b>就已固定。</li>
 * </ol>
 *
 * 结果是同一 domain 下的所有页面命中同一个缓存 store，文件被锁死在首次构造时的那个
 * pageId 上 —— {@code listAll()} 会把同一个页面重复返回 N 次（N = 目录里的 xml 个数）。
 * 把 pageId 编进 keyVal 后每个页面才拿到各自的 store，与
 * {@link WorkshopVariableStore} 的做法一致。
 *
 * @see WorkshopVariableStore
 */
public class WorkshopPageStore {

    /**
     * 页面存储目录名，位于 domain 目录之下
     */
    public static final String STORE_DIR_NAME = "workshop_pages";

    /**
     * 与 {@link OntologyDomain#ONTOLOGY_DOMAIN} 的 identity 一致，即 {@code ontology}
     */
    private static final String GROUP_NAME = OntologyDomain.ONTOLOGY_DOMAIN.getIdentity();

    private final String ontologyDomainId;

    private WorkshopPageStore(String ontologyDomainId) {
        this.ontologyDomainId = Objects.requireNonNull(ontologyDomainId,
                "param ontologyDomainId can not be null");
    }

    public static WorkshopPageStore create(String ontologyDomainId) {
        if (StringUtils.isEmpty(ontologyDomainId)) {
            throw new IllegalArgumentException("param ontologyDomainId can not be empty");
        }
        return new WorkshopPageStore(ontologyDomainId);
    }

    /**
     * 存储根目录：{@code ontology/{domain}/workshop_pages}
     */
    public File getStoreDir() {
        return new File(TIS.pluginCfgRoot, getStoreDirRelativePath());
    }

    /**
     * 单个页面的配置文件
     */
    public File getPageFile(String pageId) {
        return new File(getStoreDir(), pageId + XmlFile.KEY_XML_DOT_EXTENSION);
    }

    /**
     * 取得某个页面专属的 PluginStore
     */
    public IPluginStore<WorkshopPage> getStore(final String pageId) {
        if (StringUtils.isEmpty(pageId)) {
            throw new IllegalArgumentException("param pageId can not be empty");
        }
        KeyedPluginStore.Key<WorkshopPage> key = new KeyedPluginStore.Key<WorkshopPage>(
                GROUP_NAME,
                getStoreDirRelativePath() + File.separator + pageId,
                WorkshopPage.class) {
            @Override
            public String getSerializeFileRelativePath() {
                // keyVal 已含 pageId，因此 getSubDirPath() 就是完整文件路径（不含 .xml）
                return this.getSubDirPath();
            }
        };
        return TIS.getPluginStore(key);
    }

    /**
     * 加载单个页面，不存在返回 null
     */
    @SuppressWarnings("all")
    public WorkshopPage load(String pageId) {
        if (StringUtils.isEmpty(pageId)) {
            return null;
        }
        if (!getPageFile(pageId).exists()) {
            return null;
        }
        return getStore(pageId).getPlugin();
    }

    /**
     * 列出 domain 下的全部页面，按 sortOrder 升序
     */
    public List<WorkshopPage> listAll() {
        File storeDir = getStoreDir();
        if (!storeDir.exists()) {
            return Collections.emptyList();
        }
        List<WorkshopPage> pages = new ArrayList<>();
        for (File f : FileUtils.listFiles(storeDir, new String[]{XmlFile.KEY_XML_EXTENSION}, false)) {
            String pageId = StringUtils.removeEnd(f.getName(), XmlFile.KEY_XML_DOT_EXTENSION);
            WorkshopPage page = load(pageId);
            if (page != null) {
                pages.add(page);
            }
        }
        pages.sort((a, b) -> {
            int ao = a.sortOrder != null ? a.sortOrder : 0;
            int bo = b.sortOrder != null ? b.sortOrder : 0;
            return Integer.compare(ao, bo);
        });
        return pages;
    }

    /**
     * 保存页面（新增或更新）。文件路径由 {@code page.id} 决定，因此 id 不能为空。
     */
    public void save(IPluginContext pluginContext, Optional<Context> context,
                     WorkshopPage page, boolean update) {
        Objects.requireNonNull(page, "param page can not be null");
        if (StringUtils.isEmpty(page.getId())) {
            throw new IllegalStateException("page id can not be empty, page name:" + page.name);
        }
        getStore(page.getId()).setPlugins(pluginContext, context,
                Collections.singletonList(new Descriptor.ParseDescribable<>(page)), update);
    }

    /**
     * 删除页面，返回是否真的删掉了文件
     */
    public boolean delete(String pageId) {
        File pageFile = getPageFile(pageId);
        boolean deleted = false;
        if (pageFile.exists()) {
            try {
                FileUtils.forceDelete(pageFile);
                deleted = true;
            } catch (IOException e) {
                throw new IllegalStateException("delete page file:" + pageFile.getAbsolutePath()
                        + " faild", e);
            }
        }
        // 清掉进程内缓存，否则后续 getPlugin() 还会返回已删除的实例
        getStore(pageId).cleanPlugins();
        return deleted;
    }

    /**
     * 存储目录相对于 {@code TIS.pluginCfgRoot} 的路径，同时也是 Key.keyVal 的前缀
     */
    private String getStoreDirRelativePath() {
        return GROUP_NAME + File.separator + ontologyDomainId + File.separator + STORE_DIR_NAME;
    }
}
