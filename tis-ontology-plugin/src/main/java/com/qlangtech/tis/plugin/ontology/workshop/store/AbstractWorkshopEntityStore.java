package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.SetPluginsResult;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
 * Workshop 聚合子实体（section / overlay / widget）的独立落盘存储基类。
 *
 * <p>与 {@link WorkshopPageStore} 同一套存储约定：一个实体一个 XML 文件，目录按
 * <b>module</b> 隔离：
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop/{moduleName}/{dirName}/{storageKey}.xml
 * </pre>
 * 编辑器的主保存路径是增量提交（{@code module-apply}）：只对变化的实体调 {@link #save}、
 * 对显式删除的实体调 {@link #delete}。{@link #replaceAll} 保留给「前端没有增量基准」的
 * 兜底整对象快照路径，以内存快照为准逐实体覆盖写，再删除快照里已经不存在的文件。
 * 两个路径都不做单字段 patch 合并。
 *
 * <h3>storageKey 与 identity 的区别</h3>
 * section / overlay / widget 的 identity 现在都是前端生成的 UUID，本身满足
 * {@link ValidatorCommons#pattern_identity}，可以直接当文件名。widget 子类仍覆盖
 * {@link #getStorageKey(String)}：存量 widget 的 identity 是旧版可编辑名称
 * （可能含中文、空格、括号），必须编码成安全且单射的文件名，才能映射回原文件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/22
 */
public abstract class AbstractWorkshopEntityStore<T extends Describable<?> & IdentityName> {

    protected final String ontologyDomainId;
    protected final String moduleName;

    protected AbstractWorkshopEntityStore(String ontologyDomainId, String moduleName) {
        if (StringUtils.isEmpty(ontologyDomainId)) {
            throw new IllegalArgumentException("param ontologyDomainId can not be empty");
        }
        if (StringUtils.isEmpty(moduleName)) {
            throw new IllegalArgumentException("param moduleName can not be empty");
        }
        this.ontologyDomainId = ontologyDomainId;
        this.moduleName = moduleName;
    }

    /**
     * 本存储使用的主目录名（位于 {@code ontology/{domain}/workshop/{moduleName}} 之下）
     */
    protected abstract String getStoreDirName();

    protected abstract BasicWorkshopStoreGetter<T> getStoreGetter();

    /**
     * identity → 落盘文件名（不含扩展名）。默认要求 identity 满足 identity 命名规范，
     * 顺带挡住 {@code ../} 这类路径穿越；需要编码的子类覆盖本方法。
     */
    protected String getStorageKey(String identity) {
        if (StringUtils.isEmpty(identity)) {
            throw new IllegalArgumentException("entity identity can not be empty");
        }
        if (!ValidatorCommons.pattern_identity.matcher(identity).matches()) {
            throw new IllegalArgumentException("illegal entity identity: " + identity);
        }
        return identity;
    }

    /**
     * 存储根目录：{@code ontology/{domain}/workshop/{moduleName}/{dirName}}
     */
    public File getStoreDir() {
        return new File(WorkshopModule.getWorkshopDir(ontologyDomainId, moduleName), getStoreDirName());
    }

    /**
     * 单个实体的配置文件
     */
    public File getEntityFile(String identity) {
        return new File(getStoreDir(), getStorageKey(identity) + XmlFile.KEY_XML_DOT_EXTENSION);
    }

    /**
     * 加载单个实体，不存在返回 {@code null}
     */
    public T load(String identity) {
        if (StringUtils.isEmpty(identity) || !getEntityFile(identity).isFile()) {
            return null;
        }
        return getStoreGetter().load(ontologyDomainId, Optional.of(moduleName), getStorageKey(identity));
    }

    /**
     * 列出本模块下的全部实体（未排序）。子类按自己的排序口径覆盖。
     */
    public List<T> listAll() {
        File storeDir = getStoreDir();
        if (!storeDir.isDirectory()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(getStoreGetter().loadAll(ontologyDomainId, Optional.of(moduleName)));
    }

    /**
     * 保存单个实体。是新增还是更新由文件是否已存在决定 —— 调用方无需跟踪状态。
     */
    public void save(IPluginContext pluginContext, Optional<Context> context, T entity) {
        Objects.requireNonNull(entity, "entity can not be null");
        String identity = entity.identityValue();
        boolean update = getEntityFile(identity).isFile();
        IPluginStore<T> store = getStore(identity);
        SetPluginsResult result = store.setPlugins(pluginContext, context,
                Collections.singletonList(new Descriptor.ParseDescribable<>(entity)), update);
        if (!result.success) {
            // 具体字段错误已由插件框架写入 context，这里只负责中止整批快照保存
            throw new IllegalStateException("save " + entity.getClass().getSimpleName()
                    + " faild, identity:" + identity);
        }
        // 模块实例缓存的是区块/组件的内存树，落盘后必须让它下次读取时整体重建
        WorkshopModule.invalidateWorkshopData(ontologyDomainId, moduleName);
    }

    /**
     * 以给定快照覆盖本模块的全部实体：逐实体覆盖写，然后删除快照中不存在的文件。
     *
     * @return 被删除的实体 identity（快照里已不存在的那些）
     */
    public Set<String> replaceAll(IPluginContext pluginContext, Optional<Context> context, List<T> entities) {
        Objects.requireNonNull(entities, "param entities can not be null");

        Set<String> incomingKeys = new HashSet<>();
        for (int i = 0; i < entities.size(); i++) {
            T entity = entities.get(i);
            Objects.requireNonNull(entity, "entity at index " + i + " can not be null");
            String identity = entity.identityValue();
            if (!incomingKeys.add(getStorageKey(identity))) {
                throw new IllegalStateException("duplicate entity in snapshot: " + identity);
            }
        }

        // 先全部写入，再删差集：写失败时直接抛出，不会留下「已删旧文件、新文件没写进去」的中间态
        for (T entity : entities) {
            save(pluginContext, context, entity);
        }

        return deleteNotInStorageKeys(incomingKeys);
    }

    /**
     * 差集删除：删掉本模块存储目录里 identity 不在 {@code keepIdentities} 中的实体文件。
     * <p>
     * 供「实例自管理写入」链路使用（{@code IStoreSelfManagement#manipuldateProcess} 逐实体写盘后，
     * 由调用方统一清理快照里已经不存在的实体）；{@link #replaceAll} 内部同样走这里。
     *
     * @param keepIdentities 本次快照里出现的实体 identity（不是文件名）
     * @return 被删除的实体 identity
     */
    public Set<String> deleteNotIn(Set<String> keepIdentities) {
        Objects.requireNonNull(keepIdentities, "param keepIdentities can not be null");
        Set<String> keepKeys = new HashSet<>();
        for (String identity : keepIdentities) {
            keepKeys.add(getStorageKey(identity));
        }
        return deleteNotInStorageKeys(keepKeys);
    }

    /**
     * @param keepStorageKeys 已由 {@link #getStorageKey(String)} 转换过的文件名集合
     */
    private Set<String> deleteNotInStorageKeys(Set<String> keepStorageKeys) {
        Set<String> removed = new HashSet<>();
        File storeDir = getStoreDir();
        if (!storeDir.isDirectory()) {
            return removed;
        }
        for (File file : FileUtils.listFiles(storeDir, new String[]{XmlFile.KEY_XML_EXTENSION}, false)) {
            String storageKey = StringUtils.removeEnd(file.getName(), XmlFile.KEY_XML_DOT_EXTENSION);
            if (keepStorageKeys.contains(storageKey)) {
                continue;
            }
            T stale = getStoreGetter().load(ontologyDomainId, Optional.of(moduleName), storageKey);
            String identity = stale != null ? stale.identityValue() : storageKey;
            try {
                FileUtils.forceDelete(file);
            } catch (IOException e) {
                throw new IllegalStateException("delete file:" + file.getAbsolutePath() + " faild", e);
            }
            // 清掉进程内缓存，否则后续 getPlugin() 还会返回已删除的实例
            getStoreByStorageKey(storageKey).cleanPlugins();
            removed.add(identity);
        }
        if (!removed.isEmpty()) {
            WorkshopModule.invalidateWorkshopData(ontologyDomainId, moduleName);
        }
        return removed;
    }

    /**
     * 删除实体，返回是否真的删掉了文件。
     */
    public boolean delete(IPluginContext pluginContext, Optional<Context> context, String identity) {
        File entityFile = getEntityFile(identity);
        boolean deleted = false;
        if (entityFile.isFile()) {
            try {
                FileUtils.forceDelete(entityFile);
                deleted = true;
            } catch (IOException e) {
                throw new IllegalStateException("delete file:" + entityFile.getAbsolutePath() + " faild", e);
            }
        }
        getStore(identity).cleanPlugins();
        WorkshopModule.invalidateWorkshopData(ontologyDomainId, moduleName);
        return deleted;
    }

    private IPluginStore<T> getStore(String identity) {
        return getStoreByStorageKey(getStorageKey(identity));
    }

    /**
     * @param storageKey 已经过 {@link #getStorageKey(String)} 转换的文件名
     */
    private IPluginStore<T> getStoreByStorageKey(String storageKey) {
        if (StringUtils.isEmpty(storageKey)) {
            throw new IllegalArgumentException("param storageKey can not be empty");
        }
        UploadPluginMeta pluginMeta = UploadPluginMeta.create(ONTOLOGY)
                .putExtraParams(KEY_START_PERSISTENCE, Boolean.TRUE.toString())
                .putExtraParams(NAME_ONTOLOGY_DOMAIN, ontologyDomainId)
                .putExtraParams(IdentityName.PLUGIN_IDENTITY_NAME, storageKey);
        return getStoreGetter().getPluginStore(
                OntologyPluginMeta.createPluginMeta(pluginMeta), Optional.of(moduleName));
    }

    /**
     * 构造供自管理实例（{@link com.qlangtech.tis.extension.IStoreSelfManagement}）使用的
     * 插件元数据。实例的 {@code manipuldateProcess} 只依赖 domain + module 两个
     * extraParam，identity 由实例自身提供，无需重复下发。
     */
    public static UploadPluginMeta createSelfManagementMeta(String ontologyDomainId, String moduleName) {
        if (StringUtils.isEmpty(ontologyDomainId)) {
            throw new IllegalArgumentException("param ontologyDomainId can not be empty");
        }
        if (StringUtils.isEmpty(moduleName)) {
            throw new IllegalArgumentException("param moduleName can not be empty");
        }
        return UploadPluginMeta.create(ONTOLOGY)
                .putExtraParams(NAME_ONTOLOGY_DOMAIN, ontologyDomainId)
                .putExtraParams(OntologyDomain.KEY_WORKSHOP, moduleName);
    }

    /**
     * 从标准保存链路的插件元数据里取 ontology 域 ID。
     * <p>
     * 取值约定与 {@link WorkshopVariable#manipuldateProcess} 一致：
     * {@code ontologyDomainId} + {@code workshop}（模块名）两个 extraParam。
     */
    public static String getOntologyDomainId(UploadPluginMeta meta) {
        String ontologyDomainId = meta.getExtraParam(NAME_ONTOLOGY_DOMAIN);
        if (StringUtils.isEmpty(ontologyDomainId)) {
            throw new IllegalStateException("plugin meta extra param '" + NAME_ONTOLOGY_DOMAIN
                    + "' must not be empty");
        }
        return ontologyDomainId;
    }

    /**
     * 从插件元数据里取 workshop 模块名（extraParam key 为 {@code workshop}，
     * 见 {@code WorkshopVariable.PARAM_MODULE_NAME}）。
     */
    public static String getModuleName(UploadPluginMeta meta) {
        String moduleName = meta.getExtraParam(OntologyDomain.KEY_WORKSHOP);
        if (StringUtils.isEmpty(moduleName)) {
            throw new IllegalStateException("plugin meta extra param '" + OntologyDomain.KEY_WORKSHOP
                    + "' must not be empty");
        }
        return moduleName;
    }

    /**
     * widget 的存量 identity 直接当文件名不安全：旧版名称可能含空格、中文、括号
     * （{@code Widget 1}、{@code 对象表格}、{@code xxx (副本)}），而 identity 校验只允许
     * {@code [A-Z\d a-z_-]+}。这里用 URL 编码把它变成单射的文件名；磁盘上的名字不好看，
     * 但解析时只把它当不透明 key，实例里的 {@code id} 仍是持久化的原值。
     * 新数据是 UUID，编码后原样不变。
     */
    protected static String encodeIdentity(String identity) {
        if (StringUtils.isEmpty(identity)) {
            throw new IllegalArgumentException("entity identity can not be empty");
        }
        try {
            // URLEncoder 把 '/' 编码成 %2F、空格编码成 '+'，不含路径分隔符
            return URLEncoder.encode(identity, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
