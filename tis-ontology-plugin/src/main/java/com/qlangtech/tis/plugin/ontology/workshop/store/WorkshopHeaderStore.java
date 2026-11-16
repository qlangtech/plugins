package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.SetPluginsResult;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopHeader;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import static com.qlangtech.tis.plugin.ontology.Ontology.ONTOLOGY;
import static com.qlangtech.tis.plugin.ontology.OntologyDomain.NAME_ONTOLOGY_DOMAIN;
import static com.qlangtech.tis.plugin.ontology.OntologyValueType.KEY_START_PERSISTENCE;

/**
 * Workshop Header 的独立持久化存储。
 *
 * <h3>存储路径</h3>
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop/{moduleName}/header/{moduleName}.xml
 * </pre>
 * 目录按 <b>module</b> 隔离，文件名恒为模块名。
 *
 * <h3>为什么 header 必须独立落盘</h3>
 * {@code WorkshopModule.header} 是 {@code transient} 字段（同 {@code pages}/{@code variables}），
 * 而 TIS 用 XStream 序列化插件配置、会跳过 transient 字段，所以 header 无法随模块 XML 一起保存。
 *
 * <p>与页面/变量的区别：header 与 module 是 <b>1:1</b>，既没有「多个」也没有「删除」的语义，
 * 唯一的写操作是覆盖同一个文件，因此本类没有 {@code listAll()} / {@code delete()}。
 *
 * <h3>为什么不用旧的 Key.keyVal 方案</h3>
 * 旧实现（本文件改造前的注释版本）把 moduleName 编进 {@code Key.keyVal} 以避开
 * {@code TIS.getPluginStore(Key)} 的 hashCode 缓存碰撞。这里改走
 * {@link BasicWorkshopStoreGetter}：keyVal 里的 pluginId 就是 moduleName（见
 * {@code BaiscAssistStoreGetter#getPluginStore}），同样是每个模块一个独立 store，
 * 但与页面/变量共用同一套落盘约定。
 *
 * <p>{@code getPluginType()} 必须覆盖：父类默认返回 {@code ONTOLOGY.extensionPoint}，
 * 而 {@code PluginStore.setPlugins} 会拿它做
 * {@code pluginClass.isAssignableFrom(instance.getClass())} 校验，{@link WorkshopHeader}
 * 不在 Ontology 的继承链上，沿用父类的值会直接抛「plugin must be type of …Ontology」。
 *
 * @see WorkshopPageStore
 */
public class WorkshopHeaderStore {

    /**
     * header 存储目录名，位于 {@code ontology/{domain}/workshop/{moduleName}} 之下
     */
    public static final String HEADER_DIR_NAME = "header";

    static {
        // 存量 XML 兼容：collapseConfig 已从 WorkshopHeader 迁入 VerticalOrientation
        // （折叠语义只在垂直侧栏下成立，水平 + 可折叠渲染出来必坏），但已落盘的 header 文件里
        // 仍有 <collapseConfig> 元素。不 omit 的话 handleUnknownField → UnknownFieldException
        // （XStreamException，不落进 ReadError，PluginStore.processError 拦不住），
        // 整个模块加载直接失败 —— 与 WorkshopOverlayStore 的 type/title 同一套兜底。
        // omitField 顺带阻止该元素被重新写回；老配置里的「水平 + 可折叠」静默丢弃一次
        // （本就渲染崩坏，无实际损失），vertical 的折叠兜底由 VerticalOrientation.readResolve 补。
        XmlFile.DEFAULT_XSTREAM.omitField(WorkshopHeader.class, "collapseConfig");
    }

    private static final WorkshopHeaderStoreGetter storeGetter = new WorkshopHeaderStoreGetter();

    private final String ontologyDomainId;
    private final String moduleName;

    private WorkshopHeaderStore(String ontologyDomainId, String moduleName) {
        this.ontologyDomainId = Objects.requireNonNull(ontologyDomainId,
                "param ontologyDomainId can not be null");
        this.moduleName = Objects.requireNonNull(moduleName, "param moduleName can not be null");
    }

    public static WorkshopHeaderStore create(String ontologyDomainId, String moduleName) {
        if (StringUtils.isEmpty(ontologyDomainId)) {
            throw new IllegalArgumentException("param ontologyDomainId can not be empty");
        }
        if (StringUtils.isEmpty(moduleName)) {
            throw new IllegalArgumentException("param moduleName can not be empty");
        }
        return new WorkshopHeaderStore(ontologyDomainId, moduleName);
    }

    /**
     * 存储根目录：{@code ontology/{domain}/workshop/{moduleName}/header}
     */
    public File getStoreDir() {
        return new File(WorkshopModule.getWorkshopDir(ontologyDomainId, moduleName), HEADER_DIR_NAME);
    }

    /**
     * 本模块的 header 配置文件
     */
    public File getHeaderFile() {
        // moduleName 直接拼进文件名：与 @FormField(validate = identity) 同一口径，
        // 顺带挡住 "../" 这类路径穿越，避免保存接口写到模块目录之外
        if (!ValidatorCommons.pattern_identity.matcher(moduleName).matches()) {
            throw new IllegalArgumentException("illegal moduleName: " + moduleName);
        }
        return new File(getStoreDir(), moduleName + XmlFile.KEY_XML_DOT_EXTENSION);
    }

    /**
     * 加载本模块的 header；尚未配置过返回 {@code null}
     * （「未配置」与「配置成了默认值」是两回事，回落默认值由 {@link #loadOrDefault()} 决定）
     */
    public WorkshopHeader load() {
        if (!getHeaderFile().isFile()) {
            return null;
        }
        return storeGetter.load(ontologyDomainId, Optional.of(moduleName), moduleName);
    }

    /**
     * 加载本模块的 header，尚未配置过则回落默认值（visible = true）。
     *
     * <p>「回落」而不是返回 null 是刻意的：前端运行态容器与编辑器配置面板都要求
     * {@code module.header} 非空才能渲染/编辑，返回 null 会让页头整块消失。
     *
     * <p>这是 header 的<b>唯一</b>读取入口（{@code WorkshopModuleService.getHeader} 与
     * {@code WorkshopModule.loadHeader} 都走这里），所以 {@link WorkshopHeader#normalize()}
     * 挂在本方法上即可覆盖全部消费方 —— 它负责给多态字段缺失的老 XML 补上默认实现。
     */
    public WorkshopHeader loadOrDefault() {
        WorkshopHeader header = load();
        return (header != null ? header : WorkshopHeader.defaultOf(moduleName)).normalize();
    }

    /**
     * 覆盖保存本模块的 header（1:1 存储，新增与更新都写同一个文件）。
     *
     * <p>{@code id} 在这里强制覆写成 moduleName：它是落盘文件名，一旦被表单改动就会写到
     * 别的模块的文件上。覆写放在存储层而不是每个调用方，是为了让绕过表单的调用
     * （单测、脚本）也不出错。
     */
    public WorkshopHeader save(IPluginContext pluginContext, Optional<Context> context, WorkshopHeader header) {
        Objects.requireNonNull(header, "param header can not be null");
        header.id = moduleName;
        boolean update = getHeaderFile().isFile();
        IPluginStore<WorkshopHeader> store = getStore();
        SetPluginsResult result = store.setPlugins(pluginContext, context,
                Collections.singletonList(new com.qlangtech.tis.extension.Descriptor.ParseDescribable<>(header)), update);
        if (!result.success) {
            // 具体的字段校验错误已由插件框架写入 context
            throw new IllegalStateException("save workshop header faild, moduleName:" + moduleName);
        }
        // 模块实例缓存的 header 是加载时回填的，写后必须让它下次读取时整体重建
        WorkshopModule.invalidateWorkshopData(ontologyDomainId, moduleName);
        return header;
    }

    private IPluginStore<WorkshopHeader> getStore() {
        UploadPluginMeta pluginMeta = UploadPluginMeta.create(ONTOLOGY)
                .putExtraParams(KEY_START_PERSISTENCE, Boolean.TRUE.toString())
                .putExtraParams(NAME_ONTOLOGY_DOMAIN, ontologyDomainId)
                .putExtraParams(IdentityName.PLUGIN_IDENTITY_NAME, moduleName);
        return storeGetter.getPluginStore(
                OntologyPluginMeta.createPluginMeta(pluginMeta), Optional.of(moduleName));
    }

    /**
     * {@link BasicWorkshopStoreGetter} 的 header 版：目录追加 {@value #HEADER_DIR_NAME}
     * 子目录，{@code pluginClass} 换成 {@link WorkshopHeader}。
     */
    public static class WorkshopHeaderStoreGetter extends BasicWorkshopStoreGetter<WorkshopHeader> {

        @Override
        protected Class<WorkshopHeader> getPluginType() {
            return WorkshopHeader.class;
        }

        @Override
        public File getAssistRootDir(String ontologyName, Optional<String> submodule) {
            return new File(super.getAssistRootDir(ontologyName, submodule), HEADER_DIR_NAME);
        }
    }
}
