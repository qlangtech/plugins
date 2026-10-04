//package com.qlangtech.tis.plugin.ontology.workshop.store;
//
//import com.alibaba.citrus.turbine.Context;
//import com.qlangtech.tis.TIS;
//import com.qlangtech.tis.extension.Descriptor;
//import com.qlangtech.tis.extension.impl.XmlFile;
//import com.qlangtech.tis.plugin.IPluginStore;
//import com.qlangtech.tis.plugin.KeyedPluginStore;
//import com.qlangtech.tis.plugin.ontology.OntologyDomain;
//import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopHeader;
//import com.qlangtech.tis.util.IPluginContext;
//import org.apache.commons.lang.StringUtils;
//
//import java.io.File;
//import java.util.Collections;
//import java.util.Objects;
//import java.util.Optional;
//
///**
// * Workshop Header 的独立持久化存储。
// *
// * <h3>为什么 header 需要独立存储</h3>
// * 同 {@link WorkshopVariableStore}：{@code WorkshopModule.header} 被声明为 {@code transient}
// * （子实体与聚合根分开落盘），而 TIS 用 XStream 序列化插件配置且会跳过 transient 字段，
// * 所以 header 无法随模块 XML 一起落盘。
// *
// * <p>与页面/变量不同的是 <b>header 与 module 是 1:1</b>：一个模块只有一份 header 配置，
// * 因此本类没有 {@code listAll()} / {@code delete()} —— 既没有「多个」也没有「删除」的语义，
// * 唯一的写操作是覆盖同一个文件。
// *
// * <h3>存储路径</h3>
// * <pre>
// *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop_headers/{moduleName}.xml
// * </pre>
// *
// * <h3>为什么 moduleName 要放进 Key.keyVal</h3>
// * {@link KeyedPluginStore.Key#hashCode()} 只由 {@code keyVal} 与 {@code pluginClass} 决定，
// * {@link TIS#getPluginStore(KeyedPluginStore.Key)} 又按 hashCode 缓存 store 实例，而 store
// * 的目标文件在构造时就已固定。若只覆盖 {@code getSerializeFileRelativePath()} 而不把
// * moduleName 编进 keyVal，同一 domain 下的所有模块会命中同一个缓存 store，读写全部落到
// * 第一个模块的 header 文件上。详见 {@link WorkshopPageStore} 类注释里记录的同款缺陷。
// *
// * @see WorkshopPageStore
// * @see WorkshopVariableStore
// */
//public class WorkshopHeaderStore {
//
//    /**
//     * header 存储目录名，位于 domain 目录之下
//     */
//    public static final String STORE_DIR_NAME = "workshop_headers";
//
//    /**
//     * 与 {@link OntologyDomain#ONTOLOGY_DOMAIN} 的 identity 一致，即 {@code ontology}
//     */
//    private static final String GROUP_NAME = OntologyDomain.ONTOLOGY_DOMAIN.getIdentity();
//
//    private final String ontologyDomainId;
//
//    private WorkshopHeaderStore(String ontologyDomainId) {
//        this.ontologyDomainId = Objects.requireNonNull(ontologyDomainId,
//                "param ontologyDomainId can not be null");
//    }
//
//    public static WorkshopHeaderStore create(String ontologyDomainId) {
//        if (StringUtils.isEmpty(ontologyDomainId)) {
//            throw new IllegalArgumentException("param ontologyDomainId can not be empty");
//        }
//        return new WorkshopHeaderStore(ontologyDomainId);
//    }
//
//    /**
//     * 存储根目录：{@code ontology/{domain}/workshop_headers}
//     */
//    public File getStoreDir() {
//        return new File(TIS.pluginCfgRoot, getStoreDirRelativePath());
//    }
//
//    /**
//     * 某个模块的 header 配置文件
//     */
//    public File getHeaderFile(String moduleName) {
//        return new File(getStoreDir(), moduleName + XmlFile.KEY_XML_DOT_EXTENSION);
//    }
//
//    /**
//     * 取得某个模块专属的 PluginStore
//     */
//    public IPluginStore<WorkshopHeader> getStore(final String moduleName) {
//        if (StringUtils.isEmpty(moduleName)) {
//            throw new IllegalArgumentException("param moduleName can not be empty");
//        }
//        KeyedPluginStore.Key<WorkshopHeader> key = new KeyedPluginStore.Key<WorkshopHeader>(
//                GROUP_NAME,
//                getStoreDirRelativePath() + File.separator + moduleName,
//                WorkshopHeader.class) {
//            @Override
//            public String getSerializeFileRelativePath() {
//                // keyVal 已含 moduleName，因此 getSubDirPath() 就是完整文件路径（不含 .xml）
//                return this.getSubDirPath();
//            }
//        };
//        return TIS.getPluginStore(key);
//    }
//
//    /**
//     * 加载模块的 header，尚未配置过返回 null
//     * （「未配置」与「配置成了默认值」是两回事，回落默认值由调用方决定）
//     */
//    @SuppressWarnings("all")
//    public WorkshopHeader load(String moduleName) {
//        if (StringUtils.isEmpty(moduleName)) {
//            return null;
//        }
//        if (!getHeaderFile(moduleName).exists()) {
//            return null;
//        }
//        return getStore(moduleName).getPlugin();
//    }
//
//    /**
//     * 加载模块的 header，尚未配置过则回落默认值（visible = true）。
//     * <p>
//     * 「回落」而不是返回 null 是刻意的：前端 {@code module-container} 与编辑器配置面板
//     * 都要求 {@code module.header} 非空才能渲染/编辑，返回 null 会让 header 整块消失。
//     *
//     * <p>这是 header 的<b>唯一</b>读取入口（{@code WorkshopModuleService.getHeader} 与
//     * {@code WorkshopModule.loadHeader} 都走这里），所以 {@link WorkshopHeader#normalize()}
//     * 挂在本方法上即可覆盖全部消费方 —— 它负责给改造前落盘的老 XML 补上缺失的多态字段。
//     */
//    public WorkshopHeader loadOrDefault(String moduleName) {
//        WorkshopHeader header = load(moduleName);
//        return (header != null ? header : WorkshopHeader.defaultOf(moduleName)).normalize();
//    }
//
//    /**
//     * 保存模块的 header（覆盖式，新增与更新都是同一份文件）。
//     *
//     * @param update 与 {@code IPluginStore.setPlugins} 的语义一致，仅影响实例匹配方式，
//     *               1:1 存储下两者都会覆盖同一个文件
//     */
//    public void save(IPluginContext pluginContext, Optional<Context> context,
//                     String moduleName, WorkshopHeader header, boolean update) {
//        Objects.requireNonNull(header, "param header can not be null");
//        if (StringUtils.isEmpty(moduleName)) {
//            throw new IllegalStateException("param moduleName can not be empty");
//        }
//        getStore(moduleName).setPlugins(pluginContext, context,
//                Collections.singletonList(new Descriptor.ParseDescribable<>(header)), update);
//    }
//
//    /**
//     * 存储目录相对于 {@code TIS.pluginCfgRoot} 的路径，同时也是 Key.keyVal 的前缀
//     */
//    private String getStoreDirRelativePath() {
//        return GROUP_NAME + File.separator + ontologyDomainId + File.separator + STORE_DIR_NAME;
//    }
//}
