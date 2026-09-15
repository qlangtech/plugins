package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
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
 * Workshop Variable 的独立持久化存储。
 *
 * <h3>为什么变量需要独立存储</h3>
 * {@code WorkshopModule.variables} 被声明为 {@code transient}（子实体与聚合根分开落盘），
 * 而 TIS 用 XStream 序列化插件配置且会跳过 transient 字段，所以变量无法随模块 XML 一起落盘。
 * 页面（{@link com.qlangtech.tis.plugin.ontology.workshop.WorkshopPage}）走的是同一套方案，
 * 本类与之平行，区别只是多了一层 moduleName 目录：变量名只在模块内唯一，
 * 不同模块的同名变量必须互不干扰。
 *
 * <h3>存储路径</h3>
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop_variables/{moduleName}/{variableId}.xml
 * </pre>
 * 每个变量一个文件、文件内只存一个插件实例，因此单条变更不会重写同模块的其他变量。
 *
 * <h3>为什么 variableId 要放进 Key.keyVal</h3>
 * {@link KeyedPluginStore.Key#hashCode()} 只由 {@code keyVal} 与 {@code pluginClass} 决定，
 * {@link TIS#getPluginStore(KeyedPluginStore.Key)} 又按 hashCode 缓存 store 实例，而 store 的
 * 目标文件在构造时就已固定。若像页面那样只覆盖 {@code getSerializeFileRelativePath()}，
 * 同一模块下的所有变量会命中同一个缓存 store，后续读写全部落到第一个变量的文件上。
 * 把 variableId 编进 keyVal（而非只编进文件路径）才能让每个变量拿到各自的 store。
 *
 * @see com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule.DefaultDescriptor#getPagePluginStore(String, String)
 */
public class WorkshopVariableStore {

    /**
     * 变量存储目录名，位于 domain 目录之下
     */
    public static final String STORE_DIR_NAME = "workshop_variables";

    /**
     * 与 {@link OntologyDomain#ONTOLOGY_DOMAIN} 的 identity 一致，即 {@code ontology}
     */
    private static final String GROUP_NAME = OntologyDomain.ONTOLOGY_DOMAIN.getIdentity();

    private final String ontologyDomainId;
    private final String moduleName;

    private WorkshopVariableStore(String ontologyDomainId, String moduleName) {
        this.ontologyDomainId = Objects.requireNonNull(ontologyDomainId,
                "param ontologyDomainId can not be null");
        this.moduleName = Objects.requireNonNull(moduleName, "param moduleName can not be null");
    }

    public static WorkshopVariableStore create(String ontologyDomainId, String moduleName) {
        if (StringUtils.isEmpty(ontologyDomainId)) {
            throw new IllegalArgumentException("param ontologyDomainId can not be empty");
        }
        if (StringUtils.isEmpty(moduleName)) {
            throw new IllegalArgumentException("param moduleName can not be empty");
        }
        return new WorkshopVariableStore(ontologyDomainId, moduleName);
    }

    /**
     * 存储根目录：{@code ontology/{domain}/workshop_variables/{moduleName}}
     */
    public File getStoreDir() {
        return new File(TIS.pluginCfgRoot, getStoreDirRelativePath());
    }

    /**
     * 单个变量的配置文件
     */
    public File getVariableFile(String variableId) {
        return new File(getStoreDir(), variableId + XmlFile.KEY_XML_DOT_EXTENSION);
    }

    /**
     * 取得某个变量专属的 PluginStore
     */
    public IPluginStore<WorkshopVariable> getStore(final String variableId) {
        if (StringUtils.isEmpty(variableId)) {
            throw new IllegalArgumentException("param variableId can not be empty");
        }
        KeyedPluginStore.Key<WorkshopVariable> key = new KeyedPluginStore.Key<WorkshopVariable>(
                GROUP_NAME,
                getStoreDirRelativePath() + File.separator + variableId,
                WorkshopVariable.class) {
            @Override
            public String getSerializeFileRelativePath() {
                // keyVal 已含 variableId，因此 getSubDirPath() 就是完整文件路径（不含 .xml）
                return this.getSubDirPath();
            }
        };
        return TIS.getPluginStore(key);
    }

    /**
     * 加载单个变量，不存在返回 null
     */
    @SuppressWarnings("all")
    public WorkshopVariable load(String variableId) {
        if (StringUtils.isEmpty(variableId)) {
            return null;
        }
        if (!getVariableFile(variableId).exists()) {
            return null;
        }
        return getStore(variableId).getPlugin();
    }

    /**
     * 加载模块下的全部变量
     */
    public List<WorkshopVariable> listAll() {
        File storeDir = getStoreDir();
        if (!storeDir.exists()) {
            return Collections.emptyList();
        }
        List<WorkshopVariable> variables = new ArrayList<>();
        for (File f : FileUtils.listFiles(storeDir, new String[]{XmlFile.KEY_XML_EXTENSION}, false)) {
            String variableId = StringUtils.removeEnd(f.getName(), XmlFile.KEY_XML_DOT_EXTENSION);
            WorkshopVariable variable = load(variableId);
            if (variable != null) {
                variables.add(variable);
            }
        }
        return variables;
    }

    /**
     * 按变量名查找（大小写不敏感），用于模块内唯一性校验
     */
    public WorkshopVariable findByName(String name) {
        if (StringUtils.isEmpty(name)) {
            return null;
        }
        for (WorkshopVariable variable : listAll()) {
            if (name.equalsIgnoreCase(variable.getName())) {
                return variable;
            }
        }
        return null;
    }

    /**
     * 保存变量（新增或更新）。文件路径由 {@code variable.id} 决定，因此 id 不能为空。
     */
    public void save(IPluginContext pluginContext, Optional<Context> context,
                     WorkshopVariable variable, boolean update) {
        Objects.requireNonNull(variable, "param variable can not be null");
        if (StringUtils.isEmpty(variable.getId())) {
            throw new IllegalStateException("variable id can not be empty, variable name:" + variable.getName());
        }
        IPluginStore<WorkshopVariable> store = getStore(variable.getId());
        store.setPlugins(pluginContext, context,
                Collections.singletonList(new Descriptor.ParseDescribable<>(variable)), update);
    }

    /**
     * 删除变量，返回是否真的删掉了文件
     */
    public boolean delete(IPluginContext pluginContext, Optional<Context> context, String variableId) {
        File variableFile = getVariableFile(variableId);
        boolean deleted = false;
        if (variableFile.exists()) {
            try {
                FileUtils.forceDelete(variableFile);
                deleted = true;
            } catch (IOException e) {
                throw new IllegalStateException("delete variable file:" + variableFile.getAbsolutePath()
                        + " faild", e);
            }
        }
        // 清掉进程内缓存，否则后续 getPlugin() 还会返回已删除的实例
        getStore(variableId).cleanPlugins();
        return deleted;
    }

    /**
     * 存储目录相对于 {@code TIS.pluginCfgRoot} 的路径，同时也是 Key.keyVal 的前缀
     */
    private String getStoreDirRelativePath() {
        return GROUP_NAME + File.separator + ontologyDomainId
                + File.separator + STORE_DIR_NAME + File.separator + moduleName;
    }
}