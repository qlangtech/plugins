package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.SetPluginsResult;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.StaticConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.StringVariable;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

/**
 * {@link WorkshopVariable} 落盘链路的端到端回归测试。
 *
 * <p>覆盖的是「变量实例 → {@code IPluginStore} → xml 文件 → 反序列化回实例」这整条链路，
 * 而不是单点的字段校验：
 * <ol>
 *   <li>{@code WorkshopVariable.workshopVariable}（{@link com.qlangtech.tis.util.HeteroEnum}）
 *       按插件元数据解析出<b>该变量专属</b>的 store —— 元数据里缺任何一个 extraParam
 *       （domain / workshop / identityName / startPersistence）都会走到
 *       {@code noSaveStore} 或直接抛异常，而这类错误在编译期毫无提示</li>
 *   <li>落盘位置：{@code ontology/{domain}/workshop/{module}/{variableName}.xml}，
 *       即 {@link WorkshopModule.BasicWorkshopStoreGetter} 约定的 workshop 目录</li>
 *   <li>反序列化后 instance 的字段（尤其抽象字段 {@code definitionConfig} 的具体子类）
 *       依然完整 —— XStream 对声明类型为抽象类的字段要能写回具体类</li>
 * </ol>
 *
 * <p><b>元数据契约</b>：{@code BasicWorkshopStoreGetter.getPluginStore()} 只认
 * {@code OntologyPluginMeta} 上的四个 extraParam，缺一不可（见
 * {@link #pluginMeta(WorkshopVariable)}）。这些键名散落在框架各处，
 * 写错的表现是「静默落到了 noSaveStore 上，什么都没存」而不是报错，故在这里一次性钉住。
 *
 * @see WorkshopVariable#workshopVariable
 */
public class WorkshopVariableTest {

    /**
     * 测试用 domain。刻意用一个不会与真实配置撞车的名字，
     * {@link #cleanUp()} 会按它整棵删掉。
     */
    private static final String ONTOLOGY_DOMAIN_ID = "test_workshop_variable_domain";

    /**
     * 变量在 workshop 目录下再按 moduleName 分一层目录（变量名只在模块内唯一），
     * 所以测试也需要一个模块名。
     */
    private static final String MODULE_NAME = "test_workshop_module";

    /**
     * 测试会真的写到 {@code TIS.pluginCfgRoot} 下（落盘位置由 store 的 Key 决定，
     * 没法让变量改道到临时目录），故结束时把整个 module 目录删掉，避免污染本机的 TIS 配置。
     * <p>
     * 注意删除只覆盖到 module 一层：domain 目录下的 {@code .lastmodified} 会被框架在
     * JVM 退出时回写（实测 {@code @After} 里刚删完 {@code exists=false}，进程退出后目录
     * 又带着新时间戳出现），所以这里不声称能清干净——残留的就只有一个时间戳文件。
     */
    @After
    public void cleanUp() throws Exception {
        FileUtils.deleteQuietly(getWorkshopDir());
    }

    /**
     * 一个合法的 {@link VariableType#STRING} 变量：静态值定义。
     *
     * <p>{@link StaticConfig} 在 {@code VariableDefinitionConfig#TYPE_DEFINITIONS}
     * 的 STRING 行里，因此 {@link WorkshopVariable#validateDefinition} 会放行；
     * 若换成 {@code ObjectSetDefinitionConfig} 这类只能产出 OBJECT_SET 的定义方式，
     * 落盘守卫会先一步拒绝。
     */
    @Test
    public void testHeteroEnum() {
        StringVariable variable = new StringVariable();
        // name 同时是 Key 的键与落盘文件名（id 已删除）。
        variable.name = "test_string_var";
        StaticConfig definitionConfig = new StaticConfig();
        definitionConfig.value = "hello workshop";
        variable.definitionConfig = definitionConfig;

        // 变量类型(子类) ↔ 定义方式 的组合先在落盘前过一遍守卫，非法组合不该走到写文件
        WorkshopVariable.validateDefinition(variable);

        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        IPluginStore<WorkshopVariable> variableStore
                = WorkshopVariable.workshopVariable.getPluginStore(pluginContext, pluginMeta(variable));
        Assert.assertNotNull("变量 store 不应为 null，元数据齐全时必须走可落盘的 store", variableStore);

        SetPluginsResult result = variableStore.setPlugins(pluginContext, Optional.<Context>empty(),
                Collections.singletonList(new Descriptor.ParseDescribable<>(variable)), false);
        Assert.assertTrue("保存应当成功", result.success);
        Assert.assertTrue("首次保存应当被判定为配置有变更", result.cfgChanged);

        // 落盘位置是元数据约定的结果，不是变量自己挑的：domain / workshop / variableName 三层都在路径里
        File storeFile = new File(getWorkshopDir(), variable.getName() + ".xml");
        Assert.assertTrue("变量应落在 " + storeFile.getAbsolutePath(), storeFile.exists());

        // 清掉进程内缓存再读，否则读到的是刚 setPlugins 进去的内存实例，文件坏掉也发现不了
        variableStore.cleanPlugins();
        WorkshopVariable loaded = variableStore.getPlugin();
        Assert.assertNotNull("重新加载应当读到刚保存的变量", loaded);
        // 文件里记的是具体子类名，读回来必须是同一个子类，否则前端无法回填表单
        Assert.assertEquals(StringVariable.class, loaded.getClass());
        Assert.assertEquals(variable.name, loaded.getName());
        Assert.assertNotNull("definitionConfig 是抽象类型字段，反序列化后不能为 null", loaded.getDefinitionConfig());
        Assert.assertEquals(StaticConfig.class, loaded.getDefinitionConfig().getClass());
        Assert.assertEquals(definitionConfig.value, ((StaticConfig) loaded.getDefinitionConfig()).value);
    }

    // ==================================================================
    //  辅助
    // ==================================================================

    /**
     * 变量专属 store 的元数据。
     *
     * <p>四个 key 与 {@code BaiscAssistStoreGetter#getPluginStore} 的读取一一对应，
     * 少一个的后果分别是：
     * <ul>
     *   <li>{@code startPersistence} —— 走到 {@code IPluginStore.noSaveStore}，
     *       保存「成功」但磁盘上什么都没有</li>
     *   <li>{@code ontology}（domain）—— 抛 {@code ontologyName can not be empty}</li>
     *   <li>{@code identityName} —— 抛 {@code pluginIdVal can not be empty}，
     *       它同时还是落盘文件名</li>
     *   <li>{@code workshop}（moduleName）—— {@code WorkshopVariable} 那边
     *       {@code Optional.of(...)} 会直接 NPE</li>
     * </ul>
     */
    private static UploadPluginMeta pluginMeta(WorkshopVariable variable) {
        UploadPluginMeta pluginMeta = UploadPluginMeta.create(Ontology.ONTOLOGY);
        pluginMeta.putExtraParams(OntologyDomain.NAME_ONTOLOGY_DOMAIN, ONTOLOGY_DOMAIN_ID);
        pluginMeta.putExtraParams(OntologyDomain.KEY_WORKSHOP, MODULE_NAME);
        pluginMeta.putExtraParams(OntologyValueType.KEY_START_PERSISTENCE, Boolean.TRUE.toString());
        pluginMeta.putExtraParams(IdentityName.PLUGIN_IDENTITY_NAME, variable.getName());
        return pluginMeta;
    }

    /**
     * {@code {TIS.pluginCfgRoot}/ontology/{domain}/workshop/{module}}
     */
    private static File getWorkshopDir() {
        return WorkshopModule.getWorkshopDir(ONTOLOGY_DOMAIN_ID, MODULE_NAME);
    }
}