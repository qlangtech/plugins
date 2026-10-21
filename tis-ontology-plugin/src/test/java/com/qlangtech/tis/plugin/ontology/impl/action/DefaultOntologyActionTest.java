package com.qlangtech.tis.plugin.ontology.impl.action;

import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.InputTextWidget;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

/**
 * {@link ActionParameterConfig} 落盘链路的回归测试。
 *
 * <p>覆盖「参数实例 → {@code IPluginStore} → xml 文件 → 反序列化回实例」整条链路，
 * 重点钉住两件在编译期毫无提示、出错即静默的事：
 * <ol>
 *   <li><b>store 的 pluginClass 必须是 {@link ActionParameterConfig}</b>。若沿用
 *       {@code Ontology.OntologyEnum.Action.storeKeyGetter}，pluginClass 是
 *       {@code Ontology.class}，{@code PluginStore.setPlugins} 会在
 *       「plugin must be type of …Ontology」处抛异常；读侧则按同一个 pluginClass
 *       反序列化，同样取不到值。</li>
 *   <li><b>元数据里的 {@code identityName} 是落盘文件名</b>。它同时决定参数文件落在
 *       {@code params/<参数名>/} 下的具体文件名，缺了会直接抛
 *       {@code param pluginIdVal can not be empty}。</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/13
 * @see ActionParamStoreGetter
 */
public class DefaultOntologyActionTest {

    /**
     * 测试用 domain。刻意用一个不会与真实配置撞车的名字，{@link #cleanUp()} 会按它清理。
     */
    private static final String ONTOLOGY_DOMAIN_ID = "test_action_param_domain";

    /**
     * Action 名。在详情页链路里它就是 meta 的 {@code pluginIdVal}，因而也是参数文件的名字 ——
     * 参数名只作目录名，这样同一 domain 下不同 Action 的同名参数互不覆盖。
     */
    private static final String ACTION_NAME = "test_action";

    private static final String PARAM_ID = "newPriority";

    @After
    public void cleanUp() throws Exception {
        FileUtils.deleteQuietly(OntologyDomain.getActionDir(ONTOLOGY_DOMAIN_ID));
    }

    /**
     * 落盘位置约定：{@code <cfgRoot>/ontology/<domain>/action-type/params/<参数名>/<actionName>.xml}。
     */
    @Test
    public void testParamStoreFileLayout() {
        IPluginStore<?> store = ActionParameterConfig.getOntologyActionTypeParamStore(PARAM_ID, paramMeta());
        Assert.assertNotNull("元数据齐全时必须走可落盘的 store，而不是 noSaveStore", store.getTargetFile());
        Assert.assertEquals(paramFile().getAbsolutePath(),
                store.getTargetFile().getFile().getAbsolutePath());
    }

    /**
     * 端到端回环：save → xml → 反序列化回实例。
     */
    @Test
    public void testSaveAndReloadParam() {
        ActionParameterConfig param = new ActionParameterConfig();
        param.parameterId = PARAM_ID;
        param.displayName = "新优先级";
        PrimitiveDataType dataType = new PrimitiveDataType();
        dataType.widgetType = new InputTextWidget();
        param.dataType = dataType;

        IPluginContext pluginContext = Mockito.mock(IPluginContext.class);
        OntologyPluginMeta meta = paramMeta();
        ActionParameterConfig.save(pluginContext, meta, param);

        Assert.assertTrue("参数应落在 " + paramFile().getAbsolutePath(), paramFile().exists());

        // 清掉进程内缓存再读，否则读到的是刚 setPlugins 进去的内存实例，文件坏掉也发现不了
        IPluginStore<?> store = ActionParameterConfig.getOntologyActionTypeParamStore(PARAM_ID, meta);
        store.cleanPlugins();
        Object loaded = store.getPlugin();
        Assert.assertTrue("读回来的必须是 ActionParameterConfig，实际：" + loaded,
                loaded instanceof ActionParameterConfig);

        ActionParameterConfig reloaded = (ActionParameterConfig) loaded;
        Assert.assertEquals(PARAM_ID, reloaded.identityValue());
        Assert.assertEquals("显示名应当被持久化下来", "新优先级", reloaded.displayName);
        // dataType 声明类型是抽象类，XStream 反序列化后必须还原成具体子类，否则前端无法回填
        Assert.assertTrue("dataType 应还原成 PrimitiveDataType，实际：" + reloaded.dataType,
                reloaded.dataType instanceof PrimitiveDataType);
        Assert.assertTrue("widgetType 应还原成 InputTextWidget",
                ((PrimitiveDataType) reloaded.dataType).widgetType instanceof InputTextWidget);
    }

    // ==================================================================
    //  辅助
    // ==================================================================

    /**
     * 详情页链路里的参数元数据：domain + Action 名（{@code pluginIdVal}）+ 强制持久化。
     * 三者与 {@link DefaultOntologyAction.DefaultDesc#httpProcess} 里构造的完全一致。
     */
    private static OntologyPluginMeta paramMeta() {
        return OntologyPluginMeta.create(Ontology.OntologyEnum.Action, ONTOLOGY_DOMAIN_ID)
                .setPluginIdVal(ACTION_NAME)
                .setPersistence();
    }

    private static File paramFile() {
        return new File(new File(OntologyDomain.getActionDir(ONTOLOGY_DOMAIN_ID), "params"), PARAM_ID)
                .toPath().resolve(ACTION_NAME + ".xml").toFile();
    }
}
