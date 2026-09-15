package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import com.qlangtech.tis.util.DescriptorsMeta;
import com.qlangtech.tis.util.IPluginContext;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link WorkshopVariable} 多步改造的接线回归测试。
 *
 * <p>改造要点有三条，任何一条断掉都是「表单静默降级」而非编译错误，所以在这里钉住：
 * <ol>
 *   <li><b>步骤序列</b>：宿主必须被识别为 {@link MultiStepsSupportHostDescriptor}，
 *       两步按 Metadata → Definition 排列，且宿主自身不暴露任何可渲染属性
 *       （{@code MultiStepsHostPluginFormProperties.getKVTuples()} 恒空）。见
 *       {@link #hostExposesNoRenderableProperty()}</li>
 *   <li><b>步骤导航兜底</b>：本插件走自定义 {@code httpProcess} 端点，没有服务端往返给的
 *       {@code nextStepPluginDesc}，第二步的元数据全靠
 *       {@code appendExternalProps} 预埋。见 {@link #appendExternalPropsPrebakesEveryStep()}</li>
 *   <li><b>跨步过滤</b>：{@code DefinitionOfVariable.json} 的 {@code subDescEnumFilter}
 *       Groovy 脚本必须能编译，且能按第一步的 type 收敛候选。这是改造的全部动机 ——
 *       改造前 {@code WorkshopVariable.json} 从未接线，用户能选出「数值类型的对象集合定义」。
 *       见 {@link #subDescEnumFilterNarrowsByStep1Type()}</li>
 * </ol>
 *
 * <p><b>运行前提</b>：后两条依赖 TIS 的 descriptor 扫描，JDK17 下需要
 * {@code --add-opens java.base/java.lang=ALL-UNNAMED}。默认 surefire 配置里没有这段
 * argLine，所以扫描失败时<b>整类跳过而不失败</b>，与
 * {@code TestVariableTypeDefinitions#everyBasicDescriptorInRegistryIsReachable} 口径一致。
 * 本地验证用：
 * <pre>
 * mvn -o test-compile surefire:test -Dtest=TestWorkshopVariableMultiSteps \
 *     -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"
 * </pre>
 */
public class TestWorkshopVariableMultiSteps {

    @Before
    public void assumeTisReady() {
        boolean ready;
        try {
            TIS tis = TIS.get();
            // DescriptorExtensionList 是惰性的：真正的扩展扫描发生在 isEmpty()/iterator() 上。
            // 必须在这里就把它物化，否则扫描异常会漏到 @Before 外面变成测试失败而非跳过。
            DescriptorExtensionList<VariableDefinitionConfig, Descriptor<VariableDefinitionConfig>> list =
                    tis == null ? null : tis.getDescriptorList(VariableDefinitionConfig.class);
            ready = list != null && !list.isEmpty();
        } catch (Throwable t) {
            ready = false;
        }
        Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", ready);
    }

    // ===================================================================
    // 1. 步骤序列与宿主形态
    // ===================================================================

    @Test
    public void hostExposesNoRenderableProperty() {
        WorkshopVariable.DefaultDescriptor desc = new WorkshopVariable.DefaultDescriptor();
        Assert.assertTrue("宿主 descriptor 必须是 MultiStepsSupportHostDescriptor",
                desc instanceof MultiStepsSupportHostDescriptor);
        Assert.assertEquals(WorkshopVariable.class, desc.getHostClass());

        List<OneStepOfMultiSteps.BasicDesc> steps = desc.getStepDescriptionList();
        Assert.assertEquals("步骤数固定为 2", 2, steps.size());
        Assert.assertEquals(OneStepOfMultiSteps.Step.Step1, steps.get(0).getStep());
        Assert.assertEquals(OneStepOfMultiSteps.Step.Step2, steps.get(1).getStep());
        Assert.assertEquals("Metadata", steps.get(0).getStepDescription());
        Assert.assertEquals("Definition", steps.get(1).getStepDescription());
        Assert.assertEquals("第一步", steps.get(0).getDisplayName());
        Assert.assertEquals("第二步", steps.get(1).getDisplayName());
        Assert.assertFalse("第一步不是最后一步", steps.get(0).isFinalStep());
        Assert.assertTrue("第二步是最后一步", steps.get(1).isFinalStep());
        Assert.assertTrue("第一步的下一步必须是 DefinitionOfVariable.Desc",
                steps.get(0).nextPluginDesc(null).orElse(null) instanceof DefinitionOfVariable.Desc);
        Assert.assertTrue("第二步之后没有下一步", steps.get(1).nextPluginDesc(null).isEmpty());

        // 宿主已无可渲染属性：一切都在 stepsPlugin 里
        PluginFormProperties props = desc.getPluginFormPropertyTypes();
        Assert.assertEquals("MultiStepsHostPluginFormProperties",
                "MultiStepsHostPluginFormProperties", props.getClass().getSimpleName());
        Assert.assertFalse("宿主不应暴露 useless 字段", props.containProperty("useless"));
        Assert.assertTrue("宿主不应有可渲染属性", props.getKVTuples().isEmpty());

        // WorkshopVariable 实现 IdentityName，必须有且仅有一个 identity 字段；
        // 字段数不对这里会直接抛 IllegalStateException
        Assert.assertNotNull("useless 必须被识别为 identity 字段", desc.getIdentityField());
    }

    /**
     * 自定义端点没有服务端往返，第二步元数据必须全部预埋在 {@code appendExternalProps} 里。
     */
    @Test
    public void appendExternalPropsPrebakesEveryStep() {
        JSONObject stepContext = new JSONObject();
        new WorkshopVariable.DefaultDescriptor().appendExternalProps(stepContext);

        JSONArray stepDescList = stepContext.getJSONArray("stepDescList");
        Assert.assertNotNull("stepDescList 必须被预埋进 step context", stepDescList);
        Assert.assertEquals(2, stepDescList.size());
        Assert.assertEquals("Metadata", stepDescList.getJSONObject(0).getString("stepDescription"));
        Assert.assertEquals("Definition", stepDescList.getJSONObject(1).getString("stepDescription"));
        Assert.assertEquals(0, stepDescList.getJSONObject(0).getIntValue("stepIndex"));
        Assert.assertEquals(1, stepDescList.getJSONObject(1).getIntValue("stepIndex"));

        // descriptor 是 DescriptorsMeta POJO（与 DescriptorsJSON 里 firstStepDesc 同形），
        // 靠 @JSONType(serializer=...) 在外层序列化时展开成 JSON
        Assert.assertTrue("第二步的 descriptor 必须一并预埋",
                stepDescList.getJSONObject(1).get("descriptor") instanceof DescriptorsMeta);
    }

    // ===================================================================
    // 2. 跨步过滤（本次改造的全部动机）
    // ===================================================================

    /**
     * 把第一步实例按 {@code OneStepOfMultiSteps#processCurrentStep} 的 key 放进 Context 后，
     * 第二步的候选必须按第一步的 {@link VariableType} 收敛。
     *
     * <p>走的是真实的 json → Groovy {@code subDescEnumFilter} 路径，因此也顺带钉住了
     * 「{@code DefinitionOfVariable.json} 里的脚本能编译且类名/方法名没写错」。
     */
    @Test
    public void subDescEnumFilterNarrowsByStep1Type() {
        Assert.assertEquals("OBJECT_SET 只允许 ObjectSetDefinition + Function",
                2, applicableNames(VariableType.OBJECT_SET).size());
        Assert.assertEquals("GEOPOINT 只允许 ObjectProperty",
                1, applicableNames(VariableType.GEOPOINT).size());
        Assert.assertEquals("NUMERIC 允许 5 种", 5, applicableNames(VariableType.NUMERIC).size());
    }

    /**
     * 无第一步上下文时必须<b>原样放行</b>，否则会在「选了类型却没得选」处留死角 ——
     * 这条同时覆盖 {@code appendExternalProps} 预生成元数据与 LLM schema 生成两个场景。
     */
    @Test
    public void subDescEnumFilterPassesThroughWithoutStep1Context() {
        List<? extends Descriptor> all = new PropertyTypeOfDefinition().applicableDescriptors();
        Assert.assertFalse("没有 step1 上下文时应返回全部候选", all.isEmpty());
        Assert.assertEquals("无上下文时不应过滤掉任何一个",
                all.size(), DefinitionOfVariable.descFilter(new ArrayList<>(all)).size());
        Assert.assertEquals("无上下文时应返回全部 7 种定义方式", 7, all.size());
    }

    // ===================================================================
    // 辅助
    // ===================================================================

    private static Set<String> applicableNames(VariableType type) {
        MetadataOfVariable metaStep = new MetadataOfVariable();
        metaStep.type = type;

        Map<String, Object> store = new HashMap<>();
        // key 必须与 OneStepOfMultiSteps#processCurrentStep 的 put key 一致
        store.put(MetadataOfVariable.class.getName(), metaStep);
        Context turbineCtx = new StoreBackedContext(store);

        IPluginContext pluginContext = (IPluginContext) Proxy.newProxyInstance(
                TestWorkshopVariableMultiSteps.class.getClassLoader(),
                new Class[]{IPluginContext.class},
                (proxy, method, args) -> "getContext".equals(method.getName()) ? turbineCtx : null);

        IPluginContext.pluginContextThreadLocal.set(pluginContext);
        try {
            Set<String> names = new java.util.HashSet<>();
            for (Descriptor desc : new PropertyTypeOfDefinition().applicableDescriptors()) {
                // 用全限定名：7 个定义方式的 descriptor 都是内部类 DefaultDescriptor，
                // 取 getSimpleName() 会把它们全折叠成一个
                names.add(desc.getClass().getName());
            }
            return names;
        } finally {
            IPluginContext.pluginContextThreadLocal.remove();
        }
    }

    /**
     * 取第二步 {@code definitionConfig} 字段的 {@link PropertyType}，每次新建 descriptor
     * 以免复用 {@code subDescFilter} 缓存掩盖住接线问题
     */
    private static class PropertyTypeOfDefinition {
        private final PropertyType pp;

        PropertyTypeOfDefinition() {
            Descriptor<OneStepOfMultiSteps> stepDesc = new DefinitionOfVariable.Desc();
            this.pp = (PropertyType) stepDesc.getPropertyTypes().get("definitionConfig");
            Assert.assertNotNull("definitionConfig 必须有 PropertyType", this.pp);
        }

        List<? extends Descriptor> applicableDescriptors() {
            return pp.applicableDescriptors(true);
        }
    }

    private static class StoreBackedContext implements Context {
        private final Map<String, Object> store;

        StoreBackedContext(Map<String, Object> store) {
            this.store = store;
        }

        @Override
        public boolean containsKey(String key) {
            return store.containsKey(key);
        }

        @Override
        public Object get(String key) {
            return store.get(key);
        }

        @Override
        public Set<String> keySet() {
            return store.keySet();
        }

        @Override
        public Object put(String key, Object value) {
            return store.put(key, value);
        }

        @Override
        public void remove(String key) {
            store.remove(key);
        }
    }
}
