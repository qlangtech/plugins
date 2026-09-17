package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.StaticConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.StringVariable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link WorkshopVariable} 单步化改造的接线回归测试。
 *
 * <p>本类取代原先的 {@code TestWorkshopVariableMultiSteps}（多步宿主形态已不存在）。
 * 改造把「变量类型」从第一步表单里的一个 ENUM 字段，变成了 <b>Java 子类</b>，
 * 随之而来有三条不显眼的接线，任何一条断掉都是「表单静默降级」而非编译错误：
 * <ol>
 *   <li><b>枚举 ↔ 子类一一对应</b>：{@link VariableType} 的 12 个常量必须各有且只有一个
 *       子类，见 {@link #everyVariableTypeHasExactlyOneSubclass()}</li>
 *   <li><b>候选集收敛</b>：{@code definitionConfig} 的下拉必须按子类代表的类型收敛。
 *       脚本由 {@link WorkshopVariable.BasicDescriptor} 构造期注入（不再有 json 里的
 *       {@code subDescEnumFilter}），是本次改造<b>最该被钉住</b>的行为，
 *       见 {@link #eachSubclassNarrowsDefinitionConfigCandidates()}</li>
 *   <li><b>单步形态</b>：宿主字段回到一张可渲染的表单（对照改造前多步宿主
 *       {@code getKVTuples()} 恒空），见 {@link #subclassDescriptorExposesFlatRenderableProperties()}</li>
 * </ol>
 *
 * <p><b>运行前提</b>：三条都依赖 TIS 的 descriptor 扫描，JDK17 下需要
 * {@code --add-opens java.base/java.lang=ALL-UNNAMED}。默认 surefire 配置里没有这段
 * argLine，所以扫描失败时<b>整类跳过而不失败</b>，与
 * {@code TestVariableTypeDefinitions#everyBasicDescriptorInRegistryIsReachable} 口径一致。
 * 本地验证用：
 * <pre>
 * mvn -o test -Dtest=TestWorkshopVariableTypes \
 *     -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"
 * </pre>
 * 注意第二条会真的编译 Groovy 脚本，缺 {@code --add-opens} 时它同样会被跳过。
 */
public class TestWorkshopVariableTypes {

    private static final String FIELD_DEFINITION_CONFIG = "definitionConfig";

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
    // 1. 枚举 ↔ 子类一一对应
    // ===================================================================

    /**
     * 12 个 {@link VariableType} 常量与 12 个 {@link WorkshopVariable} 子类必须<b>双向相等</b>。
     *
     * <p>用双向相等而非「扫描结果 ⊆ 枚举」：后者在扫到 0 个时循环体不执行，会假通过。
     * 这条同时是「新增变量类型」的哨兵 —— 加了枚举常量却没加子类（或反之）都会红。
     */
    @Test
    public void everyVariableTypeHasExactlyOneSubclass() {
        List<Descriptor<WorkshopVariable>> descs = scanVariableDescriptors();

        Map<VariableType, Class<?>> byType = new HashMap<>();
        for (Descriptor<WorkshopVariable> d : descs) {
            WorkshopVariable.BasicDescriptor bd = (WorkshopVariable.BasicDescriptor) d;
            Class<?> previous = byType.put(bd.getVariableType(), d.getClass());
            Assert.assertNull("变量类型 " + bd.getVariableType() + " 被多个 descriptor 认领："
                    + previous + " 与 " + d.getClass(), previous);
        }

        Assert.assertEquals("枚举常量与子类必须一一对应",
                EnumSet.allOf(VariableType.class), byType.keySet());
        Assert.assertEquals("子类数与枚举常量数必须相同（多一个少一个都说明有类型没接上）",
                VariableType.values().length, descs.size());
    }

    // ===================================================================
    // 2. 候选集收敛（本次改造的全部动机）
    // ===================================================================

    /**
     * 每个子类的 {@code definitionConfig} 候选必须<b>恰好</b>等于
     * {@link VariableDefinitionConfig#TYPE_DEFINITIONS} 中该类型那一行。
     *
     * <p>这是 {@link WorkshopVariable.BasicDescriptor} 构造期注入 {@code subDescEnumFilter}
     * 这条路子唯一的端到端验证：走的是真实的 descriptor → PropertyType → Groovy 编译链路，
     * 因此也顺带钉住了「注入的脚本能编译、类名/方法名没写错、Groovy 类名逐子类唯一」。
     */
    @Test
    public void eachSubclassNarrowsDefinitionConfigCandidates() {
        List<Descriptor<WorkshopVariable>> descs = scanVariableDescriptors();
        Assert.assertFalse("必须扫到变量类型 descriptor", descs.isEmpty());

        for (Descriptor<WorkshopVariable> d : descs) {
            WorkshopVariable.BasicDescriptor bd = (WorkshopVariable.BasicDescriptor) d;
            VariableType type = bd.getVariableType();

            // 用 useCache=false 重新构建，免得复用上一次的 PropertyType / subDescFilter 缓存
            // 把「注入没生效」掩盖成「上一次编译的过滤器还在」
            PropertyType pp = (PropertyType) d.getPropertyTypes(false).get(FIELD_DEFINITION_CONFIG);
            Assert.assertNotNull(d.getClass() + " 必须有 definitionConfig 的 PropertyType", pp);

            Assert.assertTrue("subDescEnumFilter 必须已被注入到 " + d.getClass(),
                    pp.getExtraProps() != null && pp.getExtraProps().getString("subDescEnumFilter") != null);

            Set<String> actual = new HashSet<>();
            for (Descriptor candidate : pp.applicableDescriptors(true)) {
                actual.add(candidate.getClass().getName());
            }

            Set<String> expected = VariableDefinitionConfig.definitionsOf(type).stream()
                    .map(Class::getName)
                    .collect(Collectors.toSet());

            Assert.assertEquals("类型 " + type + " 的 definitionConfig 候选集必须与 "
                    + "TYPE_DEFINITIONS 该行一致", expected, actual);
        }
    }

    // ===================================================================
    // 3. CRUD 端点独立寻址
    // ===================================================================

    /**
     * 变量 CRUD 端点搬到了 {@code WorkshopVariableOperation}，前端用它的 FQCN 作
     * {@code impl} 参数寻址，所以那条路必须真的能解析到 descriptor。
     *
     * <p>同时验证它与 12 个变量类型 descriptor <b>解耦</b>：后者是前端类型选择器的数据源，
     * 混进一个「不是类型」的条目会让选择器出现第 13 个空表单项。
     */
    @Test
    public void operationDescriptorIsAddressableByItsFqcn() {
        String impl = "com.qlangtech.tis.plugin.ontology.workshop.desc.WorkshopVariableOperation";

        Descriptor operationDesc = TIS.get().getDescriptor(impl);
        Assert.assertNotNull("前端以 impl=" + impl + " 寻址变量 CRUD 端点，必须能解析到 descriptor",
                operationDesc);
        Assert.assertEquals("impl 即 descriptor 的 id", impl, operationDesc.getId());

        for (Descriptor<WorkshopVariable> vd : scanVariableDescriptors()) {
            Assert.assertNotEquals("CRUD 描述符不应混进变量类型选择器：" + vd.getClass(),
                    impl, vd.getId());
        }
    }

    // ===================================================================
    // 4. 单步形态
    // ===================================================================

    /**
     * 子类 descriptor 必须暴露一张平铺的可渲染表单 —— 对照改造前多步宿主的
     * {@code MultiStepsHostPluginFormProperties.getKVTuples()} 恒为空。
     */
    @Test
    public void subclassDescriptorExposesFlatRenderableProperties() {
        Descriptor<WorkshopVariable> d = scanVariableDescriptors().get(0);

        PluginFormProperties props = d.getPluginFormPropertyTypes();
        Assert.assertEquals("单步化之后必须是普通的 RootFormProperties",
                "RootFormProperties", props.getClass().getSimpleName());

        Assert.assertTrue("name 必须可渲染", props.containProperty("name"));
        Assert.assertTrue("definitionConfig 必须可渲染", props.containProperty(FIELD_DEFINITION_CONFIG));
        Assert.assertFalse("宿主字段不该再有可渲染属性为空的多步形态", props.getKVTuples().isEmpty());

        // WorkshopVariable 实现 IdentityName，框架要求有且仅有 1 个 identity 字段，
        // 数量不对 getPropertyTypes() 会直接抛 IllegalStateException
        Assert.assertNotNull("id 必须被识别为 identity 字段", d.getIdentityField());
        Assert.assertEquals("id", d.getIdentityField().f.getName());

        // id 是构造器里自动生成的 UUID，且决定落盘文件名，暴露在基本表单里只会让用户误改。
        // 单步化之后它第一次真的会被渲染（多步宿主时它躲在第一步里），所以用 json 的
        // advance 把它收进「高级」分组 —— 前端 [hide]="pp.advance && !item.showAllField"。
        PropertyType idProp = (PropertyType) d.getPropertyTypes(false).get("id");
        Assert.assertNotNull("id 必须有 PropertyType", idProp);
        Assert.assertTrue("id 应被收进高级分组，不作为基本表单项露出", idProp.advance());
    }

    /**
     * 落盘守卫不能因为「前端已经过滤了下拉」而失效：绕过前端直接 POST 非法组合仍要报错。
     */
    @Test
    public void validateDefinitionRejectsIncompatibleCombination() {
        WorkshopVariable stringVar = stringVariable();
        // OBJECT_SET 专用的定义方式，产出不了 STRING
        stringVar.definitionConfig = new ObjectSetDefinitionConfig();
        try {
            WorkshopVariable.validateDefinition(stringVar);
            Assert.fail("STRING + ObjectSetDefinitionConfig 应当被拒绝");
        } catch (IllegalStateException e) {
            Assert.assertTrue("报错信息应指出可用范围，实际是：" + e.getMessage(),
                    e.getMessage().contains("不能由"));
        }

        // 合法组合必须放行（否则守卫会误伤）
        stringVar.definitionConfig = new StaticConfig();
        WorkshopVariable.validateDefinition(stringVar);
    }

    // ===================================================================
    // 辅助
    // ===================================================================

    /**
     * 只需要一个「类型正确、字段可填」的壳；构造器里的 {@code id} 已自动生成 UUID。
     */
    private static WorkshopVariable stringVariable() {
        WorkshopVariable v = new StringVariable();
        v.name = "test_string_var";
        return v;
    }

    private static List<Descriptor<WorkshopVariable>> scanVariableDescriptors() {
        return new ArrayList<>(TIS.get().getDescriptorList(WorkshopVariable.class));
    }
}
