package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link VariableDefinitionConfig#TYPE_DEFINITIONS} 的回归测试。
 *
 * <p>这张表是「变量类型 → 可用定义方式」的唯一真源，驱动新建变量时的两级级联菜单
 * （一级选 {@link VariableType}，二级只列可产该类型的定义方式）。它没有编译期保护
 * （选的是单张静态 Map，而非每个子类各写一个抽象方法），所以靠这里把三种失败模式钉住：
 * <ol>
 *   <li><b>漏登记</b>：新增子类却忘了写进表里 → 该定义方式永远不出现在任何下拉里，
 *       且编译器不报错。见 {@link #everyBasicDescriptorIsRegistered()} 与
 *       {@link #everyBasicDescriptorInRegistryIsReachable()}</li>
 *   <li><b>行列错漏 / 退化成全放行</b>：改表时漏了一行、或把某个类型写成「全部可用」，
 *       等价于没做过滤。见 {@link #matrixMatchesApprovedDesign()}</li>
 *   <li><b>写成空集</b>：某类型一个候选都没有，用户选完类型二级就空了。见
 *       {@link #everyVariableTypeHasAtLeastOneDefinition()}</li>
 * </ol>
 *
 * @see VariableDefinitionConfig#TYPE_DEFINITIONS
 */
public class TestVariableTypeDefinitions {

  /**
   * 覆盖性检查（矩阵外部）：表里出现的每个 descriptor 都必须是 {@code public} 的
   * {@link VariableDefinitionConfig.BasicDescriptor}，且外层类是 {@code VariableDefinitionConfig} 子类。
   *
   * <p>与 {@link #everyBasicDescriptorIsRegistered()} 方向相反：那条查「子类漏登记」，
   * 这条查「表里写了不存在的/不可见的东西」——例如手误写成内部类的非 public 形态，
   * 会让表单侧的 Groovy 过滤脚本在运行期才炸。
   */
  @Test
  public void everyRegisteredClassIsAPublicBasicDescriptor() {
    for (Map.Entry<VariableType, List<Class<? extends VariableDefinitionConfig.BasicDescriptor>>> entry
      : VariableDefinitionConfig.TYPE_DEFINITIONS.entrySet()) {
      for (Class<? extends VariableDefinitionConfig.BasicDescriptor> descClazz : entry.getValue()) {
        Assert.assertTrue(entry.getKey() + " 登记了非 public 的 " + descClazz.getName()
          + "，Groovy 过滤脚本与跨包校验都引用不到它", Modifier.isPublic(descClazz.getModifiers()));

        Class<?> outer = descClazz.getEnclosingClass();
        Assert.assertNotNull(descClazz.getName() + " 应是嵌套的 DefaultDescriptor", outer);
        Assert.assertTrue(outer.getName() + " 应是 VariableDefinitionConfig 的子类",
          VariableDefinitionConfig.class.isAssignableFrom(outer));
      }
    }
  }

  /**
   * 覆盖性检查（矩阵内部）：每个类型都至少有一个候选。
   *
   * <p>这是 {@code MetadataOfValueType} 里那句警告对应的失败模式：
   * 「不然的话选择了某个 type 到第二步会有没有可选的约束的情况产生」。
   */
  @Test
  public void everyVariableTypeHasAtLeastOneDefinition() {
    for (VariableType type : VariableType.values()) {
      Assert.assertFalse("变量类型 " + type + " 没有任何可用定义方式，二级菜单会是空的",
        VariableDefinitionConfig.definitionsOf(type).isEmpty());
    }
    Assert.assertEquals("映射表必须覆盖全部 VariableType 常量",
      VariableType.values().length, VariableDefinitionConfig.TYPE_DEFINITIONS.size());
  }

  /** 同一个类型下不该重复登记同一个 descriptor（多半是手工加行的复制粘贴残留） */
  @Test
  public void noDuplicateDefinitionWithinAType() {
    for (Map.Entry<VariableType, List<Class<? extends VariableDefinitionConfig.BasicDescriptor>>> entry
      : VariableDefinitionConfig.TYPE_DEFINITIONS.entrySet()) {
      Set<Class<? extends VariableDefinitionConfig.BasicDescriptor>> unique = new HashSet<>(entry.getValue());
      Assert.assertEquals("VariableType." + entry.getKey() + " 重复登记了同一个 descriptor",
        unique.size(), entry.getValue().size());
    }
  }

  /**
   * 矩阵内容与设计定稿逐行一致。
   *
   * <p>口径（见 {@link VariableDefinitionConfig} 类注释「矩阵的口径」）：
   * 取官方文档与本项目附录 A 的交集，且 Function 严格按官方逐类型限定。
   * 改这张表等于改产品行为，必须同步改这里——所以这里刻意写死，不做「从表推导表」的循环断言。
   */
  @Test
  public void matrixMatchesApprovedDesign() {
    assertDefinitions(VariableType.STRING,
      StaticConfig.DefaultDescriptor.class,
      ObjectPropertyConfig.DefaultDescriptor.class,
      SQLQueryConfig.DefaultDescriptor.class,
      VariableTransformationConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    assertDefinitions(VariableType.BOOLEAN,
      StaticConfig.DefaultDescriptor.class,
      ObjectPropertyConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    assertDefinitions(VariableType.DATE,
      StaticConfig.DefaultDescriptor.class,
      ObjectPropertyConfig.DefaultDescriptor.class,
      SQLQueryConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    assertDefinitions(VariableType.TIMESTAMP,
      StaticConfig.DefaultDescriptor.class,
      ObjectPropertyConfig.DefaultDescriptor.class,
      SQLQueryConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    // 集合聚合唯一存活的类型：count/sum/average 只产出数值
    assertDefinitions(VariableType.NUMERIC,
      StaticConfig.DefaultDescriptor.class,
      ObjectPropertyConfig.DefaultDescriptor.class,
      ObjectSetAggregationConfig.DefaultDescriptor.class,
      SQLQueryConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    // 官方：GeoPoint 只 "Initialized from an object property"
    assertDefinitions(VariableType.GEOPOINT,
      ObjectPropertyConfig.DefaultDescriptor.class);

    assertDefinitions(VariableType.GEOSHAPE,
      ObjectPropertyConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    assertDefinitions(VariableType.ARRAY,
      StaticConfig.DefaultDescriptor.class,
      VariableTransformationConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    assertDefinitions(VariableType.STRUCT,
      StaticConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    // 官方明说 object set 不能由 SQL query 产出（附录 A 在这里是错的）
    assertDefinitions(VariableType.OBJECT_SET,
      ObjectSetDefinitionConfig.DefaultDescriptor.class,
      FunctionConfig.DefaultDescriptor.class);

    assertDefinitions(VariableType.OBJECT_SET_FILTER,
      StaticConfig.DefaultDescriptor.class,
      VariableTransformationConfig.DefaultDescriptor.class);

    assertDefinitions(VariableType.TIME_SERIES_SET,
      ObjectPropertyConfig.DefaultDescriptor.class);
  }

  /**
   * 表必须真的具备区分度：至少一个类型用不了全部定义方式，且几个语义强约束成立。
   *
   * <p>防止有人为了「别让用户选不到」把每个类型都写成全部可用——那等于这张表退化成恒真。
   */
  @Test
  public void matrixIsActuallyRestrictive() {
    List<VariableDefinitionConfig.BasicDescriptor> all = allDescriptors();

    Assert.assertTrue("每个类型都可用全部定义方式，这张表等于没做过滤",
      VariableDefinitionConfig.filter(VariableType.OBJECT_SET, all).size() < all.size());

    Assert.assertFalse("OBJECT_SET 不该由 SQLQuery 定义（官方明确排除）",
      VariableDefinitionConfig.definitionsOf(VariableType.OBJECT_SET)
        .contains(SQLQueryConfig.DefaultDescriptor.class));

    Assert.assertFalse("GEOPOINT 只允许 ObjectProperty（官方：only object property）",
      VariableDefinitionConfig.definitionsOf(VariableType.GEOPOINT)
        .contains(FunctionConfig.DefaultDescriptor.class));

    Assert.assertFalse("OBJECT_SET_FILTER 不该由 ObjectSetDefinition 定义（那是产出 object set 的）",
      VariableDefinitionConfig.definitionsOf(VariableType.OBJECT_SET_FILTER)
        .contains(ObjectSetDefinitionConfig.DefaultDescriptor.class));
  }

  /**
   * {@code type == null} 必须放行而不是清空。
   *
   * <p>无表单上下文时（例如 LLM schema 生成场景，参照
   * {@code ConstraintsOfValueType.getMetaStepQuietly()} 的空上下文 fallback）候选要全量返回，
   * 否则会出现「选了类型却没得选」的死角。
   */
  @Test
  public void nullTypeIsPassThroughNotEmpty() {
    List<VariableDefinitionConfig.BasicDescriptor> all = allDescriptors();

    for (VariableDefinitionConfig.BasicDescriptor desc : all) {
      Assert.assertTrue("type 为空时必须放行 " + desc.getClass().getSimpleName(),
        VariableDefinitionConfig.supports(null, desc));
    }
    Assert.assertEquals("type 为空时 filter 应原样返回全部候选",
      all, VariableDefinitionConfig.filter(null, all));

    // 另一侧：type 有值但 descriptor 缺失，不该放行
    Assert.assertFalse(VariableDefinitionConfig.supports(VariableType.STRING, null));
    Assert.assertNull(VariableDefinitionConfig.filter(VariableType.STRING, null));
  }

  /** 过滤结果与矩阵一致，且 {@code supports} 与 {@code filter} 两个入口不会各说各话 */
  @Test
  public void supportsAndFilterAgreeWithTheMatrix() {
    List<VariableDefinitionConfig.BasicDescriptor> all = allDescriptors();

    for (VariableType type : VariableType.values()) {
      List<Class<? extends VariableDefinitionConfig.BasicDescriptor>> expected =
        VariableDefinitionConfig.definitionsOf(type);
      List<VariableDefinitionConfig.BasicDescriptor> filtered =
        VariableDefinitionConfig.filter(type, all);

      Assert.assertEquals("VariableType." + type + " 的过滤结果数量与矩阵不符",
        expected.size(), filtered.size());

      for (VariableDefinitionConfig.BasicDescriptor desc : all) {
        boolean inMatrix = expected.contains(desc.getClass());
        Assert.assertEquals("supports(" + type + ", " + desc.getClass().getSimpleName() + ") 与矩阵不符",
          inMatrix, VariableDefinitionConfig.supports(type, desc));
        Assert.assertEquals("filter(" + type + ") 对 " + desc.getClass().getSimpleName() + " 的取舍与矩阵不符",
          inMatrix, filtered.contains(desc));
      }
    }
  }

  /**
   * 覆盖性检查（矩阵内部 → 外部）：{@code TYPE_DEFINITIONS} 里登记的每个 descriptor
   * 都必须是真实存在于扩展体系中的，防止写了不存在的类（编译期能过，运行期空转）。
   *
   * <p>依赖 TIS 运行期（descriptor 需被 ExtensionFinder 扫到）。纯单测环境没有 TIS 上下文时
   * <b>跳过而非失败</b>，避免误报；在带完整 harness 的集成测试里才会真正生效。
   */
  @Test
  public void everyBasicDescriptorInRegistryIsReachable() {
    List<Descriptor<VariableDefinitionConfig>> descriptors = null;
    try {
      TIS tis = TIS.get();
      if (tis != null) {
        DescriptorExtensionList<VariableDefinitionConfig, Descriptor<VariableDefinitionConfig>> list =
          tis.getDescriptorList(VariableDefinitionConfig.class);
        // DescriptorExtensionList 是惰性的：真正的扩展扫描发生在 isEmpty()/iterator() 上。
        // 必须在这个 try 里就把它物化，否则扫描异常会漏到 try 外面。
        descriptors = (list == null || list.isEmpty()) ? null : new ArrayList<>(list);
      }
    } catch (Throwable t) {
      descriptors = null;
    }
    Assume.assumeTrue("当前环境未初始化 TIS / 未扫到 descriptor（JDK17 下通常还缺 "
      + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", descriptors != null && !descriptors.isEmpty());

    Set<Class<? extends VariableDefinitionConfig.BasicDescriptor>> registered = new HashSet<>();
    for (List<Class<? extends VariableDefinitionConfig.BasicDescriptor>> val
      : VariableDefinitionConfig.TYPE_DEFINITIONS.values()) {
      registered.addAll(val);
    }

    Set<Class<?>> scanned = new HashSet<>();
    for (Descriptor<VariableDefinitionConfig> desc : descriptors) {
      if (desc instanceof VariableDefinitionConfig.BasicDescriptor) {
        scanned.add(desc.getClass());
      }
    }

    // 双向相等，而不是只断言「登记表包含扫出来的」：后者有个盲区——若扫描结果里
    // 一个 BasicDescriptor 都没有（漏扫、类名写错），循环体一次都不执行，测试会假通过。
    // 双向相等同时覆盖两种失败：少了 = 新增子类漏登记；多了 = 登记了不存在的类。
    Assert.assertEquals("扫到的定义方式与 TYPE_DEFINITIONS 登记的必须完全一致"
        + "（少了=新增子类漏登记，多了=登记了不存在/未登记的定义方式）",
      new HashSet<>(registered), scanned);
  }

  // ===================================================================
  // 辅助
  // ===================================================================

  /** 7 个定义方式的 descriptor 实例。构造仅走反射，无 TIS 依赖，单测里可直接 new */
  private static List<VariableDefinitionConfig.BasicDescriptor> allDescriptors() {
    return List.of(//
      new StaticConfig.DefaultDescriptor() //
      , new ObjectPropertyConfig.DefaultDescriptor() //
      , new ObjectSetAggregationConfig.DefaultDescriptor() //
      , new ObjectSetDefinitionConfig.DefaultDescriptor() //
      , new SQLQueryConfig.DefaultDescriptor() //
      , new VariableTransformationConfig.DefaultDescriptor() //
      , new FunctionConfig.DefaultDescriptor());
  }

  @SafeVarargs
  private static void assertDefinitions(VariableType type,
                                        Class<? extends VariableDefinitionConfig.BasicDescriptor>... expected) {
    List<Class<? extends VariableDefinitionConfig.BasicDescriptor>> actual =
      VariableDefinitionConfig.definitionsOf(type);
    // 用 Set 比较，不依赖表的书写顺序；失败信息里排序以便人眼对比
    Set<Class<? extends VariableDefinitionConfig.BasicDescriptor>> actualSet = new HashSet<>(actual);
    Set<Class<? extends VariableDefinitionConfig.BasicDescriptor>> expectedSet =
      new HashSet<>(Arrays.asList(expected));

    Assert.assertEquals("VariableType." + type + " 的可用定义方式与设计矩阵不一致",
      new HashSet<>(nameSet(expectedSet)), new HashSet<>(nameSet(actualSet)));
    Assert.assertEquals("VariableType." + type + " 的可用定义方式与设计矩阵不一致",
      expectedSet.size(), actualSet.size());
  }

  private static Set<String> nameSet(Set<Class<? extends VariableDefinitionConfig.BasicDescriptor>> clazzes) {
    Set<String> names = new HashSet<>();
    for (Class<?> clazz : new ArrayList<Class<?>>(clazzes)) {
      names.add(clazz.getSimpleName());
    }
    return names;
  }
}
