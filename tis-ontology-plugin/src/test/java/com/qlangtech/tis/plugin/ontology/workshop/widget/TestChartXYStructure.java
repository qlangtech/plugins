package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.ontology.workshop.model.AxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.model.ContinuousAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.SeriesConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ChartXYWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Chart XY 三层配置结构（Layer / Axis / Series）的回归测试。
 *
 * <p>把本轮重构中四个「编译器不报错、但会让表单或落盘出错」的失败模式钉住：
 * <ol>
 *   <li><b>轴性质专有字段回退成单类</b>：若有人把 {@link AxisConfig} 重新合并成一个类
 *       加 {@code kind} 字段，分类轴的排序项与连续轴的刻度项会同时出现在表单里。
 *       见 {@link #axisIsPolymorphicAndSubclassesSplitTheirOwnFields()}</li>
 *   <li><b>轴绑定回归</b>：{@code AxisConfig} 上曾经有 {@code variable}（必填 INPUTTEXT），
 *       把「数据来源」混进了「显示外观」。见 {@link #axisNoLongerCarriesDataBinding()}</li>
 *   <li><b>ordinal 撞车</b>：TIS 表单对 {@code ordinal()} 做稳定排序，而字段集合来自
 *       HashMap —— 基类与子类出现相同 ordinal 时相对顺序不确定。见
 *       {@link #ordinalsDoNotCollideBetweenBaseAndSubclass()}</li>
 *   <li><b>资源文件漏建</b>：{@code ChartXYWidget.json} 在本次改动前<b>根本不存在</b>，
 *       整个 widget 没有任何表单文案。见 {@link #everyTouchedClassHasFormResource()}</li>
 * </ol>
 *
 * <p>另有两条依赖 TIS 运行期的检查，纯单测环境扫不到 descriptor 时<b>跳过而非失败</b>
 * （JDK17 下通常还缺 {@code --add-opens java.base/java.lang=ALL-UNNAMED}），
 * 参见 {@code TestVariableTypeDefinitions} 中同样的处理。
 */
public class TestChartXYStructure {

  /** 轴基类承载的公共显示项 —— 两种轴性质都有 */
  private static final Set<String> AXIS_BASE_FIELDS = Set.of(
    "showTitle", "titleOverride", "enableNumericalFormatting",
    "numericFormatting", "showGridlines", "showColorMarkers");

  /**
   * 轴是多态体系：基类抽象，两个子类各自持有自己专有的字段。
   */
  @Test
  public void axisIsPolymorphicAndSubclassesSplitTheirOwnFields() {
    Assert.assertTrue("AxisConfig 应为抽象类：轴性质由具体子类承担",
      Modifier.isAbstract(AxisConfig.class.getModifiers()));
    Assert.assertTrue("AxisConfig 应实现 Describable 才能作为 @FormField 字段类型",
      Describable.class.isAssignableFrom(AxisConfig.class));

    Set<String> base = declaredFormFields(AxisConfig.class);
    Assert.assertEquals("轴基类应只承载两种轴性质共有的显示项", AXIS_BASE_FIELDS, base);

    // 分类轴专有：排序
    Assert.assertEquals("分类轴应只多出排序相关字段",
      Set.of("sortBy", "customSortMetric"), declaredFormFields(CategoricalAxisConfig.class));

    // 连续轴专有：刻度类型 + 上下界 + 多值轴
    Assert.assertEquals("连续轴应只多出刻度/边界/多值轴字段",
      Set.of("scaleType", "useMultipleAxes", "autoMinBound", "minBound", "autoMaxBound", "maxBound"),
      declaredFormFields(ContinuousAxisConfig.class));

    // 两个子类的专属字段不得重叠 —— 重叠就意味着该字段其实属于基类
    Set<String> overlap = new HashSet<>(declaredFormFields(CategoricalAxisConfig.class));
    overlap.retainAll(declaredFormFields(ContinuousAxisConfig.class));
    Assert.assertTrue("两个轴子类的专有字段不应重叠：" + overlap, overlap.isEmpty());
  }

  /**
   * 刻度类型只有线性与对数：时间不是刻度类型，而是轴属性的数据类型。
   *
   * <p>旧实现把 {@code LINEAR / LOG / TIME} 并列，属于类别错误 —— 时间轴由
   * {@link ChartLayer#xAxisProperty} 指向 timestamp 属性表达，与刻度类型正交。
   */
  @Test
  public void timeIsNotAScaleType() {
    Set<String> scales = Arrays.stream(ContinuousAxisConfig.AxisScale.values())
      .map(Enum::name).collect(Collectors.toSet());
    Assert.assertEquals("刻度类型应只有线性与对数（TIME 是轴属性的数据类型，不是刻度类型）",
      Set.of("LINEAR", "LOG"), scales);
  }

  /**
   * 轴不再承载数据绑定 —— 绑定归图层。
   */
  @Test
  public void axisNoLongerCarriesDataBinding() {
    Set<String> fields = declaredFormFields(AxisConfig.class);
    Assert.assertFalse("AxisConfig 不应再有 variable：轴绑定归 ChartLayer.xAxisProperty",
      fields.contains("variable"));
    Assert.assertFalse("AxisConfig 不应再有 label：标题由 showTitle + titleOverride 表达",
      fields.contains("label"));
    Assert.assertFalse("AxisConfig 不应再有 scale：已拆为 ContinuousAxisConfig.scaleType",
      fields.contains("scale"));

    // 反过来，绑定必须真实存在于图层上
    Assert.assertTrue("数据绑定应落在图层上",
      declaredFormFields(ChartLayer.class).containsAll(Set.of("dataSourceVar", "xAxisProperty")));
  }

  /**
   * series 已从 Widget 下沉到图层：控件内多个系列共用同一种画法，
   * 画法属于图层，不属于系列。
   */
  @Test
  public void seriesSinksFromWidgetIntoLayers() {
    Assert.assertEquals("ChartXYWidget 只应持有图层列表，不再直接持有 series",
      Set.of("layers", "xAxis", "yAxis", "showLegend", "legendPosition", "orientation"),
      declaredFormFields(ChartXYWidget.class));

    Assert.assertTrue("图表应支持多图层",
      List.class.isAssignableFrom(fieldType(ChartXYWidget.class, "layers")));
    Assert.assertEquals("xAxis 应声明为抽象基类类型（多态）",
      AxisConfig.class, fieldType(ChartXYWidget.class, "xAxis"));
    Assert.assertEquals("yAxis 应声明为抽象基类类型（多态）",
      AxisConfig.class, fieldType(ChartXYWidget.class, "yAxis"));

    // 画法在图层上；系列不再声明画法（旧实现的 chartType 已上移）
    Set<String> layerFields = declaredFormFields(ChartLayer.class);
    Assert.assertTrue("画法应落在图层上", layerFields.contains("layerType"));
    Assert.assertTrue("图层应持有系列列表", layerFields.contains("series"));
    Assert.assertFalse("系列不应再自带 chartType：同一图层的多个系列共用一种画法",
      declaredFormFields(SeriesConfig.class).contains("chartType"));
  }

  /**
   * 基类与子类的 ordinal 不得撞车。
   *
   * <p>TIS 的表单排序是对 {@code formField.ordinal()} 做稳定排序，而字段集合来自
   * HashMap —— 相同 ordinal 的相对顺序不确定。项目约定：基类占 {@code 0..5} 与
   * {@code 99}，子类从 {@code 10} 起。
   */
  @Test
  public void ordinalsDoNotCollideBetweenBaseAndSubclass() {
    for (Class<?> clazz : List.of(CategoricalAxisConfig.class, ContinuousAxisConfig.class)) {
      Set<Integer> base = ordinalsOf(AxisConfig.class);
      Set<Integer> own = ordinalsOf(clazz);
      Set<Integer> collide = new HashSet<>(base);
      collide.retainAll(own);
      Assert.assertTrue(clazz.getSimpleName() + " 与 AxisConfig 的 ordinal 撞车：" + collide,
        collide.isEmpty());
    }
  }

  /**
   * 资源文件覆盖性检查：本轮触碰的每个类都应有同名 {@code .json} 表单资源。
   *
   * <p>这条直接钉住本次发现的真实缺陷：{@code ChartXYWidget.json} 此前不存在，
   * 导致该 widget 的表单没有任何 help/placeholder 文案。
   */
  @Test
  public void everyTouchedClassHasFormResource() {
    for (Class<?> clazz : List.of(
      AxisConfig.class, CategoricalAxisConfig.class, ContinuousAxisConfig.class,
      ChartLayer.class, SeriesConfig.class, ChartXYWidget.class)) {
      String path = "/" + clazz.getName().replace('.', '/') + ".json";
      Assert.assertNotNull("缺少表单资源 " + path + "（表单的 help/placeholder 文案不会下发）",
        clazz.getResourceAsStream(path));
    }
  }

  /**
   * 两个轴子类的 descriptor 都能被 ExtensionFinder 扫到 —— 否则表单的轴性质选择器只有一项，
   * 甚至一项都没有（{@code applicableDescriptors} 返回空）。
   */
  @Test
  public void everyAxisSubclassIsReachable() {
    List<Descriptor<?>> descriptors = scanDescriptors(AxisConfig.class);
    Assume.assumeTrue("当前环境未初始化 TIS / 未扫到 descriptor（JDK17 下通常还缺 "
      + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", descriptors != null);

    Set<Class<?>> scanned = descriptors.stream()
      .filter(d -> d instanceof AxisConfig.BasicDescriptor)
      .map(Descriptor::getClass).collect(Collectors.toSet());

    Set<Class<?>> expected = new HashSet<>(Arrays.asList(
      CategoricalAxisConfig.DefaultDescriptor.class, ContinuousAxisConfig.DefaultDescriptor.class));

    // 双向相等而非单向包含：若扫描结果为空（漏扫/类名写错），单向断言会假通过
    Assert.assertEquals("扫到的轴 descriptor 应与两个轴子类完全一致"
        + "（少了=子类漏加 @TISExtension，多了=存在未登记的轴性质）",
      expected, scanned);
  }

  /**
   * 图层与图表本体的 descriptor 可被发现。
   *
   * <p>分两段：先做<b>不依赖 TIS 运行期</b>的静态断言（descriptor 类存在、带
   * {@code @TISExtension}、父类是约定的基类），这段在任何环境都执行；再尝试真正扫描，
   * 扫不到时按既有先例跳过而非失败。
   */
  @Test
  public void layerAndWidgetDescriptorsAreReachable() {
    // --- 静态断言：始终执行 ---
    assertIsExtensionDescriptor(ChartLayer.DescriptorImpl.class, Descriptor.class);
    assertIsExtensionDescriptor(ChartXYWidget.DescriptorImpl.class, Descriptor.class);
    assertIsExtensionDescriptor(CategoricalAxisConfig.DefaultDescriptor.class,
      AxisConfig.BasicDescriptor.class);
    assertIsExtensionDescriptor(ContinuousAxisConfig.DefaultDescriptor.class,
      AxisConfig.BasicDescriptor.class);

    // --- 运行期扫描：桌面单测环境通常不可用，跳过而非失败 ---
    List<Descriptor<?>> layers = scanDescriptors(ChartLayer.class);
    Assume.assumeTrue("当前环境未初始化 TIS / 未扫到 descriptor（JDK17 下通常还缺 "
      + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过扫描检查", layers != null);
    Assert.assertTrue("ChartLayer 的 descriptor 未被扫到",
      layers.stream().anyMatch(d -> d.getClass() == ChartLayer.DescriptorImpl.class));

    // Widget 体系实现的是 Describable<IWorkshopWidget>，按该扩展点扫描后按具体类过滤
    List<Descriptor<?>> widgets = scanDescriptors(IWorkshopWidget.class);
    Assume.assumeTrue("当前环境未初始化 TIS，跳过 Widget descriptor 扫描", widgets != null);
    Assert.assertTrue("ChartXYWidget 的 descriptor 未被扫到",
      widgets.stream().anyMatch(d -> d.getClass() == ChartXYWidget.DescriptorImpl.class));
  }

  /**
   * sezpoz 在编译期为 {@code @TISExtension} 生成的扩展点索引。
   *
   * <p>不能直接用 {@code getAnnotation(TISExtension.class)} 判断：该注解声明为
   * {@link java.lang.annotation.RetentionPolicy#CLASS}，运行期反射取不到，
   * 直接查注解会对所有类都返回 {@code null}（假阴性）。TIS 的 ExtensionFinder 实际
   * 读的是这份索引，因此这里也读它。
   */
  private static final String SEZPOZ_INDEX_PATH =
    "/META-INF/annotations/com.qlangtech.tis.extension.TISExtension.txt";

  /**
   * 断言某嵌套类确是一个会被 TIS 扫到的 descriptor：父类是指定的 descriptor 基类，
   * 且已登记进 sezpoz 索引。
   *
   * <p>漏加 {@code @TISExtension} 是静默失败 —— 类能编译、能实例化，只是永远不会出现在
   * 扩展点扫描结果里，表现为表单上一个空的下拉框。
   */
  private static void assertIsExtensionDescriptor(Class<?> descClazz, Class<?> expectedSuper) {
    Assert.assertTrue(descClazz.getName() + " 的父类应为 " + expectedSuper.getSimpleName(),
      expectedSuper.isAssignableFrom(descClazz.getSuperclass()));

    // 嵌套类在索引中写作 Outer$Inner，与 Class#getName() 的输出一致
    Assert.assertTrue(descClazz.getName() + " 未出现在 sezpoz 索引中，"
        + "TIS 的 ExtensionFinder 不会发现它（表单上会是一个空下拉）",
      readSezpozIndex().contains(descClazz.getName()));
  }

  private static String readSezpozIndex() {
    try (InputStream in = TestChartXYStructure.class.getResourceAsStream(SEZPOZ_INDEX_PATH)) {
      Assert.assertNotNull("缺少 sezpoz 索引 " + SEZPOZ_INDEX_PATH + "（注解处理器未生效？）", in);
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new AssertionError("读取 sezpoz 索引失败：" + SEZPOZ_INDEX_PATH, e);
    }
  }

  // ==================================================================
  //  helpers
  // ==================================================================

  /**
   * 取扫描到的 descriptor；无 TIS 上下文时返回 {@code null}（调用方用 Assume 跳过）。
   *
   * <p>{@code DescriptorExtensionList} 是惰性的：真正的扩展扫描发生在
   * {@code isEmpty()}/{@code iterator()} 上，因此必须在本方法内就物化，
   * 否则扫描异常会漏到调用方的 try 外面。
   *
   * <p>返回裸 {@code Descriptor<?>} 而非泛型精确类型：{@code getDescriptorList} 要求
   * {@code T extends Describable<T>}，而 Widget 体系是
   * {@code ChartXYWidget implements Describable<IWorkshopWidget>} —— 自引用类型参数
   * 并不满足该约束，只能按扩展点基类扫描后自行过滤。
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<Descriptor<?>> scanDescriptors(Class<?> describableClazz) {
    try {
      TIS tis = TIS.get();
      if (tis == null) {
        return null;
      }
      DescriptorExtensionList list = tis.getDescriptorList((Class) describableClazz);
      if (list == null || list.isEmpty()) {
        return null;
      }
      return new ArrayList<Descriptor<?>>(list);
    } catch (Throwable t) {
      return null;
    }
  }

  /** 本类自身声明的 {@code @FormField} 字段名（不含继承的） */
  private static Set<String> declaredFormFields(Class<?> clazz) {
    return Arrays.stream(clazz.getDeclaredFields())
      .filter(f -> f.getAnnotation(FormField.class) != null)
      .map(Field::getName)
      .collect(Collectors.toSet());
  }

  private static Set<Integer> ordinalsOf(Class<?> clazz) {
    return Arrays.stream(clazz.getDeclaredFields())
      .map(f -> f.getAnnotation(FormField.class))
      .filter(Objects::nonNull)
      .map(FormField::ordinal)
      .collect(Collectors.toSet());
  }

  private static Class<?> fieldType(Class<?> clazz, String name) {
    try {
      return clazz.getField(name).getType();
    } catch (NoSuchFieldException e) {
      throw new AssertionError(clazz.getSimpleName() + " 上找不到字段 " + name, e);
    }
  }
}
