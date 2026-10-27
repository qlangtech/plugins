package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.extension.ValueChangePipe;
import com.qlangtech.tis.extension.impl.PropValRewrite;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.AxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.model.ContinuousAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.TisPluginFormTestSupport;
import com.qlangtech.tis.plugin.ontology.workshop.model.MetricConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.NumericFormatting;
import com.qlangtech.tis.plugin.ontology.workshop.model.SecondaryMetricConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.SeriesConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.TemporalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.XAxisBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ChartXYWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractAxisTitleConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractDecimalBound;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractLayerType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractLegendConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractNumericFormatting;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractSeriesAggregation;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.ApproxUniqueCountAggregation;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AverageAggregation;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AxisTitleConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AxisTitleOff;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.BarLayerType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.CountAggregation;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.DecimalBoundConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.DecimalBoundOff;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.LegendConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.LegendOff;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.LineLayerType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.MaxAggregation;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.MinAggregation;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.NumericFormattingOff;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.ScatterLayerType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.SumAggregation;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.FormVaildateType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

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
 * <p>把重构中六个「编译器不报错、但会让表单或落盘出错」的失败模式钉住：
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
 *   <li><b>X 轴属性的级联断开</b>：{@code xAxisProperty} 是「数据源变量所指对象类型的属性名」，
 *       候选必须随 {@code dataSourceVar} 级联。级联要求 primary 是同层普通字段，若把
 *       {@code dataSourceVar} 埋回子表单（或把 {@code xAxisProperty} 改回 INPUTTEXT），
 *       级联静默失效、只能手打属性名，打错要到取数时才报。见
 *       {@link #xAxisPropertyCascadesFromDataSourceVar()}</li>
 *   <li><b>X 轴分桶退回「粒度 + 单位数」扁平组合</b>：分桶方式由 X 轴属性的数据类型决定
 *       —— 粒度与粒度单位数对分类轴无意义，取值方式对时间轴无意义，压成单类会让两组
 *       字段同时出现在表单里。见 {@link #xAxisBucketingIsPolymorphic()}</li>
 * </ol>
 *
 * <p>另有两条依赖 TIS 运行期的检查，纯单测环境扫不到 descriptor 时<b>跳过而非失败</b>
 * （JDK17 下通常还缺 {@code --add-opens java.base/java.lang=ALL-UNNAMED}），
 * 参见 {@code TestVariableTypeDefinitions} 中同样的处理。
 */
public class TestChartXYStructure {

  /**
   * 轴基类承载的公共显示项 —— 两种轴性质都有。
   *
   * <p>{@code titleOverride} 已下沉到 {@code showTitle} 的「显示」子类，
   * {@code enableNumericalFormatting} 已由 {@code numericFormatting} 的 off 子类取代，
   * 两者都不再是顶层字段。
   */
  private static final Set<String> AXIS_BASE_FIELDS = Set.of(
    "showTitle", "numericFormatting", "showGridlines", "showColorMarkers");

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

    // 分类轴专有：排序（CUSTOM 与 customSortMetric 已删除 —— 对象集聚合链路里没有
    // 「给属性赋予数值排序值」的实现，留着只会让用户配一个永远不生效的字段）
    Assert.assertEquals("分类轴应只多出排序字段",
      Set.of("sortBy"), declaredFormFields(CategoricalAxisConfig.class));

    // 连续轴专有：刻度类型 + 上下界（各一个多态字段）+ 多值轴。
    // autoMinBound / autoMaxBound 两个旁挂开关已并入 minBound / maxBound 的 off 子类。
    Assert.assertEquals("连续轴应只多出刻度/边界/多值轴字段",
      Set.of("scaleType", "useMultipleAxes", "minBound", "maxBound"),
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

    // 反过来，绑定必须真实存在于图层上，且数据源与分组键同层 —— 级联的硬前提
    Set<String> layerFields = declaredFormFields(ChartLayer.class);
    Assert.assertTrue("分组键应落在图层上", layerFields.contains("xAxisProperty"));
    Assert.assertTrue("数据源绑定应与分组键同层扁平（ValueChangePipe 只认同层普通字段）",
      layerFields.contains("dataSourceVar"));
  }

  /**
   * {@code xAxisProperty} 的候选必须随 {@code dataSourceVar} 级联。
   *
   * <p>钉住本次改动的核心，两个方向都防：
   * <ul>
   *   <li>{@code xAxisProperty} 退回 INPUTTEXT ⇒ 级联结果无处可去，用户回到手打属性名</li>
   *   <li>{@code dataSourceVar} 重新下沉进子表单或多态子类 ⇒ pipe 的 primary 不再是同层
   *       普通字段，{@code RootFormProperties.getInstancePropsJson} 静默跳过，级联失效</li>
   * </ul>
   *
   * <p>这里只断言<b>结构</b>（字段形态 + pipe 注册），不触发选项求值 —— 后者需要 TIS 运行期
   * 上下文（{@code ontology_<domain>} / {@code workshopModuleName_<module>} 等 extraParam）。
   */
  @Test
  public void xAxisPropertyCascadesFromDataSourceVar() {
    Assert.assertEquals("dataSourceVar 是级联 primary，必须是普通字符串字段",
      String.class, fieldType(ChartLayer.class, "dataSourceVar"));
    Assert.assertEquals("dataSourceVar 必须可选（选项来自当前 Module 的对象集变量）",
      FormFieldType.SELECTABLE, formFieldOf(ChartLayer.class, "dataSourceVar").type());
    Assert.assertEquals("xAxisProperty 必须是 SELECTABLE：INPUTTEXT 时级联结果无处落地",
      FormFieldType.SELECTABLE, formFieldOf(ChartLayer.class, "xAxisProperty").type());
    Assert.assertTrue("xAxisProperty 仍必填（取数与聚合下推都以它为分组键）",
      Arrays.asList(formFieldOf(ChartLayer.class, "xAxisProperty").validate())
        .contains(Validator.require));

    Set<String> layerFields = declaredFormFields(ChartLayer.class);
    Assert.assertFalse("多态外壳 dataInput 应已删除", layerFields.contains("dataInput"));
    Assert.assertFalse("旁挂的 dataInputType 不得复活", layerFields.contains("dataInputType"));

    Descriptor<?> descriptor;
    try {
      descriptor = new ChartLayer.DescriptorImpl();
    } catch (Throwable t) {
      throw new AssertionError("ChartLayer.DescriptorImpl 应可无参实例化（TIS 反射创建 descriptor 的前提）", t);
    }

    ValueChangePipe pipe = descriptor.getValueChangePipe(ChartLayer.KEY_DATA_SOURCE_VAR, false);
    Assert.assertNotNull("ChartLayer.DescriptorImpl 必须为 dataSourceVar 注册 valueChangePipe",
      pipe);
    Assert.assertArrayEquals("xAxisProperty 应是 dataSourceVar 的（唯一）级联目标",
      new String[]{ChartLayer.KEY_X_AXIS_PROPERTY}, pipe.getToField());

    // 两个 SELECTABLE 都必须注册选项供给：未注册时 DescriptorsJSON.getSelectOptions 抛异常，
    // 整张表单打不开。求值本身需要运行期上下文，故只认「未注册」这条报错。
    for (String field : List.of(ChartLayer.KEY_DATA_SOURCE_VAR, ChartLayer.KEY_X_AXIS_PROPERTY)) {
      try {
        descriptor.getSelectOptions(field);
      } catch (IllegalStateException e) {
        Assert.assertFalse("字段 " + field + " 未 registerSelectOptions：" + e.getMessage(),
          String.valueOf(e.getMessage()).contains("has not been register"));
      } catch (Throwable ignored) {
        // 已注册，仅选项求值需要 TIS 运行期上下文
      }
    }
  }

  /**
   * X 轴分桶是多态体系：分桶方式由 X 轴属性的数据类型决定，不压成「粒度 + 单位数」的扁平组合。
   *
   * <p>两种方式的字段互不适用：时间轴的「粒度 + 粒度单位数」在分类轴上无处可填，
   * 分类轴的「取值方式」在时间轴上同样没有意义。{@code limit}（桶数上限）是唯一有证据
   * 属公共的字段 —— 每种分桶都产出桶，且都被同一个硬上限约束 —— 故落在基类。
   * 判据同 {@link #axisIsPolymorphicAndSubclassesSplitTheirOwnFields()}：
   * <b>子类专有字段重叠，即意味着该字段其实属于基类</b>。
   */
  @Test
  public void xAxisBucketingIsPolymorphic() {
    Assert.assertTrue("XAxisBucketing 应为抽象类：分桶方式由具体子类承担",
      Modifier.isAbstract(XAxisBucketing.class.getModifiers()));
    Assert.assertTrue("XAxisBucketing 应实现 Describable 才能作为 @FormField 字段类型",
      Describable.class.isAssignableFrom(XAxisBucketing.class));

    // 图层声明抽象类型 —— 这是 TIS 渲染 impl 选择器的前提
    Assert.assertEquals("ChartLayer.xAxisBucketing 应声明为抽象基类类型（多态）",
      XAxisBucketing.class, fieldType(ChartLayer.class, "xAxisBucketing"));

    // 基类恰好只承载「桶数上限」这一项有证据的公共字段
    Assert.assertEquals("分桶基类应只承载桶数上限",
      Set.of("limit"), declaredFormFields(XAxisBucketing.class));

    // 时间分桶专有：粒度 + 粒度单位数
    Assert.assertEquals("时间分桶应只多出粒度与粒度单位数",
      Set.of("granularity", "unitValue"), declaredFormFields(TemporalBucketing.class));

    // 分类分桶专有：取值方式
    Assert.assertEquals("分类分桶应只多出取值方式",
      Set.of("mode"), declaredFormFields(CategoricalBucketing.class));

    // 两个子类的专有字段不得重叠 —— 重叠就意味着该字段其实属于基类
    Set<String> overlap = new HashSet<>(declaredFormFields(TemporalBucketing.class));
    overlap.retainAll(declaredFormFields(CategoricalBucketing.class));
    Assert.assertTrue("两个分桶子类的专有字段不应重叠：" + overlap, overlap.isEmpty());
  }

  /**
   * 分桶相关字段的取值集合都是固定的，必须是 ENUM / 整数输入 —— 不得退化成手输。
   *
   * <p>直接钉住 CLAUDE.md「固定取值用 ENUM」：粒度是年/季/月/周/天/时/分/秒八个固定选项，
   * 取值方式是二选一。若有人把它们改成 {@code INPUTTEXT}（用户不知道有哪些合法取值）
   * 或 {@code SELECTABLE}（暗示选项来自运行时数据，实为固定），本条即失败。
   */
  @Test
  public void bucketingOptionsAreFixedNotFreeText() {
    Assert.assertEquals("时间粒度必须是 ENUM（选项固定：年/季/月/周/天/时/分/秒）",
      FormFieldType.ENUM, formFieldOf(TemporalBucketing.class, "granularity").type());
    Assert.assertEquals("取值方式必须是 ENUM（选项固定：前 N 个高频值 / 精确去重值）",
      FormFieldType.ENUM, formFieldOf(CategoricalBucketing.class, "mode").type());

    Assert.assertEquals("粒度单位数必须是整数输入",
      FormFieldType.INT_NUMBER, formFieldOf(TemporalBucketing.class, "unitValue").type());
    Assert.assertEquals("桶数上限必须是整数输入",
      FormFieldType.INT_NUMBER, formFieldOf(XAxisBucketing.class, "limit").type());
  }

  /**
   * json 里声明的 {@code enum} 取值与 Java 枚举常量<b>必须严格一一对应</b>。
   *
   * <p>钉住本次发现并修掉的真实缺陷：{@code TemporalBucketing.json} 列了 8 个粒度
   * （含 {@code SECOND}），而 Java 枚举当时只有 7 个 —— {@code FormFieldType} 提交时走
   * {@code Enum.valueOf(fieldClazz, val)}，用户在下拉里选中那个多出来的选项，
   * <b>提交必抛 {@code IllegalArgumentException}</b>。这类不一致编译器不管、单测不覆盖就没人管。
   *
   * <p>两个方向都要查，且方向不同、后果不同：
   * <ul>
   *   <li>json 有、Java 没有 → 死选项，选中即提交报错</li>
   *   <li>Java 有、json 没有 → 死常量，下拉里不可达（json 一旦声明了 {@code enum}，
   *       选项就<b>只</b>来自 json，不再回退到反射枚举常量）</li>
   * </ul>
   */
  @Test
  public void jsonEnumOptionsMatchJavaEnumConstants() {
    for (Class<?> clazz : List.of(TemporalBucketing.class, CategoricalBucketing.class)) {
      JSONObject resource = formResourceOf(clazz);
      for (String fieldName : resource.keySet()) {
        JSONObject entry = resource.getJSONObject(fieldName);
        if (entry == null || !entry.containsKey("enum")) {
          continue;
        }
        Field field = fieldOf(clazz, fieldName);
        Assert.assertNotNull(clazz.getSimpleName() + " 的 json 为字段 " + fieldName
          + " 声明了 enum，但类（含父类）上找不到该字段", field);

        Class<?> fieldType = field.getType();
        Assert.assertTrue(clazz.getSimpleName() + "." + fieldName
          + " 声明了 enum 选项，字段类型却不是枚举：" + fieldType, fieldType.isEnum());

        Set<String> constants = Arrays.stream(fieldType.getEnumConstants())
          .map(Object::toString).collect(Collectors.toSet());
        Set<String> declared = entry.getJSONArray("enum").stream()
          .map(o -> ((JSONObject) o).getString("val")).collect(Collectors.toSet());

        Set<String> dangling = new HashSet<>(declared);
        dangling.removeAll(constants);
        Assert.assertTrue(clazz.getSimpleName() + "." + fieldName
          + " 的 json enum 中存在 Java 枚举没有的取值（用户选中后提交会抛 IllegalArgumentException）："
          + dangling, dangling.isEmpty());

        Set<String> unexposed = new HashSet<>(constants);
        unexposed.removeAll(declared);
        Assert.assertTrue(clazz.getSimpleName() + "." + fieldName
          + " 有枚举常量未在 json enum 中列出（json 声明了 enum 后选项只来自 json，该常量下拉里不可达）："
          + unexposed, unexposed.isEmpty());
      }
    }
  }

  /**
   * 分桶的桶数上限不得高于对象集聚合 API 的硬上限。
   *
   * <p>本地默认值取满 {@code MAX_LIMIT} 是有意的：默认不该替使用者悄悄丢数据，
   * 超限应由聚合侧报错。若有人把默认值调高到硬上限之上，表单会给出一个必然失败的默认配置。
   */
  @Test
  public void bucketingLimitDefaultsWithinHardCap() {
    Assert.assertEquals("时间分桶的桶数上限应默认取满硬上限",
      XAxisBucketing.MAX_LIMIT, new TemporalBucketing().limit.intValue());
    Assert.assertEquals("分类分桶的桶数上限应默认取「前 N 个高频值」的天花板",
      CategoricalBucketing.TOP_VALUES_LIMIT, new CategoricalBucketing().limit.intValue());
    Assert.assertTrue("分类分桶的默认上限不得高于硬上限",
      new CategoricalBucketing().limit <= XAxisBucketing.MAX_LIMIT);
    Assert.assertEquals("粒度单位数应默认为 1（每个粒度一个刻度）",
      1, new TemporalBucketing().unitValue.intValue());
  }

  /**
   * series 已从 Widget 下沉到图层：控件内多个系列共用同一种画法，
   * 画法属于图层，不属于系列。
   */
  @Test
  public void seriesSinksFromWidgetIntoLayers() {
    Assert.assertEquals("ChartXYWidget 只应持有图层列表，不再直接持有 series；"
        + "legend 位置已下沉到 showLegend 的「显示」子类，故顶层不再有 legendPosition",
      Set.of("layers", "xAxis", "yAxis", "showLegend", "orientation"),
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

    // areaOption / showLabels 已下沉到 layerType 的子类，图层上不得残留
    Assert.assertEquals("图层字段集合应与设计一致",
      Set.of("title", "dataSourceVar", "layerType", "xAxisProperty", "xAxisBucketing",
        "series", "selectionAsFilterVar"),
      layerFields);
  }

  /**
   * 图表方向是<b>整图级</b>约束：{@code HORIZONTAL} 只在「全部图层都是柱状图」时合法。
   *
   * <p>这条规则很容易在后续重构中被「顺手下沉到 {@link BarLayerType}」而破坏 —— 那只是
   * 把矛盾从「折线/散点被误配横向」变成「两个 bar 图层方向打架」，而整张图共用的那对
   * x/y scale 无法同时满足横向与纵向两类要求。规则与原因见 {@code ChartXYWidget#orientation}。
   */
  @Test
  public void horizontalOrientationRequiresAllBarLayers() {
    Assert.assertTrue("柱状图是唯一支持横向的画法", new BarLayerType().supportsHorizontal());
    Assert.assertFalse("折线图不支持横向", new LineLayerType().supportsHorizontal());
    Assert.assertFalse("散点图不支持横向", new ScatterLayerType().supportsHorizontal());

    Assert.assertNull("全部图层为柱状时应允许横向",
      chartWidgetWith(ChartXYWidget.Orientation.HORIZONTAL, new BarLayerType(), new BarLayerType())
        .orientationViolation());
    Assert.assertNull("纵向对任何画法组合都合法",
      chartWidgetWith(ChartXYWidget.Orientation.VERTICAL,
        new BarLayerType(), new LineLayerType(), new ScatterLayerType()).orientationViolation());

    String violation = chartWidgetWith(ChartXYWidget.Orientation.HORIZONTAL,
      new BarLayerType(), new ScatterLayerType()).orientationViolation();
    Assert.assertNotNull("含散点图层时横向必须被拒绝", violation);
    Assert.assertTrue("报错应指出具体图层与画法：" + violation,
      violation.contains("layer-2") && violation.contains("散点图"));

    // 约束与图层数量无关：单个折线/散点图层同样不能横向
    Assert.assertNotNull("单个折线图层也不能横向",
      chartWidgetWith(ChartXYWidget.Orientation.HORIZONTAL, new LineLayerType()).orientationViolation());
  }

  /**
   * 轴性质约束：X 必须是分类轴、Y 必须是连续轴。
   *
   * <p>这条不是「两边不许相同」——{@code x=连续 / y=分类} 这种互换配对同样会被渲染器画崩
   * （分桶标签落进线性刻度 → 空白图），所以按<b>轴位</b>断言。原因与将来放开的路径见
   * {@link ChartXYWidget#axisKindViolation()}。
   */
  @Test
  public void axisKindMustBeCategoricalXContinuousY() {
    ChartXYWidget widget = new ChartXYWidget();

    widget.xAxis = new CategoricalAxisConfig();
    widget.yAxis = new ContinuousAxisConfig();
    Assert.assertNull("分类 X + 连续 Y 是唯一合法组合", widget.axisKindViolation());

    // 两边都是分类：Y 没有数值刻度
    widget.yAxis = new CategoricalAxisConfig();
    String bothCategorical = widget.axisKindViolation();
    Assert.assertNotNull("Y 轴选分类轴必须被拒绝", bothCategorical);
    Assert.assertTrue("报错应指向 Y 轴：" + bothCategorical, bothCategorical.contains("Y 轴"));

    // 两边都是连续：X 上的字符串标签落进数值刻度（真散点图才允许，数据链路尚未落地）
    widget.xAxis = new ContinuousAxisConfig();
    widget.yAxis = new ContinuousAxisConfig();
    String bothContinuous = widget.axisKindViolation();
    Assert.assertNotNull("X 轴选连续轴必须被拒绝", bothContinuous);
    Assert.assertTrue("报错应指向 X 轴：" + bothContinuous, bothContinuous.contains("X 轴"));

    // 互换配对：「一分类一连续」但方向反了 —— 这正是「两边不许相同」拦不住的那一种
    widget.xAxis = new ContinuousAxisConfig();
    widget.yAxis = new CategoricalAxisConfig();
    Assert.assertNotNull("X 连续 + Y 分类同样必须被拒绝", widget.axisKindViolation());

    // 运行期兜底不能抛 NPE（绕过表单直接调用时字段可能未回填）
    ChartXYWidget empty = new ChartXYWidget();
    Assert.assertNotNull("轴未配置时应返回可读错误而不是 NPE", empty.axisKindViolation());
  }

  /**
   * 散点图层<b>不</b>豁免轴性质约束 —— 即使全是散点层，也必须是「分类 X + 连续 Y」。
   *
   * <p>这条钉住一个很容易被「顺手放开」的决定：{@code CategoricalAxisConfig} /
   * {@code ContinuousAxisConfig} 多态的存在理由之一就是「表达得了连续 X 轴」，于是看代码的人
   * 容易认为「散点图的连×连被 axisKindViolation 误伤了，改宽即可」。
   *
   * <p><b>不是误伤，也不是「暂未支持」</b>：Palantir 的散点图本身就是分类 X 轴。官方示例图
   * （{@code chart_xy_example.png} 第三张面板）的刻度间距恒定在 31–32px，而 {@code 5.0→7.0}、
   * {@code 8.0→10.0} 数值跨度为 2 —— 连续轴上应宽一倍。完整的度量与推理见
   * {@link ChartXYWidget#axisKindViolation()} 的 javadoc。
   *
   * <p>真正的「数值对散点」是超出 Palantir 的扩展，届时应升级为图级 chart mode
   * （与 {@link ChartXYWidget#orientation} 同思路），而不是删掉本条校验。
   */
  @Test
  public void scatterLayerDoesNotExemptAxisKindRule() {
    ChartXYWidget widget = new ChartXYWidget();
    ChartLayer scatter = new ChartLayer();
    scatter.title = "scatter-1";
    scatter.layerType = new ScatterLayerType();
    widget.layers.add(scatter);

    // 全部是散点层，且方向合法（纵向对任何画法都成立）
    Assert.assertNull("前置：单个散点图层 + 纵向本身是合法配置", widget.orientationViolation());

    // 轴性质仍按「分×连」断言：Palantir 的散点图也是分类 X 轴
    widget.xAxis = new ContinuousAxisConfig();
    widget.yAxis = new ContinuousAxisConfig();
    Assert.assertNotNull("散点图层不得让 X 轴改选连续轴：Palantir 的散点图也是分类 X 轴",
      widget.axisKindViolation());

    widget.xAxis = new CategoricalAxisConfig();
    Assert.assertNull("散点图层按「分类 X + 连续 Y」是合法形态："
        + "X 是服务端分桶出来的取值标签，与柱/线共用同一套 X 轴语义",
      widget.axisKindViolation());
  }

  /**
   * 表单层的轴性质过滤：{@code ChartXYWidget.json} 的 {@code xAxis} / {@code yAxis} 声明了
   * {@code subDescEnumFilter}，下拉里只出现对应的轴性质。
   *
   * <p>这条与 {@link #axisKindMustBeCategoricalXContinuousY()} 是<b>两道不同的闸</b>：
   * 过滤器只裁剪下发给前端的候选集，不参与保存期解析（{@code AttrValMap#createDescribable}
   * 按 impl 直接找 descriptor，不回查过滤器）。所以两边都要测，缺一条就是真漏洞。
   *
   * <p>同时校验 json 里的脚本字符串能对上真实方法名 —— 那种错法编译器不报错，
   * 只会在打开表单时抛异常或静默返回空列表（下拉一项都没有）。
   */
  @Test
  public void axisFieldsDeclareSubDescEnumFilter() {
    JSONObject json = formResourceOf(ChartXYWidget.class);

    for (String field : List.of("xAxis", "yAxis")) {
      JSONObject fieldJson = json.getJSONObject(field);
      Assert.assertNotNull(field + " 应在 ChartXYWidget.json 里有条目", fieldJson);
      String script = fieldJson.getString("subDescEnumFilter");
      Assert.assertNotNull(field + " 缺少 subDescEnumFilter：轴性质就退化成两个都可以选", script);
      Assert.assertTrue(field + " 的过滤器应调用 AxisConfig 上的静态方法，实际：" + script,
        script.startsWith("return com.qlangtech.tis.plugin.ontology.workshop.model.AxisConfig."));
    }

    // 过滤器方法必须真的存在且签名可用 —— 脚本字符串写错方法名编译器不报错
    Assert.assertTrue("categoricalOnly 应能从 json 脚本里被静态调用",
      json.getJSONObject("xAxis").getString("subDescEnumFilter")
        .contains("AxisConfig.categoricalOnly(desc)"));
    Assert.assertTrue("continuousOnly 应能从 json 脚本里被静态调用",
      json.getJSONObject("yAxis").getString("subDescEnumFilter")
        .contains("AxisConfig.continuousOnly(desc)"));
  }

  /**
   * 过滤器方法的<b>行为</b>：入参是全量轴 descriptor 时各自只留下自己那一支。
   *
   * <p>不依赖 TIS 运行期扫描 —— 手工构造 descriptor 实例即可，因此这条在单测环境永远生效。
   */
  @Test
  public void axisFiltersKeepOnlyTheirOwnKind() {
    List<Descriptor<?>> all = List.of(
      new CategoricalAxisConfig.DefaultDescriptor(),
      new ContinuousAxisConfig.DefaultDescriptor());

    List<? extends Descriptor> categorical = AxisConfig.categoricalOnly(all);
    Assert.assertEquals("categoricalOnly 应只剩分类轴", 1, categorical.size());
    Assert.assertEquals(CategoricalAxisConfig.class, categorical.get(0).clazz);

    List<? extends Descriptor> continuous = AxisConfig.continuousOnly(all);
    Assert.assertEquals("continuousOnly 应只剩连续轴", 1, continuous.size());
    Assert.assertEquals(ContinuousAxisConfig.class, continuous.get(0).clazz);

    // 入参为 null 时返回空列表而不是 NPE：PropertyType 在某些路径下可能传 null
    Assert.assertTrue("null 入参应返回空列表", AxisConfig.categoricalOnly(null).isEmpty());
    Assert.assertTrue("null 入参应返回空列表", AxisConfig.continuousOnly(null).isEmpty());
  }

  /**
   * 必填多态字段与方向枚举的<b>表单初值</b>必须写在 json 的 {@code dftVal} 上。
   *
   * <p>两条不同的通路，不能互相替代：
   * <ul>
   *   <li><b>表单新建成</b>：前端 {@code ItemPropVal#addNewEmptyItemProp(updateModel)} 在
   *       {@code !updateModel} 时用 {@code eprops.dftVal} 与各 descriptor 的
   *       {@code displayName} 逐字比对，命中才把 {@code descVal.impl} 预选中
   *       （见 {@code tis.plugin.ts}）。不写 {@code dftVal}，用户打开表单看到的是
   *       「请选择」的空下拉，必填项要手工选两次才能保存。</li>
   *   <li><b>Java 字段初值</b>：只在代码里 {@code new} 出来的实例上生效，以及作为
   *       XStream 反序列化后 {@code readResolve()} 的兜底值；前端表单完全看不到它。</li>
   * </ul>
   * 所以 {@code xAxis = new CategoricalAxisConfig()} 这类写法<b>不能</b>省掉 json 里的
   * {@code dftVal} —— 这里把两者的取值绑在一起断言，改任一侧都会红。
   */
  @Test
  public void requiredPolymorphicFieldsDeclareDftVal() {
    JSONObject json = formResourceOf(ChartXYWidget.class);

    // 1) 多态字段：dftVal 逐字等于默认子类的 displayName（前端就是按这个名字匹配 impl 的）
    Assert.assertEquals("xAxis.dftVal 必须等于分类轴 descriptor 的 displayName（否则表单预选不上）",
      TisPluginFormTestSupport.descriptorOf(CategoricalAxisConfig.class).getDisplayName(),
      json.getJSONObject("xAxis").getString("dftVal"));
    Assert.assertEquals("yAxis.dftVal 必须等于连续轴 descriptor 的 displayName（否则表单预选不上）",
      TisPluginFormTestSupport.descriptorOf(ContinuousAxisConfig.class).getDisplayName(),
      json.getJSONObject("yAxis").getString("dftVal"));

    // 2) ENUM 字段：dftVal 是枚举常量名，不是 shortComment（后者只作选项说明）
    String orientationDftVal = json.getJSONObject("orientation").getString("dftVal");
    Assert.assertEquals("orientation.dftVal 必须是枚举常量名，而不是选项说明文字",
      ChartXYWidget.Orientation.VERTICAL.name(), orientationDftVal);
    // 能被表单回填：前端提交的是选项 value，后端 Enum.valueOf 解析它
    Assert.assertEquals("dftVal 必须能被 Enum.valueOf 解析（否则保存时报非法枚举值）",
      ChartXYWidget.Orientation.VERTICAL, ChartXYWidget.Orientation.valueOf(orientationDftVal));

    // 3) 与 Java 初值保持一致：两条通路指向同一个默认值，避免「表单默认纵向、
    //    代码 new 出来是横向」这种只在一侧复现的差异
    ChartXYWidget widget = new ChartXYWidget();
    Assert.assertEquals("Java 初值应与 json dftVal 一致", ChartXYWidget.Orientation.VERTICAL, widget.orientation);
  }

  private static ChartXYWidget chartWidgetWith(ChartXYWidget.Orientation orientation,
                                               AbstractLayerType... layerTypes) {
    ChartXYWidget widget = new ChartXYWidget();
    widget.orientation = orientation;
    for (AbstractLayerType layerType : layerTypes) {
      ChartLayer layer = new ChartLayer();
      layer.title = "layer-" + (widget.layers.size() + 1);
      layer.layerType = layerType;
      widget.layers.add(layer);
    }
    return widget;
  }

  // ==================================================================
  // off / on 多态（本轮新增）
  // ==================================================================

  /**
   * 图例：抽象父类 + off / 显示两个子类。
   *
   * <p>原先「是否显示图例」是 {@code Boolean showLegend} + 平铺的 {@code legendPosition}
   * 两个独立真值来源，能拼出「关了图例却还留着位置」这种自相矛盾的落盘状态。多态化后
   * 位置字段只存在于「显示」子类里，该状态在类型层面不可表示。
   *
   * <p>默认值刻意保持「显示 + 下方」—— 多态化不该顺手改变既有图表的默认外观。
   */
  @Test
  public void legendIsPolymorphic() throws Exception {
    assertAbstractParentWithNoFields(AbstractLegendConfig.class);
    assertOffSubclass(LegendOff.class, AbstractLegendConfig.class, "不显示图例");

    Assert.assertEquals("显示子类应只持有位置字段",
      Set.of("legendPosition"), declaredFormFields(LegendConfig.class));
    Assert.assertTrue("图例位置必须是 ENUM（固定四向）",
      LegendConfig.LegendPosition.class.isEnum());
    assertHostField(new ChartXYWidget(), "showLegend", AbstractLegendConfig.class, LegendConfig.class);

    // 默认必须是「显示」而不是 off：否则多态化会让所有既有图表突然没有图例
    Assert.assertEquals("ChartXYWidget.json 的 showLegend.dftVal 必须逐字等于默认子类的 displayName",
      "Legend", formResourceOf(ChartXYWidget.class).getJSONObject("showLegend").getString("dftVal"));
    Assert.assertEquals("默认子类的 displayName 必须与 dftVal 逐字一致",
      "Legend", TisPluginFormTestSupport.descriptorOf(LegendConfig.class).getDisplayName());
  }

  /**
   * 图层画法：抽象父类 + 柱 / 折线 / 散点三个子类；画法专有字段放在对应子类里。
   *
   * <p>{@code areaOption} 只对折线有意义、{@code showLabels} 只对柱与折线有意义、
   * 散点两者都没有 —— 压成一个类会让三组字段同时出现在任何图层的表单里。
   */
  @Test
  public void layerTypeIsPolymorphicAndSubclassesOwnTheirFields() throws Exception {
    assertAbstractParentWithNoFields(AbstractLayerType.class);

    Assert.assertEquals("ChartLayer.layerType 应声明为抽象基类类型（多态）",
      AbstractLayerType.class, fieldType(ChartLayer.class, "layerType"));

    Assert.assertEquals("折线画法应持有面积选项与数值标签",
      Set.of("areaOption", "showLabels"), declaredFormFields(LineLayerType.class));
    Assert.assertEquals("柱状画法应只持有数值标签",
      Set.of("showLabels"), declaredFormFields(BarLayerType.class));
    Assert.assertEquals("散点画法没有任何画法专有配置",
      0, declaredFormFields(ScatterLayerType.class).size());

    // 双向相等：少了=子类漏加 @TISExtension（表单少一项），多了=未登记的画法
    Set<Class<?>> expected = new HashSet<>(Arrays.asList(
      BarLayerType.class, LineLayerType.class, ScatterLayerType.class));
    Assert.assertEquals("图层画法的具体子类应恰好是柱 / 折线 / 散点",
      expected, TisPluginFormTestSupport.concreteSubclassesOf(AbstractLayerType.class, WORKSHOP_PKG));

    // 柱 / 折线的数值标签必须能被配置为「显示」；散点声明零字段故没有 json
    Assert.assertNotNull("BarLayerType.json 应存在（showLabels 的文案从这里下发）",
      TisPluginFormTestSupport.formResource(BarLayerType.class.getName()));
    Assert.assertNotNull("LineLayerType.json 应存在",
      TisPluginFormTestSupport.formResource(LineLayerType.class.getName()));
    Assert.assertNull("零字段的散点画法不应有 .json（有 json 意味着有人给它加了字段）",
      TisPluginFormTestSupport.formResource(ScatterLayerType.class.getName()));

    Assert.assertEquals("ChartLayer.json 的 layerType.dftVal 必须逐字等于默认子类的 displayName",
      "Bar", formResourceOf(ChartLayer.class).getJSONObject("layerType").getString("dftVal"));
    assertHostField(new ChartLayer(), "layerType", AbstractLayerType.class, BarLayerType.class);
  }

  /**
   * 轴标题：抽象父类 + off / 显示两个子类；{@code titleOverride} 只存在于「显示」子类且必填。
   *
   * <p>必填是需求指定的：选了「显示标题」却填不出标题，等于配置了一个空标题 ——
   * 这种状态应当在校验阶段就被拦下，而不是渲染成一根没有文字的轴线。
   */
  @Test
  public void axisTitleIsPolymorphicAndTitleOverrideIsRequired() throws Exception {
    assertAbstractParentWithNoFields(AbstractAxisTitleConfig.class);
    assertOffSubclass(AxisTitleOff.class, AbstractAxisTitleConfig.class, "不显示轴标题");

    Assert.assertEquals("显示子类应只持有标题文本字段",
      Set.of("titleOverride"), declaredFormFields(AxisTitleConfig.class));
    Assert.assertTrue("titleOverride 必须带 Validator.require（选了显示却填不出标题 = 空标题）",
      Arrays.asList(formFieldOf(AxisTitleConfig.class, "titleOverride").validate())
        .contains(Validator.require));

    // 默认 off：沿用多态化之前的 showTitle=false
    Assert.assertEquals("AxisConfig.json 的 showTitle.dftVal 必须是 off",
      Descriptor.SWITCH_OFF,
      formResourceOf(AxisConfig.class).getJSONObject("showTitle").getString("dftVal"));
    assertHostField(new CategoricalAxisConfig(), "showTitle", AbstractAxisTitleConfig.class, AxisTitleOff.class);
  }

  /**
   * 数值格式化：抽象父类 + off / 默认开启两个子类；{@code enableNumericalFormatting}
   * 这个旁挂开关已删除。
   *
   * <p>三处宿主（轴 / 指标 / 副指标）共用同一套多态，任何一个宿主漏改都会让「关掉格式化」
   * 在那个宿主编不出来。
   */
  @Test
  public void numericFormattingIsPolymorphic() throws Exception {
    assertAbstractParentWithNoFields(AbstractNumericFormatting.class);
    assertOffSubclass(NumericFormattingOff.class, AbstractNumericFormatting.class, "不启用数值格式化");

    Assert.assertEquals("数值格式化的具体子类应恰好是 off 与默认开启",
      new HashSet<>(Arrays.asList(NumericFormattingOff.class, NumericFormatting.class)),
      TisPluginFormTestSupport.concreteSubclassesOf(AbstractNumericFormatting.class, WORKSHOP_PKG));

    Assert.assertFalse("enableNumericalFormatting 应已删除：开关与载荷合并为一个多态字段",
      declaredFormFields(AxisConfig.class).contains("enableNumericalFormatting"));

    assertHostField(new CategoricalAxisConfig(), "numericFormatting", AbstractNumericFormatting.class,
      NumericFormatting.class);
    assertHostField(new MetricConfig(), "numericFormatting", AbstractNumericFormatting.class,
      NumericFormatting.class);
    assertHostField(new SecondaryMetricConfig(), "numericFormatting", AbstractNumericFormatting.class,
      NumericFormatting.class);
  }

  /**
   * 数值轴上下界：抽象父类 + 自动计算 / 手工指定两个子类；{@code value} 只存在于手工态且必填。
   *
   * <p>原先上下界各是两个平铺字段（{@code autoMinBound} + {@code minBound}、
   * {@code autoMaxBound} + {@code maxBound}），能造出「自动算着却还留着手工值」这种
   * 永远不会生效的落盘状态。多态化后该状态在类型层面不可表示。
   *
   * <p>同一个类型服务上下界两个宿主字段：两边只差「是上界还是下界」，各建一套只会让同一个
   * 概念出现两个名字。
   */
  @Test
  public void axisBoundsArePolymorphicAndValueIsRequired() throws Exception {
    assertAbstractParentWithNoFields(AbstractDecimalBound.class);
    assertOffSubclass(DecimalBoundOff.class, AbstractDecimalBound.class, "自动计算");

    Assert.assertEquals("手工指定子类应只持有边界值字段",
      Set.of("value"), declaredFormFields(DecimalBoundConfig.class));
    Assert.assertTrue("value 必须带 Validator.require（选了手工指定却填不出值 = 与自动无区别）",
      Arrays.asList(formFieldOf(DecimalBoundConfig.class, "value").validate())
        .contains(Validator.require));

    Assert.assertEquals("数值轴边界的两个子类应恰好是 off 与手工指定",
      new HashSet<>(Arrays.asList(DecimalBoundOff.class, DecimalBoundConfig.class)),
      TisPluginFormTestSupport.concreteSubclassesOf(AbstractDecimalBound.class, WORKSHOP_PKG));

    // 旁挂开关必须彻底消失：留着就是第三个真值来源
    Set<String> continuousFields = declaredFormFields(ContinuousAxisConfig.class);
    Assert.assertFalse("autoMinBound 应已删除：开关与载荷合并为一个多态字段",
      continuousFields.contains("autoMinBound"));
    Assert.assertFalse("autoMaxBound 应已删除：开关与载荷合并为一个多态字段",
      continuousFields.contains("autoMaxBound"));

    // 默认是「自动」：多态化不该顺手把既有图表的轴改成固定上下界
    assertHostField(new ContinuousAxisConfig(), "minBound", AbstractDecimalBound.class, DecimalBoundOff.class);
    assertHostField(new ContinuousAxisConfig(), "maxBound", AbstractDecimalBound.class, DecimalBoundOff.class);
    JSONObject json = formResourceOf(ContinuousAxisConfig.class);
    Assert.assertEquals("ContinuousAxisConfig.json 的 minBound.dftVal 必须是 off",
      Descriptor.SWITCH_OFF, json.getJSONObject("minBound").getString("dftVal"));
    Assert.assertEquals("ContinuousAxisConfig.json 的 maxBound.dftVal 必须是 off",
      Descriptor.SWITCH_OFF, json.getJSONObject("maxBound").getString("dftVal"));

    // 零字段的 off 子类不建 json；手工指定态的文案从 json 下发
    Assert.assertNull("零字段的自动计算子类不应有 .json",
      TisPluginFormTestSupport.formResource(DecimalBoundOff.class.getName()));
    Assert.assertNotNull("DecimalBoundConfig.json 应存在（value 的文案从这里下发）",
      TisPluginFormTestSupport.formResource(DecimalBoundConfig.class.getName()));
  }

  /**
   * 上下界的大小关系：两侧都手工指定时，{@code maxBound} 必须严格大于 {@code minBound}。
   *
   * <p>这条约束只能落在 {@code ContinuousAxisConfig.DefaultDescriptor#validateAll}：
   * {@code DecimalBoundConfig.value} 上的 {@code Validator.require} 只管「填了没有」，
   * 任意小数都能单字段过关，大小关系是两个字段之间的事实。
   *
   * <p>走的是真实保存链路（{@code AttrValMap.parseDescribableMap} +
   * {@code validateWithScope(..., STRICT)}），而不是直接调 {@code validateAll} ——
   * 后者需要手工构造 {@code PostFormVals}，测不出「POST 线格式解析出来的多态对象是否真是
   * {@code DecimalBoundConfig}」这一层，而那正是 {@code instanceof} 判据的依赖。
   *
   * <p>反向用例同样重要：任一侧为「自动计算」时<b>不得</b>报错。自动算出来的下界完全可能
   * 高于用户手工填的上界，保存期拦截它等于禁止用户表达合法意图。
   */
  @Test
  public void maxBoundMustExceedMinBoundWhenBothManuallySet() {
    String on = DecimalBoundConfig.class.getName();
    String off = DecimalBoundOff.class.getName();

    AxisValidation ok = validateAxisBounds(on, 1d, on, 2d);
    Assert.assertTrue("1 < 2 应通过：" + ok.errors, ok.valid);

    AxisValidation inverted = validateAxisBounds(on, 5d, on, 2d);
    Assert.assertFalse("下界 5 高于上界 2 必须被拒绝", inverted.valid);
    Assert.assertTrue("错误应报在 maxBound 上（用户要改的是上界）：" + inverted.errors,
      inverted.errors.stream().anyMatch(e -> e.startsWith("maxBound:")));
    Assert.assertTrue("错误文案应带上两个值，便于用户定位：" + inverted.errors,
      inverted.errors.stream().anyMatch(e -> e.contains("5.0") && e.contains("2.0")));

    Assert.assertFalse("上下界相等会得到零高度的轴，同样非法",
      validateAxisBounds(on, 2d, on, 2d).valid);
    Assert.assertTrue("负数区间 -5 < -1 应通过",
      validateAxisBounds(on, -5d, on, -1d).valid);

    // 只有一侧手工指定时无需比较：另一侧由图表实际值推断，没有确定值可比
    Assert.assertTrue("下界自动 + 上界手工：不比较",
      validateAxisBounds(off, null, on, 2d).valid);
    Assert.assertTrue("下界手工 + 上界自动：不比较",
      validateAxisBounds(on, 500d, off, null).valid);
    Assert.assertTrue("两侧都自动：不比较",
      validateAxisBounds(off, null, off, null).valid);
  }

  /** 一次保存期校验的结果：是否通过 + 挂在各字段上的错误（{@code field:msg} 形式） */
  private static final class AxisValidation {
    private final boolean valid;
    private final List<String> errors;

    private AxisValidation(boolean valid, List<String> errors) {
      this.valid = valid;
      this.errors = errors;
    }
  }

  /**
   * 按 POST 线格式组装一条连续轴的表单并走保存期校验。
   *
   * <p>线格式的两层包装都不能省：多态字段在外层是 {@code {"descVal":{"impl","vals"}}}，
   * 叶子值在内层是 {@code {"_primaryVal": 值}}。少一层不报错，只会静默取不到值
   * （见记忆 {@code project_describable_get_post_wire_format_inverse}）。
   *
   * <p>{@code showTitle} / {@code numericFormatting} 这两个必填多态字段一并填上默认态，
   * 否则基础校验先失败，测不到 {@code validateAll}。
   */
  private static AxisValidation validateAxisBounds(
    String minImpl, Double minVal, String maxImpl, Double maxVal) {
    Descriptor<?> descriptor = TisPluginFormTestSupport.descriptorOf(ContinuousAxisConfig.class);

    JSONObject vals = new JSONObject();
    vals.put("showTitle", describablePost(AxisTitleOff.class.getName(), new JSONObject()));
    vals.put("numericFormatting", describablePost(NumericFormatting.class.getName(), new JSONObject()));
    vals.put("minBound", describablePost(minImpl, boundVals(minVal)));
    vals.put("maxBound", describablePost(maxImpl, boundVals(maxVal)));

    JSONObject body = new JSONObject();
    body.put("impl", ContinuousAxisConfig.class.getName());
    body.put("vals", vals);

    AttrValMap attrVals = AttrValMap.parseDescribableMap(
      java.util.Optional.empty(), body, descriptor, PropValRewrite.dftRewrite());
    IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
    Descriptor.PluginValidateResult result = attrVals.validateWithScope(
      msgHandler, new DefaultContext(), 0, 0, FormVaildateType.STRICT);

    List<String> errors = new ArrayList<>();
    Mockito.mockingDetails(msgHandler).getInvocations().forEach(inv -> {
      if ("addFieldError".equals(inv.getMethod().getName())) {
        errors.add(inv.getArgument(1) + ":" + inv.getArgument(2));
      }
    });
    return new AxisValidation(result.isValid(), errors);
  }

  /** {@code {"value":{"_primaryVal":...}}}；{@code null} 表示该侧选「自动计算」，不带任何值 */
  private static JSONObject boundVals(Double value) {
    JSONObject vals = new JSONObject();
    if (value != null) {
      JSONObject wrapper = new JSONObject();
      wrapper.put("_primaryVal", value);
      vals.put("value", wrapper);
    }
    return vals;
  }

  /** 多态字段的 POST 外层包装 */
  private static JSONObject describablePost(String impl, JSONObject vals) {
    JSONObject inner = new JSONObject();
    inner.put("impl", impl);
    inner.put("vals", vals);
    JSONObject outer = new JSONObject();
    outer.put("descVal", inner);
    return outer;
  }

  /**
   * 系列聚合：{@code property} 上提到抽象基类，六种聚合派生子类。
   *
   * <p>三个静默失效点：
   * <ol>
   *   <li>{@code property} 若挂在基类却<b>不</b>出现在 {@code AbstractSeriesAggregation.json}，
   *       表单上根本没有这个输入框，所有非 Count 聚合永远算不出值；</li>
   *   <li>{@code CountAggregation.json} 若没把 {@code property} disable，计数会要求用户
   *       填一个用不上的度量属性；</li>
   *   <li>{@code property} 若挂 {@code Validator.require}，Count 的默认配置永远校验失败
   *       （disable 只影响渲染，不影响校验遍历）。「非 Count 必须有 property」在
   *       {@code ChartXYDataProcessor} 执行期校验。</li>
   * </ol>
   */
  @Test
  public void seriesAggregationIsPolymorphic() throws Exception {
    Assert.assertTrue("AbstractSeriesAggregation 应是抽象类",
      Modifier.isAbstract(AbstractSeriesAggregation.class.getModifiers()));
    Assert.assertEquals("property 应上提到聚合抽象层（六种聚合里五种都要它）",
      Set.of("property"), declaredFormFields(AbstractSeriesAggregation.class));
    Assert.assertFalse("property 不得挂 Validator.require：Count 子类把它 disable 了，"
        + "挂了会让 Count 的默认配置永远校验失败",
      Arrays.asList(formFieldOf(AbstractSeriesAggregation.class, "property").validate())
        .contains(Validator.require));

    Set<Class<?>> expected = new HashSet<>(Arrays.asList(
      CountAggregation.class, SumAggregation.class, AverageAggregation.class,
      MinAggregation.class, MaxAggregation.class, ApproxUniqueCountAggregation.class));
    Assert.assertEquals("系列聚合的具体子类应恰好是六种",
      expected, TisPluginFormTestSupport.concreteSubclassesOf(AbstractSeriesAggregation.class, WORKSHOP_PKG));

    // ⚠️ 唯一允许「零自有字段却有 json」的类：它的 json 只做继承属性覆盖
    JSONObject countJson = TisPluginFormTestSupport.formResource(CountAggregation.class.getName());
    Assert.assertNotNull("CountAggregation.json 应存在：它负责把基类的 property 从表单移除",
      countJson);
    Assert.assertEquals("CountAggregation.json 应只覆盖 property 一项（零自有字段，不该描述别的）",
      Set.of("property"), countJson.keySet());
    Assert.assertTrue("CountAggregation 应把 property 从表单移除（计数不需要度量属性）",
      countJson.getJSONObject("property").getBooleanValue("disable"));

    for (Class<?> clazz : Arrays.asList(SumAggregation.class, AverageAggregation.class,
      MinAggregation.class, MaxAggregation.class, ApproxUniqueCountAggregation.class)) {
      Assert.assertNull(clazz.getSimpleName() + " 零自有字段，不应有 .json（继承基类的即可）",
        TisPluginFormTestSupport.formResource(clazz.getName()));
    }

    // 原 variable 已删除：它必填、候选恒为空、前端 SELECTABLE 分支不给手输入口、后端零读取，
    // 三者叠加使「添加系列」永远保存不了（服务端 prop:variable can not be empty）。
    // 数据来源本就是图层职责（ChartLayer.dataSourceVar），不该逐系列重选。
    Assert.assertEquals("SeriesConfig.property 应已下沉到聚合子表单；variable 不得复活",
      Set.of("name", "aggregation", "segmentBy", "displayOverride", "color",
        "nullDisplay", "segmentOverride"),
      declaredFormFields(SeriesConfig.class));
    Assert.assertEquals("SeriesConfig.json 的 aggregation.dftVal 必须逐字等于默认子类的 displayName",
      "Count", formResourceOf(SeriesConfig.class).getJSONObject("aggregation").getString("dftVal"));
    assertHostField(new SeriesConfig(), "aggregation", AbstractSeriesAggregation.class,
      CountAggregation.class);
  }

  /**
   * {@code MULTI_DESCRIBLE_PLUGIN} 元素的两处 descriptor 必须实现完整契约。
   *
   * <p>只 extends 普通 {@link Descriptor} 时前端拿不到 {@code viewStyle / colsCfg / maxHeight} ——
   * 子表格静默退化成默认形态（无表头、无宽度、无法点击编辑），不报任何错。
   */
  @Test
  public void multiDescribleElementsDeclareTableView() {
    for (Class<?> clazz : List.of(ChartLayer.class, SeriesConfig.class)) {
      Class<?> descriptorClass = clazz.getDeclaredClasses().length == 0 ? null
        : Arrays.stream(clazz.getDeclaredClasses())
        .filter(c -> Descriptor.class.isAssignableFrom(c))
        .findFirst().orElse(null);
      Assert.assertNotNull(clazz.getSimpleName() + " 应有嵌套 descriptor", descriptorClass);
      Assert.assertTrue(clazz.getSimpleName() + " 的 descriptor 必须 extends MultiDescribleElementDescriptor"
          + "（否则前端拿不到 viewStyle/colsCfg/maxHeight，子表格静默退化）",
        com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor.class
          .isAssignableFrom(descriptorClass));

      com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor<?> descriptor;
      try {
        descriptor = (com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor<?>)
          descriptorClass.getDeclaredConstructor().newInstance();
      } catch (ReflectiveOperationException e) {
        throw new AssertionError(clazz.getSimpleName() + " 的 descriptor 应可无参实例化", e);
      }

      Assert.assertEquals(clazz.getSimpleName() + " 应采用表格形态",
        com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor.ViewStyle.Table, descriptor.viewStyle());

      List<com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor.ColConfig> cols = descriptor.colsConfig();
      Assert.assertFalse(clazz.getSimpleName() + " 的 colsConfig 不应为空", cols.isEmpty());
      Assert.assertTrue(clazz.getSimpleName() + " 的 colsConfig 至少要有一列 clickable"
          + "（否则无法打开编辑面板）",
        cols.stream().anyMatch(col -> Boolean.TRUE.equals(col.getClickable())));

      Set<String> formFields = declaredFormFields(clazz);
      for (com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor.ColConfig col : cols) {
        Assert.assertTrue(clazz.getSimpleName() + " 的列 " + col + " 不是真实的 @FormField"
            + "（列名写错时表格该列静默为空）", formFields.contains(col.getTitleKey()));
      }

      Assert.assertTrue(clazz.getSimpleName() + " 的 maxHeight 应显式调大（默认仅 100px）",
        descriptor.maxHeight() > 100);
    }
  }

  // ==================================================================
  // off / on 断言的公共部分
  // ==================================================================

  /** 本模块全部插件类的公共包前缀 —— 多态子类可跨 chartxy / model 两个包 */
  private static final String WORKSHOP_PKG = "com.qlangtech.tis.plugin.ontology.workshop";

  private static void assertAbstractParentWithNoFields(Class<?> clazz) {
    Assert.assertTrue(clazz.getSimpleName() + " 应是抽象类：两态由具体子类承担",
      Modifier.isAbstract(clazz.getModifiers()));
    Assert.assertTrue(clazz.getSimpleName() + " 应实现 Describable 才能作为 @FormField 字段类型",
      Describable.class.isAssignableFrom(clazz));
    Assert.assertEquals(clazz.getSimpleName() + " 作为抽象父类不得声明 @FormField"
        + "（与子类 ordinal 撞号）", 0, declaredFormFields(clazz).size());
    Assert.assertNull(clazz.getSimpleName() + " 无字段的抽象父类不应有 .json",
      TisPluginFormTestSupport.formResource(clazz.getName()));
  }

  private static void assertOffSubclass(
    Class<?> offClazz, Class<?> baseClazz, String expectedShortComment) {
    Assert.assertEquals(offClazz.getSimpleName() + " 应是零字段（关闭态不需要任何参数）",
      0, declaredFormFields(offClazz).size());
    Assert.assertNull(offClazz.getSimpleName() + " 零字段的关闭子类不应有 .json"
      + "（有 json 意味着有人给它加了字段）",
      TisPluginFormTestSupport.formResource(offClazz.getName()));

    Descriptor<?> descriptor = TisPluginFormTestSupport.descriptorOf(offClazz);
    Assert.assertEquals(offClazz.getSimpleName() + " 的 displayName 必须是 Descriptor.SWITCH_OFF"
        + "（宿主 .json 的 dftVal 按裸字符串匹配它）",
      Descriptor.SWITCH_OFF, descriptor.getDisplayName());
    Assert.assertEquals(offClazz.getSimpleName() + " 的 descriptor id 应是自身类 FQCN",
      offClazz.getName(), descriptor.getId());
    Assert.assertEquals(offClazz.getSimpleName() + " descriptor 的泛型实参应是抽象父类",
      baseClazz, descriptor.getT());
    Assert.assertEquals(offClazz.getSimpleName() + " 应带中文说明",
      expectedShortComment,
      ((com.qlangtech.tis.extension.DescriptorUseableShortComment) descriptor).shortComment());
  }

  /**
   * 宿主字段三条同时成立才算接线正确：类型是抽象父类（否则 off 在类型层面不可表示）、
   * Java 初值是约定的默认子类、带 {@code Validator.require}（多态化后它恒非空）。
   *
   * <p>传实例而不是 {@code Class}：宿主本身可能也是抽象类（{@code AxisConfig}），
   * 只能借具体子类实例来读字段初值。
   */
  private static void assertHostField(Object host, String fieldName,
                                      Class<?> expectedType, Class<?> expectedDefault) throws Exception {
    Class<?> hostClazz = host.getClass();
    Field field = hostClazz.getField(fieldName);
    Assert.assertEquals(hostClazz.getSimpleName() + "." + fieldName + " 的声明类型应为抽象父类",
      expectedType, field.getType());
    Assert.assertEquals(hostClazz.getSimpleName() + "." + fieldName + " 的 Java 初值应为默认子类",
      expectedDefault, field.get(host).getClass());
    Assert.assertTrue(hostClazz.getSimpleName() + "." + fieldName
        + " 应带 Validator.require（多态化后它恒非空）",
      Arrays.asList(field.getAnnotation(FormField.class).validate()).contains(Validator.require));
  }

  /**
   * 图层的「选择写回」是绑定到一个<b>已存在的对象集合过滤器变量</b>，不是布尔开关。
   *
   * <p>对应 Palantir 图层的 <i>Selection as filter</i>：「经由输出的 <i>Object set filter</i>
   * 变量，允许对本图层的选择做下游组件过滤」。旧实现是个 {@code Boolean} 占位字段，
   * 既表达不了「写回哪个变量」，也无法被下游消费。
   *
   * <p>钉住三条，任一条破了都是静默失败：
   * <ol>
   *   <li>字段类型是 {@code String}（变量名）——回退成 {@code Boolean} 即刻红；</li>
   *   <li>控件是 {@code SELECTABLE} —— 候选来自当前 Module 的对象集合过滤器变量；</li>
   *   <li>选项供给已注册 —— 未注册时 {@code getSelectOptions} 抛
   *       {@code IllegalStateException}，表单<b>报错</b>而不是给个空下拉。</li>
   * </ol>
   */
  @Test
  public void selectionAsFilterBindsObjectSetFilterVariable() {
    Assert.assertEquals("选择写回字段应存变量名（String），而非布尔开关",
      String.class, fieldType(ChartLayer.class, "selectionAsFilterVar"));

    Assert.assertEquals("选择写回应为 SELECTABLE：候选是当前 Module 的对象集合过滤器变量",
      FormFieldType.SELECTABLE, formFieldOf(ChartLayer.class, "selectionAsFilterVar").type());

    // 留空合法（Palantir 该选项本就是 Optional）—— 挂了 require 会让「不启用」变成非法配置
    Assert.assertFalse("选择写回应允许留空，不应挂 Validator.require",
      Arrays.asList(formFieldOf(ChartLayer.class, "selectionAsFilterVar").validate())
        .contains(Validator.require));

    // 选项注册：判定口径照抄 TestWidgetVariableContract.everySelectableBindingFieldIsRegistered()
    // —— 只有「确证未注册」才算失败，求值过程需要 TIS 上下文而抛的其它异常一律放行。
    Descriptor<?> descriptor;
    try {
      descriptor = new ChartLayer.DescriptorImpl();
    } catch (Throwable t) {
      throw new AssertionError("ChartLayer.DescriptorImpl 应可无参实例化（TIS 反射创建 descriptor 的前提）", t);
    }
    try {
      descriptor.getSelectOptions(ChartLayer.KEY_SELECTION_AS_FILTER_VAR);
    } catch (IllegalStateException e) {
      Assert.assertFalse("ChartLayer 未在 DescriptorImpl 构造器中为 selectionAsFilterVar "
          + "registerSelectOptions —— 表单会报错：" + e.getMessage(),
        String.valueOf(e.getMessage()).contains("has not been register"));
    } catch (Throwable t) {
      // 已注册，只是选项 getter 求值需要 TIS 运行期上下文（单测环境无 thread-local plugin context）
    }
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
    assertNoOrdinalCollision(AxisConfig.class,
      List.of(CategoricalAxisConfig.class, ContinuousAxisConfig.class));
    assertNoOrdinalCollision(XAxisBucketing.class,
      List.of(TemporalBucketing.class, CategoricalBucketing.class));
  }

  private static void assertNoOrdinalCollision(Class<?> base, List<Class<?>> subclasses) {
    Set<Integer> baseOrdinals = ordinalsOf(base);
    for (Class<?> clazz : subclasses) {
      Set<Integer> own = ordinalsOf(clazz);
      Set<Integer> collide = new HashSet<>(baseOrdinals);
      collide.retainAll(own);
      Assert.assertTrue(clazz.getSimpleName() + " 与 " + base.getSimpleName() + " 的 ordinal 撞车：" + collide,
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
    // XAxisBucketing 基类持有 limit 字段，因此它<b>要</b>在列 —— PluginExtraProps.load()
    // 自顶向下合并各级 <SimpleName>.json，基类字段的文案正是靠基类那份资源下发的。
    for (Class<?> clazz : List.of(
      AxisConfig.class, CategoricalAxisConfig.class, ContinuousAxisConfig.class,
      ChartLayer.class, SeriesConfig.class, ChartXYWidget.class,
      XAxisBucketing.class, TemporalBucketing.class, CategoricalBucketing.class,
      // 本轮多态化的「启用」子类：各自声明了 @FormField，必须有 json
      LegendConfig.class, BarLayerType.class, LineLayerType.class, AxisTitleConfig.class,
      // ⚠️ AbstractSeriesAggregation 是唯一带字段的抽象基类，它的 json 会被子类作为祖先合并
      AbstractSeriesAggregation.class,
      // ⚠️ CountAggregation 零自有字段却有 json —— 只写 property.disable 覆盖继承属性
      CountAggregation.class)) {
      String path = "/" + clazz.getName().replace('.', '/') + ".json";
      Assert.assertNotNull("缺少表单资源 " + path + "（表单的 help/placeholder 文案不会下发）",
        clazz.getResourceAsStream(path));
    }

    // 反方向：零字段类（抽象基类 / off 子类 / 无字段的 on 子类 / 其余聚合子类）不得有 json
    for (Class<?> clazz : List.of(
      AbstractLegendConfig.class, LegendOff.class,
      AbstractLayerType.class, ScatterLayerType.class,
      AbstractAxisTitleConfig.class, AxisTitleOff.class,
      AbstractNumericFormatting.class, NumericFormattingOff.class,
      SumAggregation.class, AverageAggregation.class, MinAggregation.class,
      MaxAggregation.class, ApproxUniqueCountAggregation.class)) {
      String path = "/" + clazz.getName().replace('.', '/') + ".json";
      Assert.assertNull(clazz.getSimpleName() + " 没有任何 @FormField 字段，不应有 "
        + path + "（有 json 意味着有人给它加了字段却没更新本断言）",
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
   * 分桶子类的 descriptor 都能被 ExtensionFinder 扫到 —— 否则表单的分桶方式选择器
   * 一项都没有（{@code applicableDescriptors} 返回空）。
   */
  @Test
  public void everyBucketingSubclassIsReachable() {
    List<Descriptor<?>> descriptors = scanDescriptors(XAxisBucketing.class);
    Assume.assumeTrue("当前环境未初始化 TIS / 未扫到 descriptor（JDK17 下通常还缺 "
      + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", descriptors != null);

    Set<Class<?>> scanned = descriptors.stream()
      .filter(d -> d instanceof XAxisBucketing.BasicDescriptor)
      .map(Descriptor::getClass).collect(Collectors.toSet());

    Set<Class<?>> expected = new HashSet<>(Arrays.asList(
      TemporalBucketing.DefaultDescriptor.class, CategoricalBucketing.DefaultDescriptor.class));

    // 双向相等而非单向包含：少了=子类漏加 @TISExtension，多了=存在未登记的分桶方式
    // （数值轴分桶 byRanges / byFixedWidth 尚未落地时不应建子类，否则又是一条死路选项）
    Assert.assertEquals("扫到的分桶 descriptor 应与两个分桶子类完全一致"
        + "（少了=子类漏加 @TISExtension，多了=存在未登记的分桶方式）",
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
    // 图层与系列都是 MULTI_DESCRIBLE_PLUGIN 的元素，descriptor 必须是 MultiDescribleElementDescriptor
    // （只 extends 普通 Descriptor 时前端拿不到 viewStyle/colsCfg/maxHeight，子表格静默退化）
    assertIsExtensionDescriptor(ChartLayer.DescriptorImpl.class,
      com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor.class);
    assertIsExtensionDescriptor(SeriesConfig.DefaultDescriptor.class,
      com.qlangtech.tis.plugin.IPluginStore.MultiDescribleElementDescriptor.class);
    assertIsExtensionDescriptor(ChartXYWidget.DescriptorImpl.class, Descriptor.class);
    assertIsExtensionDescriptor(CategoricalAxisConfig.DefaultDescriptor.class,
      AxisConfig.BasicDescriptor.class);
    assertIsExtensionDescriptor(ContinuousAxisConfig.DefaultDescriptor.class,
      AxisConfig.BasicDescriptor.class);
    assertIsExtensionDescriptor(TemporalBucketing.DefaultDescriptor.class,
      XAxisBucketing.BasicDescriptor.class);
    assertIsExtensionDescriptor(CategoricalBucketing.DefaultDescriptor.class,
      XAxisBucketing.BasicDescriptor.class);

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

  /** 读某类的同名 {@code .json} 表单资源 —— 与 {@code PluginExtraProps.load()} 的定位方式一致 */
  private static JSONObject formResourceOf(Class<?> clazz) {
    String path = "/" + clazz.getName().replace('.', '/') + ".json";
    try (InputStream in = clazz.getResourceAsStream(path)) {
      Assert.assertNotNull("缺少表单资源 " + path, in);
      return JSONObject.parseObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new AssertionError("读取表单资源失败：" + path, e);
    }
  }

  /**
   * 沿类层次向上找字段 —— json 的键可能对应基类字段
   * （如 {@code CategoricalBucketing.json} 里的 {@code limit} 实际声明在 {@link XAxisBucketing}）。
   */
  private static Field fieldOf(Class<?> clazz, String name) {
    for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
      try {
        return c.getDeclaredField(name);
      } catch (NoSuchFieldException ignored) {
        // 继续往父类找
      }
    }
    return null;
  }

  /** 取指定字段上的 {@code @FormField} 注解，用于断言表单控件类型 */
  private static FormField formFieldOf(Class<?> clazz, String name) {
    try {
      FormField formField = clazz.getField(name).getAnnotation(FormField.class);
      Assert.assertNotNull(clazz.getSimpleName() + "." + name + " 上没有 @FormField", formField);
      return formField;
    } catch (NoSuchFieldException e) {
      throw new AssertionError(clazz.getSimpleName() + " 上找不到字段 " + name, e);
    }
  }
}
