package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.AxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.CategoricalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.ChartLayer;
import com.qlangtech.tis.plugin.ontology.workshop.model.ContinuousAxisConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.LayerDataInput;
import com.qlangtech.tis.plugin.ontology.workshop.model.ObjectSetDataInput;
import com.qlangtech.tis.plugin.ontology.workshop.model.SeriesConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.TemporalBucketing;
import com.qlangtech.tis.plugin.ontology.workshop.model.XAxisBucketing;
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
 *   <li><b>数据输入退回「枚举 + 绑定字段」扁平组合</b>：旧实现的 {@code dataInputType}
 *       全工程零读者，而 {@code ChartLayer.DescriptorImpl} 无条件注册对象集变量选项
 *       —— 于是「时间序列集」也列对象集变量，三选一里两个是死路。见
 *       {@link #dataInputIsPolymorphic()}</li>
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

    // 反过来，绑定必须真实存在于图层上：分组键在图层，数据源在图层的数据输入上
    Assert.assertTrue("分组键应落在图层上",
      declaredFormFields(ChartLayer.class).contains("xAxisProperty"));
    Assert.assertTrue("数据源绑定应落在图层的数据输入子类上（不在轴上）",
      declaredFormFields(ObjectSetDataInput.class).contains("dataSourceVar"));
  }

  /**
   * 数据输入是多态体系：输入方式由子类承担，不压成「类型枚举 + 绑定字段」的扁平组合。
   *
   * <p>旧实现的两个失败模式分别被钉住：
   * <ul>
   *   <li>{@code dataInputType} 是旁挂的第二套类型身份 —— 有了子类它就该消失
   *       （判别标准：Java 类型已经能表达这个区分）</li>
   *   <li>{@code dataSourceVar} 的选项供给挂在宿主 descriptor 上，与判别符脱钩 ——
   *       选了「时间序列集」也照样列对象集变量。下沉后选项由子类自己的 descriptor 供给</li>
   * </ul>
   */
  @Test
  public void dataInputIsPolymorphic() {
    Assert.assertTrue("LayerDataInput 应为抽象类：输入方式由具体子类承担",
      Modifier.isAbstract(LayerDataInput.class.getModifiers()));
    Assert.assertTrue("LayerDataInput 应实现 Describable 才能作为 @FormField 字段类型",
      Describable.class.isAssignableFrom(LayerDataInput.class));

    // 基类不承载任何字段：各输入方式的数据绑定形态互不适用，全部下沉
    Assert.assertTrue("数据输入基类不应承载任何 @FormField 字段，实际为："
        + declaredFormFields(LayerDataInput.class),
      declaredFormFields(LayerDataInput.class).isEmpty());

    // 图层声明抽象类型 —— 这是 TIS 渲染 impl 选择器的前提
    Assert.assertEquals("ChartLayer.dataInput 应声明为抽象基类类型（多态）",
      LayerDataInput.class, fieldType(ChartLayer.class, "dataInput"));

    // 判别枚举与扁平的绑定字段都不得复活
    Set<String> layerFields = declaredFormFields(ChartLayer.class);
    Assert.assertFalse("ChartLayer 不应再有 dataInputType：类型身份归 LayerDataInput 子类",
      layerFields.contains("dataInputType"));
    Assert.assertFalse("dataSourceVar 应下沉到 ObjectSetDataInput，不再留在图层上",
      layerFields.contains("dataSourceVar"));

    // 绑定字段确实落在子类上
    Assert.assertEquals("对象集输入应只持有数据源变量绑定",
      Set.of("dataSourceVar"), declaredFormFields(ObjectSetDataInput.class));
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
    assertNoOrdinalCollision(LayerDataInput.class, List.of(ObjectSetDataInput.class));
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
    // LayerDataInput 抽象基类自身没有 @FormField 字段，也就没有需要下发的文案，故不在此列。
    // XAxisBucketing 基类持有 limit 字段，因此它<b>要</b>在列 —— PluginExtraProps.load()
    // 自顶向下合并各级 <SimpleName>.json，基类字段的文案正是靠基类那份资源下发的。
    for (Class<?> clazz : List.of(
      AxisConfig.class, CategoricalAxisConfig.class, ContinuousAxisConfig.class,
      ChartLayer.class, SeriesConfig.class, ChartXYWidget.class, ObjectSetDataInput.class,
      XAxisBucketing.class, TemporalBucketing.class, CategoricalBucketing.class)) {
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
   * 数据输入子类的 descriptor 都能被 ExtensionFinder 扫到 —— 否则表单的输入方式选择器
   * 一项都没有（{@code applicableDescriptors} 返回空）。
   */
  @Test
  public void everyDataInputSubclassIsReachable() {
    List<Descriptor<?>> descriptors = scanDescriptors(LayerDataInput.class);
    Assume.assumeTrue("当前环境未初始化 TIS / 未扫到 descriptor（JDK17 下通常还缺 "
      + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", descriptors != null);

    Set<Class<?>> scanned = descriptors.stream()
      .filter(d -> d instanceof LayerDataInput.BasicDescriptor)
      .map(Descriptor::getClass).collect(Collectors.toSet());

    Set<Class<?>> expected = new HashSet<>(List.of(ObjectSetDataInput.DefaultDescriptor.class));

    // 双向相等而非单向包含：少了=子类漏加 @TISExtension，多了=存在未登记的输入方式
    Assert.assertEquals("扫到的数据输入 descriptor 应与已落地的输入方式完全一致"
        + "（函数聚合 / 时间序列集未接线时不应建子类，否则又是一条死路选项）",
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
    assertIsExtensionDescriptor(ChartLayer.DescriptorImpl.class, Descriptor.class);
    assertIsExtensionDescriptor(ChartXYWidget.DescriptorImpl.class, Descriptor.class);
    assertIsExtensionDescriptor(CategoricalAxisConfig.DefaultDescriptor.class,
      AxisConfig.BasicDescriptor.class);
    assertIsExtensionDescriptor(ContinuousAxisConfig.DefaultDescriptor.class,
      AxisConfig.BasicDescriptor.class);
    assertIsExtensionDescriptor(ObjectSetDataInput.DefaultDescriptor.class,
      LayerDataInput.BasicDescriptor.class);
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
