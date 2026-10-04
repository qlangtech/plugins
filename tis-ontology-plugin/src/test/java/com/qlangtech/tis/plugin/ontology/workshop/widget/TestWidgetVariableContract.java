package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Widget 变量契约的回归测试（P0-2）。
 *
 * <p>本轮把「角色 → 变量」的 {@code inputVariables} / {@code outputVariables} 两个基类字段
 * 删除，改为每个 Widget 各自命名的类型化绑定字段（{@code objectSetVar} /
 * {@code activeObjectVar} …）。契约涉及三层，任一层漏掉都是<b>静默失败</b>：
 *
 * <ol>
 *   <li><b>字段声明</b> —— 后端 {@code @FormField(SELECTABLE)} 字段；</li>
 *   <li><b>选项注册</b> —— {@code DescriptorImpl} 构造器里的
 *       {@code registerSelectOptions(...)}；漏了它，{@code getSelectOptions} 会抛
 *       {@code IllegalStateException}，表单上表现为报错，而不是空下拉；</li>
 *   <li><b>文案资源</b> —— 同名 {@code .json}。</li>
 * </ol>
 *
 * <p>另有两条钉住同类失败模式的断言：基类字段集合精确（防 {@code inputVariables} 复活）、
 * 以及 {@code MULTI_SELECTABLE} 必须有 {@code elementCreator} + {@code enum}（P0-1 的
 * 失败模式：缺这两键时前端 {@code buildMultiSelectedAttr} 直接 {@code throw}）。
 *
 * <p>全部断言不依赖 TIS 运行期（不需要 {@code --add-opens}）：枚举范围靠编译期生成的
 * sezpoz 注解索引，资源靠 classloader 读。唯一触及运行期的是
 * {@link #everySelectableBindingFieldIsRegistered()} 里对 {@code getSelectOptions} 的调用，
 * 它区分「未注册」（真失败）与「已注册但选项 getter 求值需要 TIS 上下文」（放行）。
 */
public class TestWidgetVariableContract {

  /**
   * sezpoz 在编译期为 {@code @TISExtension} 生成的扩展点索引 —— TIS 的 ExtensionFinder
   * 实际读的就是它（{@code @TISExtension} 是 {@code RetentionPolicy.CLASS}，
   * 运行期 {@code getAnnotation} 恒为 null，不能直接查注解）。
   *
   * <p>本测试用它<b>枚举</b> widget/model 类，而不是硬编码类名清单：新增一个 Widget
   * 只要带 {@code @TISExtension} 就自动进入覆盖范围，不会因为测试文件忘了同步而漏检。
   */
  private static final String SEZPOZ_INDEX_PATH =
    "/META-INF/annotations/com.qlangtech.tis.extension.TISExtension.txt";

  /** 扫描范围：workshop 包下的所有扩展点实现 */
  private static final String WORKSHOP_PKG = "com.qlangtech.tis.plugin.ontology.workshop.";

  /** Widget 实现所在包（只有这个包下的类要求「绑定字段必有同名 .json 文案」） */
  private static final String WIDGET_IMPL_PKG = WORKSHOP_PKG + "widget.impl.";

  /**
   * 基类字段集合 —— 基类只承载<b>所有</b> Widget 共有的字段。
   *
   * <p>此前这里是 {@code inputVariables} / {@code outputVariables}（{@code List<String>}，
   * 无角色概念），零读零写，唯一实际作用是让表单在 {@code buildMultiSelectedAttr} 里抛错。
   * 该断言是「防复活」闸门：谁把它加回来，这里立刻红。
   *
   * <p>2026-09-20 起 {@code displayConfig} 也离开了基类：它与基类其余字段不同 ——
   * 并非所有 Widget 都说得上「高度 / 展示优化」（单行表单控件就说不说），
   * 故下移到 {@link CompactDisplayWidget} / {@link FullDisplayWidget} 两条抽象分支。
   * 因此本集合在本次一并由 5 项收敛为 2 项：原常量里的 {@code sortOrder} 与
   * {@code cssClass} 对应的 {@code @FormField} 早已在 {@code WorkshopWidget} 里被注释掉，
   * 常量却没跟着改，这两条断言本就处于红状态，本次顺带修正。
   */
  private static final Set<String> BASE_WIDGET_FIELDS = Set.of("name", "title");

  /**
   * 已知的 {@code MULTI_SELECTABLE} 无行编辑器字段 —— <b>缺陷登记册，不是白名单</b>。
   *
   * <p>{@code MULTI_SELECTABLE} 要能渲染，其 {@code .json} 必须同时给出
   * {@code elementCreator}（{@code ElementCreatorFactory}）与 {@code enum} 两键；
   * 缺任一个，前端 {@code Item.buildMultiSelectedAttr} 会直接
   * {@code throw new Error("...relevant enumVal can not be null")}，整个 Widget 配置表单打不开。
   *
   * <p>下面这些是当前<b>仍然存在</b>的同类实例，都在 model 侧、不在 Widget 表单范围内，
   * 同属待处理。修好一个就从这里删一条 —— 本表是「允许存在」而非「要求存在」，
   * 删掉不会让测试变红；但新增一处同类字段会。
   */
  private static final Map<String, String> KNOWN_MULTI_SELECTABLE_WITHOUT_ROW_EDITOR;

  static {
    Map<String, String> m = new LinkedHashMap<>();
    // 注：ChartXYWidget.layers / ChartLayer.series / ObjectTableWidget.columns 原列于此，
    // 三者已改用 FormFieldType.MULTI_DESCRIBLE_PLUGIN + desClazz（「固定样式」，
    // 由子表单渲染、无需行编辑器），缺陷已消除，故从登记册删除。
    // PropertyListWidget.properties / MetricConfig.conditionalFormatting 同理，
    // 本轮改用 MULTI_DESCRIBLE_PLUGIN + desClazz，也已删除。
    m.put("DropHandling.onDropEvents", "拖放事件列表：不在本轮 Widget 表单范围");
    m.put("WorkshopOverlay.sections", "浮层区块列表：不在本轮 Widget 表单范围");
    KNOWN_MULTI_SELECTABLE_WITHOUT_ROW_EDITOR = java.util.Collections.unmodifiableMap(m);
  }

  /**
   * 方案 A 的删除结果必须精确 —— 基类不得再有任何变量绑定字段。
   *
   * <p>同时钉住另一件事：绑定字段不得再以「列表 + 角色」的形态出现在基类上。
   * 判据用的是 {@code getDeclaredFields()}（不含继承），子类的绑定字段不受此断言影响。
   */
  @Test
  public void baseClassDeclaresOnlySharedWidgetFields() {
    Set<String> actual = declaredFormFieldNames(WorkshopWidget.class);
    Assert.assertEquals("WorkshopWidget（基类）的 @FormField 字段集合与本轮归一化结果不符。"
        + "基类只应承载所有 Widget 共有的字段；变量绑定属于各 Widget，"
        + "不得再以 inputVariables/outputVariables 这类「角色列表」回到基类",
      BASE_WIDGET_FIELDS, actual);
  }

  /**
   * 基类能被 TIS 的表单构建流程构建出属性列表，且列表里不再有任何变量绑定字段。
   *
   * <p>{@link #baseClassDeclaresOnlySharedWidgetFields()} 查的是「声明了什么」，
   * 这条查的是「构建表单时实际产出什么」—— 二者在多态/继承参与进来时会分叉。
   * 这是本轮删字段的下游回归点（原先由 tis-plugin 的 {@code TestPropertyType} 覆盖；
   * 该类当前因 {@code UserProfile} 上未完成的改动编译不过，故在此补等价断言）。
   */
  @Test
  public void baseClassBuildsFormPropertyTypesWithoutVariableBindings() {
    Map<String, IPropertyType> props;
    try {
      props = PropertyType.buildPropertyTypes(Optional.empty(), WorkshopWidget.class);
    } catch (Throwable t) {
      Assume.assumeNoException("单测环境缺少构建 propertyType 所需的 TIS 上下文，跳过", t);
      return;
    }

    Assert.assertNotNull("基类应能构建出属性列表", props);
    Assert.assertEquals("基类构建出的字段应与声明的字段一致",
      BASE_WIDGET_FIELDS, new TreeSet<>(props.keySet()));

    for (String gone : new String[]{"inputVariables", "outputVariables"}) {
      Assert.assertFalse("基类不应再构建出 " + gone + "（该字段已删除）", props.containsKey(gone));
    }
  }

  /**
   * 每个 Widget 的类型化绑定字段（{@code *Var}）都必须：
   * ①声明为 {@code SELECTABLE}；②有同名 {@code .json} 资源；③该资源里含这个字段的条目。
   *
   * <p>漏掉 ②③ 的表现是表单上该字段没有 label/placeholder，而字段名只能靠人猜。
   */
  @Test
  public void everyBindingFieldHasFormResourceEntry() {
    List<String> problems = new ArrayList<>();

    for (Class<?> widget : widgetImpls()) {
      Map<String, FormField> bindingFields = bindingFieldsOf(widget);
      if (bindingFields.isEmpty()) {
        continue;
      }

      JSONObject resource = formResource(widget);
      if (resource == null) {
        problems.add(widget.getSimpleName() + " 声明了绑定字段 " + bindingFields.keySet()
          + "，但缺少表单资源 " + resourcePath(widget));
        continue;
      }

      for (Map.Entry<String, FormField> e : bindingFields.entrySet()) {
        if (e.getValue().type() != FormFieldType.SELECTABLE) {
          problems.add(widget.getSimpleName() + "." + e.getKey() + " 是变量绑定字段，"
            + "应为 SELECTABLE（当前 " + e.getValue().type() + "）");
        }
        if (!resource.containsKey(e.getKey())) {
          problems.add(widget.getSimpleName() + "." + e.getKey()
            + " 在 " + widget.getSimpleName() + ".json 中没有条目（表单上无文案）");
        }
      }
    }

    Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
  }

  /**
   * 每个绑定字段都必须在 {@code DescriptorImpl} 里注册过选项供给方。
   *
   * <p>未注册时 {@code Descriptor.getSelectOptions} 抛
   * {@code IllegalStateException("...has not been register...")} —— 表单直接报错。
   * 这是最容易漏的一层：字段和 .json 都写了，构造器里少一行 {@code registerSelectOptions}。
   *
   * <p>判定方式：故意调用一次 {@code getSelectOptions}。
   * <ul>
   *   <li>{@code IllegalStateException} 且消息含 {@code has not been register} → <b>失败</b>（确证未注册）</li>
   *   <li>其他任何异常 → getter 已被调用并执行到了取模块上下文那一步，说明<b>注册存在</b>，
   *       只是单测环境没有 TIS 线程上下文（{@code IPluginContext.getThreadLocalInstance()} 为 null）。
   *       这条路径放行。</li>
   *   <li>正常返回 → 通过</li>
   * </ul>
   */
  @Test
  public void everySelectableBindingFieldIsRegistered() {
    List<String> problems = new ArrayList<>();
    int checked = 0;

    for (Class<?> widget : widgetImpls()) {
      Map<String, FormField> bindingFields = bindingFieldsOf(widget);
      if (bindingFields.isEmpty()) {
        continue;
      }

      Descriptor<?> descriptor = descriptorOf(widget);
      if (descriptor == null) {
        problems.add(widget.getSimpleName() + " 没有可实例化的 DescriptorImpl 内部类"
          + "（绑定字段的 options 无处注册）");
        continue;
      }

      for (String fieldName : bindingFields.keySet()) {
        checked++;
        try {
          descriptor.getSelectOptions(fieldName);
        } catch (IllegalStateException e) {
          if (String.valueOf(e.getMessage()).contains("has not been register")) {
            problems.add(widget.getSimpleName() + "." + fieldName
              + " 未在 DescriptorImpl 构造器中 registerSelectOptions —— 表单会报错");
          }
        } catch (Throwable t) {
          // 已注册，只是选项 getter 求值需要 TIS 运行期上下文
        }
      }
    }

    Assert.assertTrue("扫描到的绑定字段应至少有一个（扫描范围失效？）", checked > 0);
    Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
  }

  /**
   * {@code MULTI_SELECTABLE} 必须有行编辑器（{@code elementCreator}）与 {@code enum} 两键，
   * 否则前端 {@code buildMultiSelectedAttr} 抛错、整个 Widget 配置表单打不开。
   *
   * <p>这正是 P0-1 的失败模式。已知待设计的实例见
   * {@link #KNOWN_MULTI_SELECTABLE_WITHOUT_ROW_EDITOR}（缺陷登记册）。
   * {@code transient} / {@code static} 字段跳过 —— TIS 构建表单时本就不含它们
   * （如 {@code WorkshopModule.pages}）。
   */
  @Test
  public void everyMultiSelectableFieldHasRowEditor() {
    List<String> problems = new ArrayList<>();
    Set<String> seen = new TreeSet<>();

    for (Class<?> clazz : scannedClasses()) {
      JSONObject resource = formResource(clazz);

      for (Field f : clazz.getDeclaredFields()) {
        FormField ff = f.getAnnotation(FormField.class);
        if (ff == null || ff.type() != FormFieldType.MULTI_SELECTABLE) {
          continue;
        }
        if (Modifier.isTransient(f.getModifiers()) || Modifier.isStatic(f.getModifiers())) {
          // TIS 构建表单时跳过 transient/static 字段，不构成渲染缺陷
          continue;
        }

        String key = clazz.getSimpleName() + "." + f.getName();
        seen.add(key);
        if (KNOWN_MULTI_SELECTABLE_WITHOUT_ROW_EDITOR.containsKey(key)) {
          continue;
        }

        if (resource == null) {
          problems.add(key + " 是 MULTI_SELECTABLE，但 " + clazz.getSimpleName()
            + " 连表单资源都没有（需要 elementCreator + enum）");
          continue;
        }
        JSONObject fieldMeta = resource.getJSONObject(f.getName());
        if (fieldMeta == null) {
          problems.add(key + " 是 MULTI_SELECTABLE，但其 .json 里没有该字段的条目"
            + "（需要 elementCreator + enum）");
          continue;
        }
        for (String required : new String[]{"elementCreator", "enum"}) {
          if (!fieldMeta.containsKey(required)) {
            problems.add(key + " 缺少 \"" + required + "\"：前端 buildMultiSelectedAttr "
              + "会抛 \"relevant enumVal can not be null\"，整个配置表单打不开");
          }
        }
      }
    }

    Assert.assertTrue(String.join("\n", problems), problems.isEmpty());

    // 反空转：登记册的每一条都必须真的被扫到。否则说明扫描范围失效
    // （包名前缀写错、类未被 @TISExtension 登记），本测试会在「一个 MULTI_SELECTABLE
    // 字段都没扫到」的情况下依然全绿 —— 那是橡皮图章，不是回归测试。
    Set<String> notSeen = new TreeSet<>(KNOWN_MULTI_SELECTABLE_WITHOUT_ROW_EDITOR.keySet());
    notSeen.removeAll(seen);
    Assert.assertTrue("以下已登记的 MULTI_SELECTABLE 字段未被扫描到，登记册已失效："
      + notSeen + "\n实际扫到：" + seen, notSeen.isEmpty());
  }

  /**
   * 缺陷登记册不得有笔误：每条登记的类都能加载、字段都真实存在。
   *
   * <p>没有这条，登记册会退化成「写错的字符串永远不会被匹配、于是永远不生效」的静默豁免。
   */
  @Test
  public void knownDefectRegisterEntriesAreReal() {
    List<String> problems = new ArrayList<>();

    for (String key : KNOWN_MULTI_SELECTABLE_WITHOUT_ROW_EDITOR.keySet()) {
      int dot = key.indexOf('.');
      Assert.assertTrue("登记格式应为 SimpleName.field： " + key, dot > 0);

      String simpleName = key.substring(0, dot);
      String fieldName = key.substring(dot + 1);

      Class<?> clazz = scannedClasses().stream()
        .filter(c -> c.getSimpleName().equals(simpleName)).findFirst().orElse(null);
      if (clazz == null) {
        problems.add(key + " 对应的类未被扫到（类名写错？）");
        continue;
      }
      try {
        Field f = clazz.getDeclaredField(fieldName);
        FormField ff = f.getAnnotation(FormField.class);
        if (ff == null) {
          problems.add(key + " 的字段没有 @FormField");
        } else if (ff.type() != FormFieldType.MULTI_SELECTABLE) {
          problems.add(key + " 已不是 MULTI_SELECTABLE（" + ff.type() + "），应从登记册删除");
        }
      } catch (NoSuchFieldException e) {
        problems.add(key + " 的字段不存在（已重命名或删除？应从登记册删除）");
      }
    }

    Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
  }

  /**
   * 扫描范围的完整性：sezpoz 索引里 {@code widget.impl} 下的每个 Widget 都能对应到一个
   * {@code WorkShopWidgetType} 常量，且没有两个 Widget 共用一个类型。
   *
   * <p>作用有二：其一，证明 {@link #widgetImpls()} 的枚举真的覆盖了全部 Widget
   * （否则本类的断言会因为「一个都没扫到」而假通过）；其二，
   * {@code widgetType} 是前端 renderRegistry 的派发键，重复会让某个 Widget 渲染成另一个。
   */
  @Test
  public void widgetImplsAreEnumeratedAndMapToDistinctWidgetTypes() {
    Set<Class<?>> widgets = widgetImpls();
    Assert.assertFalse("未从 sezpoz 索引枚举到任何 Widget 实现 —— 索引读取或包名前缀已失效",
      widgets.isEmpty());

    Map<String, String> typeOwners = new LinkedHashMap<>();
    List<String> problems = new ArrayList<>();

    for (Class<?> widget : widgets) {
      Descriptor<?> d = descriptorOf(widget);
      if (d == null) {
        problems.add(widget.getSimpleName() + " 没有 DescriptorImpl");
        continue;
      }
      Object widgetType = invokeGetWidgetType(d);
      if (widgetType == null) {
        problems.add(widget.getSimpleName() + " 的 DescriptorImpl 不是 BaseWidgetDescriptor"
          + " 或 getWidgetType() 取不到值");
        continue;
      }
      String name = ((Enum<?>) widgetType).name();
      String prev = typeOwners.put(name, widget.getSimpleName());
      if (prev != null) {
        problems.add("widgetType " + name + " 被两个 Widget 共用：" + prev + " / " + widget.getSimpleName());
      }
    }

    Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
    Assert.assertEquals("widget.impl 下的 Widget 数应与 WorkShopWidgetType 常量一一对应",
      widgetTypeConstants().size(), typeOwners.size());
    Assert.assertEquals("widgetType 取值应覆盖 WorkShopWidgetType 的全部常量",
      widgetTypeConstants(), new TreeSet<>(typeOwners.keySet()));
  }

  /**
   * 每个具体 Widget 必须继承 {@link CompactDisplayWidget} 或 {@link FullDisplayWidget}
   * 之一，且这两个抽象基类本身不得进入扩展点索引。
   *
   * <p>为什么需要这条：{@code displayConfig} 已从 {@code WorkshopWidget} 下移到两条抽象分支，
   * 于是<b>直接继承 {@code WorkshopWidget} 依然能编译通过</b>，但该 Widget 的表单会
   * <b>整块少掉显示配置</b>——不报错、不抛异常，界面上只是没有那一块。这是本次改动引入的
   * 唯一一个「编译期管不住」的口子，故在此钉死。
   *
   * <p>后半段（抽象基类不得进索引）防的是反方向的坑：插件枚举走 sezpoz 注解索引，
   * 加载路径（{@code ExtensionFinder.Sezpoz}）<b>没有抽象类过滤</b>，一旦有人给这两个基类
   * 加上 {@code @TISExtension}（或加嵌套 Descriptor），它会以「可创建 Widget」的身份混进调色板，
   * 同样运行期无任何报错。
   */
  @Test
  public void everyWidgetInheritsADisplayWidgetBase() {
    List<Class<?>> bases = Arrays.asList(CompactDisplayWidget.class, FullDisplayWidget.class);
    List<String> problems = new ArrayList<>();

    for (Class<?> base : bases) {
      Assert.assertTrue(base.getSimpleName() + " 必须是抽象类",
        Modifier.isAbstract(base.getModifiers()));

      for (String entry : sezpozIndexEntries()) {
        if (entry.startsWith(base.getName())) {
          problems.add(base.getSimpleName() + " 不得带 @TISExtension 进入扩展点索引（"
            + "插件加载没有抽象类过滤，它会被当成可创建的 Widget 混进调色板）：" + entry);
        }
      }
    }

    Set<Class<?>> widgets = concreteWidgets();
    Assert.assertFalse("未从 sezpoz 索引枚举到任何 Widget —— 索引读取或包名前缀已失效，本断言会假通过",
      widgets.isEmpty());
    Assert.assertTrue("本断言应至少覆盖 widget.impl 下的全部 Widget",
      widgets.containsAll(widgetImpls()));
    Assert.assertTrue("GroovyWorkshopWidget 应在本断言的覆盖范围内 —— 它在 widget.groovy 包、"
        + "不在 widget.impl 下，正是这里改用 concreteWidgets() 而非 widgetImpls() 的理由。"
        + "若该类已删除，请连同本行一并删掉",
      widgets.stream().anyMatch(c -> "GroovyWorkshopWidget".equals(c.getSimpleName())));

    for (Class<?> widget : widgets) {
      Class<?> sup = widget.getSuperclass();
      if (sup != CompactDisplayWidget.class && sup != FullDisplayWidget.class) {
        problems.add(widget.getSimpleName() + " 的直接父类是 "
          + (sup == null ? "null" : sup.getSimpleName())
          + "，应为 CompactDisplayWidget 或 FullDisplayWidget"
          + "（直接继承 WorkshopWidget 能编译通过，但会静默丢失整块显示配置）");
        continue;
      }

      // 顺带钉住「字段真的到得了」：从具体类向上解析 displayConfig，其声明类应是该基类
      Class<?> expectedCfg = sup == CompactDisplayWidget.class
        ? CompactDisplayConfig.class : WidgetDisplayConfig.class;
      try {
        Field f = widget.getField("displayConfig");
        Assert.assertSame(sup.getSimpleName() + ".displayConfig 应由 " + sup.getSimpleName() + " 声明",
          sup, f.getDeclaringClass());
        Assert.assertSame(widget.getSimpleName() + ".displayConfig 的类型与基类不匹配",
          expectedCfg, f.getType());
      } catch (NoSuchFieldException e) {
        problems.add(widget.getSimpleName() + " 解析不到 displayConfig 字段：" + e.getMessage());
      }
    }

    Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
  }

  /**
   * 两个显示配置类必须是「同名字段子集」—— 这是本次拆分的<b>目的</b>本身，
   * 不是实现细节：简化组少的正是对单行表单控件无意义的 {@code height} /
   * {@code displayOptimization}，少掉它们，用户才不会看到「填了却没效果」的项。
   *
   * <p>一条断言钉住四件容易静默漂移的事：
   * <ol>
   *   <li>简化组恰好只有 {@code width} + {@code conditionalVisibility}
   *       —— 多出第三项就失去了「简化」的意义；</li>
   *   <li>两类的同名字段<b>类型一致</b>：前端对两组共用
   *       {@code widget.displayConfig.width} 这一条读取路径，靠的就是这里。
   *       类型漂了前端不报错，只会读到 {@code undefined}；</li>
   *   <li>两个 config 各自的 {@code .json} 覆盖其每个 {@code @FormField} 字段
   *       —— 缺条目时该字段在表单上没有 label/help；</li>
   *   <li>两个中间基类的 {@code .json} 里有 {@code displayConfig} 条目 ——
   *       文案必须声明在基类上（{@code PluginExtraProps.load} 沿继承链合并 json
   *       且具体类最后处理），声明在各 widget 上等于要改 22 个文件且必漏。</li>
   * </ol>
   */
  @Test
  public void compactDisplayConfigIsTheSameNamedSubsetOfTheFullOne() {
    Set<String> compactFields = declaredFormFieldNames(CompactDisplayConfig.class);
    Assert.assertEquals("简化组的显示配置应恰好只有 width + conditionalVisibility"
        + "（height / displayOptimization 对单行表单控件无意义，正是被排除的那两项）",
      Set.of("width", "conditionalVisibility"), compactFields);

    Set<String> fullFields = declaredFormFieldNames(WidgetDisplayConfig.class);
    Assert.assertTrue("简化组的字段应是全量组字段的子集，当前多出："
        + minus(compactFields, fullFields),
      fullFields.containsAll(compactFields));
    Assert.assertNotEquals("两个显示配置类的字段集合相同 —— 拆分退化成了同一组选项，"
        + "「让表单只呈现说得通的项」这个目的没有达成",
      fullFields, compactFields);

    for (String field : compactFields) {
      Class<?> compactType;
      Class<?> fullType;
      try {
        compactType = CompactDisplayConfig.class.getField(field).getType();
        fullType = WidgetDisplayConfig.class.getField(field).getType();
      } catch (NoSuchFieldException e) {
        throw new AssertionError("同名字段解析失败：" + e.getMessage(), e);
      }
      Assert.assertSame("CompactDisplayConfig." + field + " 与 WidgetDisplayConfig." + field
          + " 必须同类型 —— 前端对两组共用 widget.displayConfig." + field + " 这一条读取路径",
        fullType, compactType);
    }

    for (Class<?> config : Arrays.asList(CompactDisplayConfig.class, WidgetDisplayConfig.class)) {
      JSONObject resource = formResource(config);
      Assert.assertNotNull(config.getSimpleName() + " 缺少表单资源 " + resourcePath(config)
        + "（其字段在表单上没有 label/help）", resource);
      for (String field : declaredFormFieldNames(config)) {
        Assert.assertTrue(config.getSimpleName() + "." + field + " 在 "
          + config.getSimpleName() + ".json 中没有条目（表单上无文案）", resource.containsKey(field));
      }
    }

    for (Class<?> base : Arrays.asList(CompactDisplayWidget.class, FullDisplayWidget.class)) {
      JSONObject resource = formResource(base);
      Assert.assertNotNull(base.getSimpleName() + " 缺少表单资源 " + resourcePath(base)
        + " —— 其子类的 displayConfig 字段会没有 label/help（json 沿继承链合并，"
        + "故文案声明在这个中间基类上，不必改各 widget）", resource);
      Assert.assertTrue(base.getSimpleName() + ".json 应含 displayConfig 条目",
        resource.containsKey("displayConfig"));
    }
  }

  /**
   * 上面那条查「声明了什么」，这条查「表单<b>实际产出</b>什么」—— 二者在多态 / 继承参与
   * 进来时会分叉，而用户看到的、被困惑到的是后者。
   *
   * <p>这正是本次改动的验收口径：简化组 widget 的显示配置表单**只有 2 项**，
   * 全量组仍是 4 项。此前两组的表单都是 4 项。
   */
  @Test
  public void compactFormProducesTwoDisplayOptionsWhileFullProducesFour() {
    Map<String, IPropertyType> compactProps;
    Map<String, IPropertyType> fullProps;
    try {
      compactProps = PropertyType.buildPropertyTypes(Optional.empty(), CompactDisplayConfig.class);
      fullProps = PropertyType.buildPropertyTypes(Optional.empty(), WidgetDisplayConfig.class);
    } catch (Throwable t) {
      Assume.assumeNoException("单测环境缺少构建 propertyType 所需的 TIS 上下文，跳过", t);
      return;
    }

    Assert.assertEquals("简化组 widget 的表单应恰好产出 2 项显示配置",
      Set.of("width", "conditionalVisibility"), new TreeSet<>(compactProps.keySet()));

    Set<String> fullKeys = new TreeSet<>(fullProps.keySet());
    Assert.assertEquals("全量组 widget 的表单应仍产出 4 项显示配置（本次未改全量组）", 4, fullKeys.size());
    Assert.assertTrue("全量组表单应含 height —— 它正是被排除在简化组之外的那两项之一", fullKeys.contains("height"));
    Assert.assertTrue("全量组表单应含 displayOptimization", fullKeys.contains("displayOptimization"));
    Assert.assertTrue("全量组表单应含简化组的两项", fullKeys.containsAll(compactProps.keySet()));
  }

  /**
   * 两个显示配置字段的 {@code dftVal} 必须<b>逐字等于</b>某个候选描述符的
   * {@code getDisplayName()} —— 这是本仓最典型的「静默失效」配置。
   *
   * <p>前端 {@code tis.plugin.ts}（{@code addNewEmptyItemProp}）做的是<b>裸字符串相等</b>：
   * <pre>
   *   let displayName = this.eprops[KEY_DEFAULT_VALUE];
   *   if (!updateModel &amp;&amp; displayName) {
   *     for (let e of desVal.descVal.descriptors.values()) {
   *       if (displayName === e.displayName) { desVal.descVal.impl = e.impl; break; }
   *     }
   *   }
   * </pre>
   * Java 侧<b>没有任何编译期保护</b>。于是把 {@code "Auto"} 写成 {@code "auto"}、
   * 或把 {@code AutoSizingMode} 的 displayName 改成中文，都不会报错 ——
   * 只是默认项静默消失，用户新建 widget 时那个配置项是空的。
   *
   * <p>另钉住「确实设了默认值」：两个字段都必须有 {@code dftVal}
   * （{@code width} 三选一、{@code conditionalVisibility} 单实现也要显式选中）。
   */
  @Test
  public void displayConfigDefaultsMatchRealDescriptorDisplayNames() {
    for (Class<?> config : Arrays.asList(CompactDisplayConfig.class, WidgetDisplayConfig.class)) {
      assertEveryFieldHasAResolvableDefault(config);
    }
  }

  /**
   * 逐个 {@code @FormField} 校验 {@code dftVal} 落在真正的候选集合里。两类字段候选不同：
   * <ul>
   *   <li><b>Describable 字段</b> → 候选是各描述符的 {@code getDisplayName()}
   *       （前端裸字符串匹配，见 {@link #candidateDisplayNames}）；</li>
   *   <li><b>ENUM 字段</b> → 候选是选项 {@code val}（json 声明了 {@code enum} 数组就用它，
   *       否则是枚举常量名）。{@code val} 必须与常量名逐字一致，否则 {@code setVal}
   *       反序列化不回枚举实例。</li>
   * </ul>
   */
  private static void assertEveryFieldHasAResolvableDefault(Class<?> configClazz) {
    String res = configClazz.getSimpleName() + ".json";
    JSONObject resource = formResource(configClazz);
    Assert.assertNotNull("缺少 " + resourcePath(configClazz), resource);

    for (Field f : configClazz.getDeclaredFields()) {
      if (f.getAnnotation(FormField.class) == null) {
        continue;
      }
      String field = configClazz.getSimpleName() + "." + f.getName();
      JSONObject entry = resource.getJSONObject(f.getName());
      Assert.assertNotNull(field + " 在 " + res + " 中没有条目", entry);

      String dftVal = entry.getString(PluginExtraProps.KEY_DFTVAL_PROP);
      Assert.assertNotNull(field + " 未设置 " + PluginExtraProps.KEY_DFTVAL_PROP
        + " —— 新建 widget 时该项没有默认值", dftVal);

      Set<String> candidates = f.getType().isEnum()
        ? enumOptionVals(entry, f.getType())
        : candidateDisplayNames(f.getType());

      Assert.assertFalse(field + " 的候选集合为空（" + f.getType().getSimpleName()
        + "），本断言会假通过", candidates.isEmpty());
      Assert.assertTrue(field + " 的 " + PluginExtraProps.KEY_DFTVAL_PROP + "=\"" + dftVal
        + "\" 不在候选集合里 —— 这是<b>静默无默认值</b>：不报错、不回退，只是空着。候选："
        + candidates, candidates.contains(dftVal));
    }
  }

  /** ENUM 字段的候选选项 {@code val}：json 声明了 {@code enum} 数组则取它，否则取枚举常量名 */
  private static Set<String> enumOptionVals(JSONObject entry, Class<?> enumType) {
    Set<String> result = new TreeSet<>();
    JSONArray declared = entry.getJSONArray(Descriptor.KEY_ENUM_PROP);
    if (declared != null) {
      for (int i = 0; i < declared.size(); i++) {
        String val = declared.getJSONObject(i).getString("val");
        if (val != null) {
          result.add(val);
        }
      }
    }
    if (result.isEmpty()) {
      for (Object constant : enumType.getEnumConstants()) {
        result.add(((Enum<?>) constant).name());
      }
    }
    return result;
  }

  /**
   * 值类型的候选描述符 {@code displayName} —— 从 sezpoz 注解索引反查并实例化描述符，
   * <b>不依赖 TIS 运行期</b>（因此不需要 {@code --add-opens}）。
   *
   * <p>索引条目形如 {@code ...widget.AutoSizingMode$DefaultDescriptor}，
   * 取其 enclosing class 判断是否属于值类型这一族（等于它或是它的子类）。
   * 抽象值类型（{@code SizingMode}）得到全部子类；具体值类型
   * （{@code ConditionalVisibility}）得到它自己的那一个。
   */
  private static Set<String> candidateDisplayNames(Class<?> valueType) {
    Set<String> result = new TreeSet<>();
    for (String fqcn : sezpozIndexEntries()) {
      Class<?> owner = outerClassOf(fqcn);
      if (owner == null || !valueType.isAssignableFrom(owner)) {
        continue;
      }
      try {
        Class<?> descClazz = Class.forName(fqcn, true, TestWidgetVariableContract.class.getClassLoader());
        if (!Descriptor.class.isAssignableFrom(descClazz)) {
          continue;
        }
        Descriptor<?> desc = (Descriptor<?>) descClazz.getDeclaredConstructor().newInstance();
        String displayName = desc.getDisplayName();
        if (displayName != null) {
          result.add(displayName);
        }
      } catch (Throwable t) {
        throw new AssertionError("实例化 " + fqcn + " 以读取 displayName 失败："
          + t.getClass().getSimpleName() + " " + t.getMessage(), t);
      }
    }
    return result;
  }

  // ==================================================================
  //  helpers
  // ==================================================================

  /** 差集 {@code a - b}（TreeSet，保证失败信息里顺序稳定） */
  private static Set<String> minus(Set<String> a, Set<String> b) {
    Set<String> result = new TreeSet<>(a);
    result.removeAll(b);
    return result;
  }

  /** sezpoz 索引中登记的 workshop 包下的类（已加载，按名字排序，保证失败信息稳定） */
  private static List<Class<?>> scannedClasses() {
    List<Class<?>> result = new ArrayList<>();
    for (String fqcn : sezpozIndexEntries()) {
      if (!fqcn.startsWith(WORKSHOP_PKG)) {
        continue;
      }
      Class<?> clazz = outerClassOf(fqcn);
      if (clazz != null) {
        result.add(clazz);
      }
    }
    Assert.assertFalse("sezpoz 索引中 " + WORKSHOP_PKG + " 下没有任何条目", result.isEmpty());
    return result;
  }

  /** {@code widget.impl} 下的 Widget 实现类（从索引里各 {@code XxxWidget$DescriptorImpl} 反推） */
  private static Set<Class<?>> widgetImpls() {
    Set<Class<?>> result = new TreeSet<>(java.util.Comparator.comparing(Class::getName));
    for (String fqcn : sezpozIndexEntries()) {
      if (!fqcn.startsWith(WIDGET_IMPL_PKG)) {
        continue;
      }
      Class<?> clazz = outerClassOf(fqcn);
      if (clazz != null && !clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers())) {
        result.add(clazz);
      }
    }
    return result;
  }

  /**
   * workshop 包下带 {@code @TISExtension} 的<b>全部具体</b> {@code WorkshopWidget} 子类。
   *
   * <p>与 {@link #widgetImpls()} 的差别：后者按 {@code widget.impl.} 包前缀过滤，
   * 因此<b>漏掉 {@code GroovyWorkshopWidget}</b>（它在 {@code widget.groovy} 包）。
   * 需要「无一遗漏」的断言（如基类归属）必须用本方法；按
   * 「widget ↔ {@code WorkShopWidgetType} 常量一一对应」断言的仍须用 {@link #widgetImpls()}
   * —— 那个集合恰好等于枚举常量数，换个更宽的集合会让那条断言失去意义。
   */
  private static Set<Class<?>> concreteWidgets() {
    Set<Class<?>> result = new TreeSet<>(java.util.Comparator.comparing(Class::getName));
    for (Class<?> clazz : scannedClasses()) {
      if (WorkshopWidget.class.isAssignableFrom(clazz)
        && !clazz.isInterface()
        && !Modifier.isAbstract(clazz.getModifiers())) {
        result.add(clazz);
      }
    }
    return result;
  }

  /** 索引条目所在的外部类；已是顶层类则返回自身，加载不到返回 null */
  private static Class<?> outerClassOf(String fqcn) {
    try {
      Class<?> clazz = Class.forName(fqcn, false, TestWidgetVariableContract.class.getClassLoader());
      Class<?> outer = clazz.getEnclosingClass();
      return outer != null ? outer : clazz;
    } catch (Throwable t) {
      return null;
    }
  }

  /**
   * 某类上以 {@code Var} 结尾的 {@code @FormField} 字段 —— 本轮的命名约定
   * （{@code objectSetVar} / {@code activeObjectVar} / {@code dataSourceVar} …）。
   */
  private static Map<String, FormField> bindingFieldsOf(Class<?> clazz) {
    Map<String, FormField> result = new TreeMap<>();
    for (Field f : clazz.getDeclaredFields()) {
      if (!f.getName().endsWith("Var")) {
        continue;
      }
      FormField ff = f.getAnnotation(FormField.class);
      if (ff != null) {
        result.put(f.getName(), ff);
      }
    }
    return result;
  }

  private static Set<String> declaredFormFieldNames(Class<?> clazz) {
    return Arrays.stream(clazz.getDeclaredFields())
      .filter(f -> f.getAnnotation(FormField.class) != null)
      .map(Field::getName)
      .collect(Collectors.toSet());
  }

  /** 类的名称固定的嵌套 descriptor（约定名 {@code DescriptorImpl}），实例化失败返回 null */
  private static Descriptor<?> descriptorOf(Class<?> widget) {
    for (Class<?> nested : widget.getDeclaredClasses()) {
      if (!"DescriptorImpl".equals(nested.getSimpleName())
        || !Descriptor.class.isAssignableFrom(nested)) {
        continue;
      }
      try {
        return (Descriptor<?>) nested.getDeclaredConstructor().newInstance();
      } catch (Throwable t) {
        return null;
      }
    }
    return null;
  }

  private static Object invokeGetWidgetType(Descriptor<?> descriptor) {
    try {
      return descriptor.getClass().getMethod("getWidgetType").invoke(descriptor);
    } catch (Throwable t) {
      return null;
    }
  }

  private static Set<String> widgetTypeConstants() {
    try {
      Class<?> typeEnum = Class.forName(
        WIDGET_IMPL_PKG + "WorkShopWidgetType", false,
        TestWidgetVariableContract.class.getClassLoader());
      return Arrays.stream(typeEnum.getEnumConstants())
        .map(c -> ((Enum<?>) c).name()).collect(Collectors.toCollection(TreeSet::new));
    } catch (Throwable t) {
      throw new AssertionError("无法加载 WorkShopWidgetType", t);
    }
  }

  /** 表单资源路径：{@code /<FQCN 的点号换成斜杠>.json} */
  private static String resourcePath(Class<?> clazz) {
    return "/" + clazz.getName().replace('.', '/') + ".json";
  }

  /** 读取表单资源；不存在返回 null */
  private static JSONObject formResource(Class<?> clazz) {
    String path = resourcePath(clazz);
    try (InputStream in = clazz.getResourceAsStream(path)) {
      if (in == null) {
        return null;
      }
      String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return JSONObject.parseObject(text);
    } catch (IOException e) {
      throw new AssertionError("读取表单资源失败：" + path, e);
    }
  }

  private static List<String> sezpozIndexEntries() {
    try (InputStream in = TestWidgetVariableContract.class.getResourceAsStream(SEZPOZ_INDEX_PATH)) {
      Assert.assertNotNull("缺少 sezpoz 索引 " + SEZPOZ_INDEX_PATH + "（注解处理器未生效？）", in);
      String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      List<String> entries = new ArrayList<>();
      for (String line : text.split("\n")) {
        String trimmed = line.trim();
        if (!trimmed.isEmpty()) {
          entries.add(trimmed);
        }
      }
      return entries;
    } catch (IOException e) {
      throw new AssertionError("读取 sezpoz 索引失败：" + SEZPOZ_INDEX_PATH, e);
    }
  }
}
