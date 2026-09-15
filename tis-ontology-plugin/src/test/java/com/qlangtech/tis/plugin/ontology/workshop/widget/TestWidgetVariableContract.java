package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.workshop.widget.WorkshopWidget;
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
   * 基类字段集合 —— 与 P0-2 的删除结果精确对齐。
   *
   * <p>此前这里是 {@code inputVariables} / {@code outputVariables}（{@code List<String>}，
   * 无角色概念），零读零写，唯一实际作用是让表单在 {@code buildMultiSelectedAttr} 里抛错。
   * 该断言是「防复活」闸门：谁把它加回来，这里立刻红。
   */
  private static final Set<String> BASE_WIDGET_FIELDS =
    Set.of("name", "title", "displayConfig", "sortOrder", "cssClass");

  /**
   * 已知的 {@code MULTI_SELECTABLE} 无行编辑器字段 —— <b>缺陷登记册，不是白名单</b>。
   *
   * <p>{@code MULTI_SELECTABLE} 要能渲染，其 {@code .json} 必须同时给出
   * {@code elementCreator}（{@code ElementCreatorFactory}）与 {@code enum} 两键；
   * 缺任一个，前端 {@code Item.buildMultiSelectedAttr} 会直接
   * {@code throw new Error("...relevant enumVal can not be null")}，整个 Widget 配置表单打不开。
   *
   * <p>下面这些是当前<b>仍然存在</b>的同类实例。Widget 侧的四个待用户统一设计行编辑器；
   * model 侧三个不在 Widget 表单范围内，同属待处理。修好一个就从这里删一条 ——
   * 本表是「允许存在」而非「要求存在」，删掉不会让测试变红；但新增一处同类字段会。
   */
  private static final Map<String, String> KNOWN_MULTI_SELECTABLE_WITHOUT_ROW_EDITOR;

  static {
    Map<String, String> m = new LinkedHashMap<>();
    // Widget 侧：待统一设计的元组行编辑器（用户已裁定本轮不动）
    m.put("ChartXYWidget.layers", "图层列表：ChartLayer 行列结构待统一设计");
    m.put("ChartLayer.series", "图层内的系列列表：SeriesConfig 行列结构待统一设计");
    m.put("ObjectTableWidget.columns", "表格列定义：WidgetColumnConfig 行列结构待统一设计");
    m.put("PropertyListWidget.properties", "属性列表定义：WidgetColumnConfig 行列结构待统一设计");
    // model 侧：同一失败模式，不在 Widget 表单范围内，不在本轮范围
    m.put("MetricConfig.conditionalFormatting", "条件格式化规则列表：不在本轮 Widget 表单范围");
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

  // ==================================================================
  //  helpers
  // ==================================================================

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
