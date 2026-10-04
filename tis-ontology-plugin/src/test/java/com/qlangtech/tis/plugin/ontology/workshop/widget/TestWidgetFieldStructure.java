package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.model.event.EventConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Widget 配置表单的<b>结构</b>回归测试。
 *
 * <p>{@link TestWidgetVariableContract} 管的是「绑定字段 → 选项 → 文案」这条变量契约；
 * 本类管另外三条同样会<b>静默失败</b>的结构契约 —— 它们都不会让编译失败，
 * 只会让配置表单里少几个字段、或者让前端的分支永远走不到：
 *
 * <ol>
 *   <li>{@link #noWidgetIsAnEmptyShell()} —— Widget 至少要有自己的配置字段。
 *       曾经的失败模式：{@code ActionButtonWidget} / {@code ButtonGroupWidget} /
 *       {@code TabsWidget} 一个 {@code @FormField} 都没有，前端组件写好了却无从配置。</li>
 *   <li>{@link #everyMultiDescriblePluginFieldElementImplementsContract()} ——
 *       {@code MULTI_DESCRIBLE_PLUGIN} 的 {@code desClazz} 必须实现
 *       {@link IPluginStore.MultiDescribleElement}。抽象层的序列化逻辑对实现类有硬要求
 *       （要有恰好一个 {@code identity = true} 字段，见
 *       {@code DescriptorsJSON} 的 {@code pkField} 下发路径），漏实现是运行期才炸。</li>
 *   <li>{@link #everyEventSubtypeKebabNameMatchesFrontendUnion()} —— {@code EventConfig}
 *       每个子类的 FQCN→kebab 推导结果，必须落在前端 {@code WorkshopEvent} 联合类型的
 *       {@code type} 字面量集合内。这是「适配器可以是机械的
 *       {@code { type: resolveDefinitionKind(cfg), ...cfg }}」的前提；子类名一旦改动
 *       （比如给 {@code OpenOverlayConfig} 补上 {@code Event} 后缀 → {@code open-overlay-event}），
 *       前端 {@code switch} 会落进 {@code default} 分支，事件被静默丢弃。</li>
 * </ol>
 */
public class TestWidgetFieldStructure {

  /** 与 {@code TestWidgetVariableContract} 共用同一套 sezpoz 索引枚举方式，理由见该类的注释 */
  private static final String SEZPOZ_INDEX_PATH =
    "/META-INF/annotations/com.qlangtech.tis.extension.TISExtension.txt";

  private static final String WORKSHOP_PKG = "com.qlangtech.tis.plugin.ontology.workshop.";
  private static final String WIDGET_IMPL_PKG = WORKSHOP_PKG + "widget.impl.";

  /**
   * 前端 {@code src/workshop/models/event.model.ts} 里 {@code WorkshopEvent} 联合类型的
   * 全部 {@code type} 字面量。改动这里等于改动前端契约，必须两边一起改。
   */
  private static final Set<String> FRONTEND_EVENT_TYPES = Set.of(
    "open-overlay", "close-overlay",
    "switch-to-page", "expand-section", "collapse-section", "toggle-section", "switch-to-tab",
    "reset-value", "recompute", "set-value", "stream-llm",
    "send-to-aip-assist",
    "open-workshop-module", "open-quiver-analysis", "open-object-view",
    "open-object-explorer", "open-notepad-document", "open-vertex-exploration",
    "refresh-data-in-module",
    "enable-auto-refresh", "disable-auto-refresh",
    "toggle-light-dark-mode");

  /**
   * 有配置面板的 Widget 不能是空壳 —— 前端组件读到的每个字段都得有地方配。
   *
   * <p>判据用「整条继承链上的 {@code @FormField} 总数」而非「本类声明的」：
   * 只靠基类的 {@code name}/{@code title} 也构不成一个可用的组件。
   */
  @Test
  public void noWidgetIsAnEmptyShell() {
    Set<Class<?>> widgets = widgetImpls();
    Assert.assertFalse("未从 sezpoz 索引枚举到任何 Widget 实现 —— 索引读取或包名前缀已失效",
      widgets.isEmpty());

    List<String> empty = new ArrayList<>();
    for (Class<?> widget : widgets) {
      if (allFormFields(widget).isEmpty()) {
        empty.add(widget.getSimpleName());
      }
    }

    Assert.assertTrue("以下 Widget 没有任何配置字段（含继承），前端组件读到的配置无处可填："
      + empty, empty.isEmpty());
  }

  /**
   * {@code MULTI_DESCRIBLE_PLUGIN} 的元素类必须实现 {@link IPluginStore.MultiDescribleElement}。
   *
   * <p>该接口带来 {@code identityValue()}，与「恰好一个 {@code identity = true} 字段」一起
   * 支撑子表单行的落盘文件名与 {@code pkField} 下发；漏了它，表单能渲染出来，
   * 但保存/回填阶段才暴露问题。
   */
  @Test
  public void everyMultiDescriblePluginFieldElementImplementsContract() {
    List<String> problems = new ArrayList<>();
    int checked = 0;

    for (Class<?> clazz : scannedClasses()) {
      for (Field f : clazz.getDeclaredFields()) {
        FormField ff = f.getAnnotation(FormField.class);
        if (ff == null || ff.type() != FormFieldType.MULTI_DESCRIBLE_PLUGIN) {
          continue;
        }
        checked++;
        String key = clazz.getSimpleName() + "." + f.getName();
        Class<?> element = ff.desClazz();
        if (element == Describable.class) {
          problems.add(key + " 是 MULTI_DESCRIBLE_PLUGIN 但没有指定 desClazz");
          continue;
        }
        if (!IPluginStore.MultiDescribleElement.class.isAssignableFrom(element)) {
          problems.add(key + " 的元素类 " + element.getName()
            + " 没有实现 IPluginStore.MultiDescribleElement（identityValue 无处取）");
        }
      }
    }

    Assert.assertTrue("扫描到的 MULTI_DESCRIBLE_PLUGIN 字段应至少有一个（扫描范围失效？）", checked > 0);
    Assert.assertTrue(String.join("\n", problems), problems.isEmpty());
  }

  /**
   * {@code EventConfig} 每个子类的短 key 必须与前端 {@code WorkshopEvent} 的 {@code type} 字面量一致，
   * 且两者必须<b>双向</b>对齐 —— 后端少一个只是少个入口，前端多一个则是永远走不到的悬空分支，
   * 两者都值得知道。
   */
  @Test
  public void everyEventSubtypeKebabNameMatchesFrontendUnion() {
    Set<String> derived = new TreeSet<>();
    List<String> problems = new ArrayList<>();

    for (Class<?> clazz : scannedClasses()) {
      if (!EventConfig.class.isAssignableFrom(clazz) || clazz.equals(EventConfig.class)) {
        continue;
      }
      if (Modifier.isAbstract(clazz.getModifiers())) {
        continue;
      }
      Object instance;
      try {
        instance = clazz.getDeclaredConstructor().newInstance();
      } catch (Throwable t) {
        problems.add(clazz.getSimpleName() + " 无法无参实例化：" + t);
        continue;
      }
      String type = ((EventConfig) instance).getEventType();
      derived.add(type);
      if (!FRONTEND_EVENT_TYPES.contains(type)) {
        problems.add(clazz.getSimpleName() + " 推导出的 type 为 '" + type
          + "'，不在前端 WorkshopEvent 的字面量集合内 —— 前端 switch 会落到 default 分支"
          + "把事件丢掉。子类名不要加 Event 后缀（见 StreamLlmConfig 类注释）");
      }
    }

    Assert.assertFalse("未扫描到任何 EventConfig 子类 —— 扫描范围失效", derived.isEmpty());
    Assert.assertTrue(String.join("\n", problems), problems.isEmpty());

    Set<String> missing = new TreeSet<>(FRONTEND_EVENT_TYPES);
    missing.removeAll(derived);
    Assert.assertTrue("前端声明了但后端没有对应 EventConfig 子类的事件类型：" + missing
      + "\n后端已实现：" + derived, missing.isEmpty());
  }

  /**
   * 表单的 Describable 嵌套链<b>不能有环</b>。
   *
   * <p>失败模式：{@code MetricConfig.secondaryMetric : MetricConfig} 这种自嵌套。
   * {@code DescriptorsJSON} 遍历嵌套 Describable 时既没有环检测、也没有深度上限
   * （{@code DefaultDescriptorsJSON.JSONAttrVal.putDescriptors} 立即递归求值），
   * 于是「主指标 → 副指标 → 副指标 → …」无限下钻，<b>指标卡的表单直接
   * StackOverflowError 打不开</b>。
   *
   * <p>这个环 2026-09-19 之前一直休眠（{@code MetricConfig} 在插件模块内零引用），
   * 是被接进表单后才爆的；而 {@link Descriptor#getPropertyTypes()} 本身只建一层、
   * 不会递归，所以常规的结构断言扫不到它 —— 必须像本方法这样自己走一遍。
   *
   * <p>修法一律是<b>扁平化</b>：同类配对关系（主/副指标）把两个字段平级挂在宿主上，
   * 不要做成父子嵌套。
   */
  @Test
  public void noCycleInDescribableNesting() {
    List<String> cycles = new ArrayList<>();

    // 覆盖范围目前只取 widget.impl。原先收窄是因为 Module / Page / Section / Overlay
    // 同样是表单根，但从它们下钻会被「identity 字段数与是否实现 IdentityName 不匹配」
    // 这个**另一个**独立缺陷挡住（Descriptor.getPropertyTypes 直接抛
    // IllegalStateException），两种失败模式混在一个断言里会互相掩盖。
    // WorkshopPage 已随 id → name 迁移补齐 IdentityName，该阻塞已解除；
    // 是否把覆盖面放宽到这几个根，待 identity 迁移的断言重建任务一并决定。
    for (Class<?> widget : widgetImpls()) {
      Descriptor<?> desc = TIS.get().getDescriptor((Class) widget);
      if (desc == null) {
        cycles.add(widget.getSimpleName() + " 没有可实例化的 Descriptor");
        continue;
      }
      walkDescriptor(widget.getSimpleName(), desc, 0, new LinkedHashSet<>(), cycles);
    }

    Assert.assertTrue("以下 Widget 的表单里，Describable 嵌套链存在环，"
      + "前端拉取表单会 StackOverflowError：\n" + String.join("\n", cycles), cycles.isEmpty());
  }

  /**
   * 沿 {@code isDescribable} 字段下钻，把环描出来。
   *
   * <p>{@code ancestors} 用描述符类名而不是字段路径做判据 —— 环的判据是「描述符又回到
   * 自己」，同一条链上重复出现同名字段但描述符不同（如两个不同的 XxxConfig）不算环。
   */
  private static void walkDescriptor(String path, Descriptor<?> desc, int depth,
                                     Set<String> ancestors, List<String> cycles) {
    String self = desc.getClass().getName();
    if (!ancestors.add(self)) {
      cycles.add(path + "  <-- 环回到 " + self);
      return;
    }
    if (depth > 12) {
      cycles.add(path + "  <-- 深度超过 12 层仍未收敛");
      return;
    }

    for (Map.Entry<String, IPropertyType> e : desc.getPropertyTypes().entrySet()) {
      PropertyType pt = (PropertyType) e.getValue();
      if (!pt.isDescribable()) {
        continue;
      }
      for (Descriptor<?> inner : pt.getApplicableDescriptors()) {
        walkDescriptor(path + "." + e.getKey(), inner, depth + 1,
          new LinkedHashSet<>(ancestors), cycles);
      }
    }
  }

  // ==================================================================
  //  helpers —— 与 TestWidgetVariableContract 同构；两处都从 sezpoz 索引枚举，
  //  避免硬编码类名清单（新增一个 Widget 只要带 @TISExtension 就自动进入覆盖范围）
  // ==================================================================

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

  private static Class<?> outerClassOf(String fqcn) {
    try {
      Class<?> clazz = Class.forName(fqcn, false, TestWidgetFieldStructure.class.getClassLoader());
      Class<?> outer = clazz.getEnclosingClass();
      return outer != null ? outer : clazz;
    } catch (Throwable t) {
      return null;
    }
  }

  /** 整条继承链上的 {@code @FormField} 字段名（子类遮蔽基类同名字段时只记一次） */
  private static Map<String, Field> allFormFields(Class<?> clazz) {
    Map<String, Field> result = new LinkedHashMap<>();
    for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Field f : c.getDeclaredFields()) {
        if (f.getAnnotation(FormField.class) == null) {
          continue;
        }
        result.putIfAbsent(f.getName(), f);
      }
    }
    return result;
  }

  private static List<String> sezpozIndexEntries() {
    try (InputStream in = TestWidgetFieldStructure.class.getResourceAsStream(SEZPOZ_INDEX_PATH)) {
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
