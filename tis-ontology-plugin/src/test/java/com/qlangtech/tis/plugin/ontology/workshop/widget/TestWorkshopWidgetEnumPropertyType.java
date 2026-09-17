package com.qlangtech.tis.plugin.ontology.workshop.widget;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.ElementPluginDesc;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.ontology.workshop.widget.groovy.GroovyWorkshopWidget;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 枚举型 {@code @FormField} 属性在表单层的三条契约。
 *
 * <p>本类从 tis-plugin 的 {@code TestPropertyType} 迁移而来 —— 三个用例都以
 * Workshop Widget 的具体类作为夹具（{@link WidgetColumnConfig} 的
 * {@code format} / {@code align}、{@link GroovyWorkshopWidget} 的 {@code renderHint}），
 * 随这些类一起搬到本模块。被断言的 {@link PropertyType} 逻辑仍在 tis-plugin 内核，
 * 此处是从插件侧对其做同等的回归覆盖。
 *
 * <ol>
 *   <li>{@link #testReflectEnumConstantsAsOptions()} —— 枚举属性且 json 资源未显式声明
 *       enum 时，反射枚举常量生成选项，val 与 label 都取常量名；</li>
 *   <li>{@link #testEnumImplementsShortCommentAsOptionWithEndType()} —— 枚举实现
 *       {@code DescriptorUseableShortComment} 时，shortComment 以 help 属性下发前端；</li>
 *   <li>{@link #testEnumFieldSetVal()} —— 前端提交枚举常量名后需转成枚举实例回填属性，
 *       非法常量名必须被拒绝。</li>
 * </ol>
 *
 * <p>三个用例都依赖 {@code TIS.get().getDescriptor(...)}，纯单测环境扫不到 descriptor 时
 * <b>跳过而非失败</b>（JDK17 下通常还缺
 * {@code --add-opens java.base/java.lang=ALL-UNNAMED}），
 * 与 {@code TestChartXYStructure}、{@code TestVariableTypeDefinitions} 处理方式一致。
 */
public class TestWorkshopWidgetEnumPropertyType {

  /**
   * 属性为 java 枚举类型，且 json 资源中没有显式声明 enum 时，直接反射枚举常量生成选项
   */
  @Test
  public void testReflectEnumConstantsAsOptions() {
    Descriptor<WidgetColumnConfig> desc = descriptor(WidgetColumnConfig.class);
    Assume.assumeTrue("当前环境未初始化 TIS / 未扫到 descriptor（JDK17 下通常还缺 "
      + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", desc != null);

    Map<String, /*** fieldname */IPropertyType> props
      = PropertyType.buildPropertyTypes(ElementPluginDesc.create(desc), WidgetColumnConfig.class);

    PropertyType align = (PropertyType) props.get("align");
    Assert.assertNotNull(align);
    List<Option> opts = align.getEnumPropOptions();
    Assert.assertEquals(3, opts.size());
    // val 取枚举常量名，保证能够反序列化回枚举实例
    Assert.assertEquals("left", opts.get(0).getValue());
    Assert.assertEquals("right", opts.get(2).getValue());
    // label 也取枚举常量名
    Assert.assertEquals("left", opts.get(0).getName());
    Assert.assertEquals("right", opts.get(2).getName());

    PropertyType format = (PropertyType) props.get("format");
    Assert.assertNotNull(format);
    List<Option> formatOpts = format.getEnumPropOptions();
    Assert.assertEquals(4, formatOpts.size());
    Assert.assertEquals("text", formatOpts.get(0).getValue());
    Assert.assertEquals("text", formatOpts.get(0).getName());

    // 枚举没有实现DescriptorUseableShortComment，选项不需要附带说明信息，就是普通的Option，既不显示图标也没有help说明
    JSONObject formatOpt = format.getExtraProps().getJSONArray(Descriptor.KEY_ENUM_PROP).getJSONObject(0);
    Assert.assertNull(formatOpt.getString(Option.KEY_END_TYPE));
    Assert.assertNull(formatOpt.getString(Option.KEY_HELP));
  }

  /**
   * 枚举实现了DescriptorUseableShortComment，选项使用OptionWithEndType承载shortComment，最终以help属性传递给前端展示
   */
  @Test
  public void testEnumImplementsShortCommentAsOptionWithEndType() {
    Descriptor<GroovyWorkshopWidget> desc = descriptor(GroovyWorkshopWidget.class);
    Assume.assumeTrue("当前环境未初始化 TIS / 未扫到 descriptor（JDK17 下通常还缺 "
      + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", desc != null);

    Map<String, /*** fieldname */IPropertyType> props
      = PropertyType.buildPropertyTypes(ElementPluginDesc.create(desc), GroovyWorkshopWidget.class);

    PropertyType renderHint = (PropertyType) props.get("renderHint");
    Assert.assertNotNull(renderHint);

    // shortComment 需要传递给前端作为选项的说明信息，所以序列化到enum中的选项含help属性
    JSONObject first = renderHint.getExtraProps().getJSONArray(Descriptor.KEY_ENUM_PROP).getJSONObject(0);
    Assert.assertEquals("generic_form", first.getString(Option.KEY_VALUE));
    Assert.assertEquals("generic_form", first.getString(Option.KEY_LABEL));
    // 前端 enum-icon-select 组件中 help 会以 <em> 形式展示在选项右侧
    Assert.assertEquals("通用表单", first.getString(Option.KEY_HELP));
    // 未指定endType，前端不展示图标
    Assert.assertNull(first.getString(Option.KEY_END_TYPE));
  }

  /**
   * 前端提交的是枚举选项的val（即枚举常量名），需要转成对应的枚举实例才能赋值到实例属性上
   */
  @Test
  public void testEnumFieldSetVal() {
    Map<String, /*** fieldname */IPropertyType> props
      = PropertyType.buildPropertyTypes(Optional.empty(), WidgetColumnConfig.class);

    PropertyType align = (PropertyType) props.get("align");
    Assert.assertNotNull(align);
    Assert.assertEquals(WidgetColumnConfig.ColumnAlign.class, align.fieldClazz);

    WidgetColumnConfig columnConfig = new WidgetColumnConfig();
    align.setVal(columnConfig, "right");
    Assert.assertEquals(WidgetColumnConfig.ColumnAlign.right, columnConfig.align);

    // 枚举实例直接赋值（例如属性默认值由插件json资源中的脚本指定）
    align.setVal(columnConfig, WidgetColumnConfig.ColumnAlign.left);
    Assert.assertEquals(WidgetColumnConfig.ColumnAlign.left, columnConfig.align);

    // 未选择选项时属性置空
    align.setVal(columnConfig, "");
    Assert.assertNull(columnConfig.align);

    try {
      align.setVal(columnConfig, "CC");
      Assert.fail("illegal enum name CC shall be rejected");
    } catch (RuntimeException e) {
      // expected: No enum constant WidgetColumnConfig.ColumnAlign.CC
    }
  }

  /**
   * 取 descriptor；环境未初始化 TIS 或扫不到时返回 null，由调用方 Assume 跳过。
   * <p>
   * {@code TIS.getDescriptor(...)} 本身返回裸 {@code Descriptor}，这里不额外泛型化 ——
   * {@link GroovyWorkshopWidget} 实现的是 {@code Describable<IWorkshopWidget>} 而非
   * {@code Describable<GroovyWorkshopWidget>}，用 {@code <T extends Describable<T>>} 反而套不上。
   */
  @SuppressWarnings("rawtypes")
  private static Descriptor descriptor(Class<? extends Describable> clazz) {
    try {
      TIS tis = TIS.get();
      if (tis == null) {
        return null;
      }
      return tis.getDescriptor(clazz);
    } catch (Throwable t) {
      return null;
    }
  }
}