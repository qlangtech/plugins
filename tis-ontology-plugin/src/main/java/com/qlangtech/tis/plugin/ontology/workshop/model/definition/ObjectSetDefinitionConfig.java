package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

/**
 * 对象集合定义配置
 * 变量值通过 Ontology 查询获取的对象集合
 */
public class ObjectSetDefinitionConfig extends VariableDefinitionConfig {

  public static final String KEY_FILTER_VAR = "filterVar";

  @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
  public String objectType;

  @FormField(ordinal = 1, type = FormFieldType.TEXTAREA)
  public String filters; // JSON array: [{ "property": "string", "operator": "string", "value": "any" }]

  /**
   * 本对象集合在查询时额外叠加哪个「对象集合过滤器」变量（{@code VariableType.OBJECT_SET_FILTER}）的谓词。
   *
   * <h3>为什么需要这个字段</h3>
   * 本字段出现之前，对象集变量是<b>自足</b>的：它只读自己的 {@link #filters}。而
   * {@code FilterListWidget.filterOutputVar}、{@code ChartLayer.selectionAsFilterVar} 这类
   * Widget 写出的过滤条件，在本字段之前<b>没有任何读取方</b>——写进变量值缓存就再没人看，
   * 整条链路是断的（参见设计文档 05-08 中把 {@code selectionAsFilter} 标为 P2 占位）。
   * 本字段把「对象集」与「过滤器」两种变量接起来：对象集变量解析时读取被引用过滤变量的值，
   * 与自身的 {@link #filters} 合并后一起发给 Ontology 查询。
   *
   * <h3>为什么是「引用变量」而不是「让 Widget 直连对象集」</h3>
   * 过滤条件由谁产生（FilterList、图层刷选）与对象集查什么，是两件独立的事。经变量中转后
   * 一个过滤变量可以同时供多个对象集/表格消费，且过滤变量的值变化会沿变量依赖图自动触发
   * 本变量重算——前端 {@code VariableService} 已按变量依赖做拓扑排序重算，无需本字段为此做什么。
   *
   * <h3>合并语义</h3>
   * 与 {@link #filters} 是<b>「与」</b>关系（两组谓词拼接成一个列表）；被引用的过滤变量若
   * 声明的 objectType 与本配置的 {@link #objectType} 不一致，其谓词会被丢弃并记日志，
   * 而不是静默施加一个查不出任何数据的条件。值形态见前端
   * {@code src/workshop/models/object-set-filter.model.ts} 的 {@code ObjectSetFilterValue}——
   * 该值只活在浏览器内存中、不落盘，故 Java 侧没有对应类。
   *
   * <h3>可选性</h3>
   * 不配置时行为与加此字段之前完全一致（只用 {@link #filters}），存量配置不受影响。
   */
  @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, advance = true)
  public String filterVar;

  /** 通过 Ontology 查询服务取对象集合，前端直接发起 */
  @Override
  public boolean isBackendComputed() {
    return false;
  }

  @TISExtension
  public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

    public DefaultDescriptor() {
      super();
      // 只列 OBJECT_SET_FILTER 类型的变量：本字段要读的是「过滤条件」，不是别的值。
      // registerSelectOptions 收的是 Callable，惰性求值——无插件上下文的 descriptor 构造
      // （如单测里直接 new DefaultDescriptor()）不会被触发而抛异常。
      this.registerSelectOptions(KEY_FILTER_VAR, WidgetOptionHelper::getObjectSetFilterVariableOptions);
    }

    @Override
    public String getDisplayName() {
      return "ObjectSet Definition";
    }

    @Override
    public String shortComment() {
      return "对象集合定义";
    }
  }
}