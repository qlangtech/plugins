package com.qlangtech.tis.plugin.ontology.workshop.model.definition;

import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetFilterConfig.FilterOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * {@link ObjectSetDefinitionConfig#resolveFilterCriteria()} 的归一规则。
 *
 * <p>这条归一在<b>两条</b>路径上跑（前端 {@code object-set-filter.model.ts#toOntologyFilters}
 * 走 REST，本方法走后端 {@code get_data}），两者必须得出相同的谓词；否则会出现
 * 「同一个对象集，两条路径查出不同行数」这种最难排查的偏差。本类把后端的这一半钉住。
 *
 * <p>纯单测：不碰本体上下文（属性是否存在由 {@code ObjectSetVariable#applyConfiguredFilters}
 * 校验），故直接 new + 赋字段即可。
 */
public class TestObjectSetFilterCriteria {

  private static ObjectSetFilterConfig filter(String property, FilterOperator operator, String value) {
    ObjectSetFilterConfig cfg = new ObjectSetFilterConfig();
    cfg.label = property + "-" + (operator == null ? "null" : operator.name());
    cfg.property = property;
    cfg.operator = operator;
    cfg.value = value;
    return cfg;
  }

  private static ObjectSetDefinitionConfig configOf(ObjectSetFilterConfig... filters) {
    ObjectSetDefinitionConfig config = new ObjectSetDefinitionConfig();
    config.filters = List.of(filters);
    return config;
  }

  /**
   * {@code filters} 为 null 必须安全返回空列表。
   *
   * <p>字段初值 {@code new ArrayList<>()} 只在构造器里生效，而变量实例是从 XML 反序列化出来的
   * （XStream 不走构造器）；存量配置里没有 {@code filters} 节点时它就是 null。少了这条，
   * 所有老对象集的取数都会 NPE。
   */
  @Test
  public void testNullFilters() {
    ObjectSetDefinitionConfig config = new ObjectSetDefinitionConfig();
    config.filters = null;
    Assert.assertTrue(config.resolveFilterCriteria().isEmpty());
  }

  @Test
  public void testEmptyFilters() {
    Assert.assertTrue(new ObjectSetDefinitionConfig().resolveFilterCriteria().isEmpty());
  }

  /** 操作符经 {@code valueToken()} 落成线格式：{@code in_} → {@code in} */
  @Test
  public void testOperatorToken() {
    List<ObjectTypeBinding.FilterCriterion> criteria = configOf(
            filter("city", FilterOperator.in_, "北京,上海"),
            filter("amount", FilterOperator.greaterThanOrEqual, "10"))
            .resolveFilterCriteria();

    Assert.assertEquals(2, criteria.size());
    Assert.assertEquals(ObjectTypeBinding.DataSourceQueryCriteria.OP_IN, criteria.get(0).operator());
    Assert.assertEquals(ObjectTypeBinding.DataSourceQueryCriteria.OP_GREATER_THAN_OR_EQUAL,
            criteria.get(1).operator());
  }

  /** {@code operator} 为 null（XStream 不走字段初值）时回落到 equals */
  @Test
  public void testNullOperatorFallsBackToEquals() {
    List<ObjectTypeBinding.FilterCriterion> criteria = configOf(filter("city", null, "北京"))
            .resolveFilterCriteria();

    Assert.assertEquals(1, criteria.size());
    Assert.assertEquals(ObjectTypeBinding.DataSourceQueryCriteria.OP_EQUALS, criteria.get(0).operator());
    Assert.assertEquals("北京", criteria.get(0).value());
  }

  /** 多值算子：逗号拆分 + 去空白，空项丢弃 */
  @Test
  public void testMultiValuedSplitsAndTrims() {
    List<ObjectTypeBinding.FilterCriterion> criteria = configOf(
            filter("city", FilterOperator.in_, " 北京 , ,上海,  "))
            .resolveFilterCriteria();

    Assert.assertEquals(1, criteria.size());
    Assert.assertEquals(List.of("北京", "上海"), criteria.get(0).value());
  }

  /** 多值算子拆完为空 → 跳过该行（{@code IN ('')} 永远查不中，且 appendWhere 会抛空集合） */
  @Test
  public void testMultiValuedAllBlankSkipped() {
    Assert.assertTrue(configOf(filter("city", FilterOperator.in_, " , ")).resolveFilterCriteria().isEmpty());
    Assert.assertTrue(configOf(filter("city", FilterOperator.in_, "")).resolveFilterCriteria().isEmpty());
    Assert.assertTrue(configOf(filter("city", FilterOperator.in_, null)).resolveFilterCriteria().isEmpty());
  }

  /** 单值算子的空值同样跳过 —— 「没填」与「故意要 = ''」在输入端无从区分 */
  @Test
  public void testSingleValuedBlankValueSkipped() {
    Assert.assertTrue(configOf(filter("city", FilterOperator.equals, "   ")).resolveFilterCriteria().isEmpty());
    Assert.assertTrue(configOf(filter("city", FilterOperator.contains, null)).resolveFilterCriteria().isEmpty());
  }

  /**
   * 单值算子的值<b>不 trim</b>：{@code contains} 的 {@code %v%} 由 appendWhere 自己拼，
   * 提前 trim 会改掉用户刻意填的首尾空格。
   */
  @Test
  public void testSingleValuedValueNotTrimmed() {
    List<ObjectTypeBinding.FilterCriterion> criteria = configOf(
            filter("name", FilterOperator.contains, " abc "))
            .resolveFilterCriteria();

    Assert.assertEquals(1, criteria.size());
    Assert.assertEquals(" abc ", criteria.get(0).value());
  }

  /** 空白 property 跳过（同 WorkshopOntologyService 的口径） */
  @Test
  public void testBlankPropertySkipped() {
    Assert.assertTrue(configOf(filter("  ", FilterOperator.equals, "x")).resolveFilterCriteria().isEmpty());
    Assert.assertTrue(configOf(filter(null, FilterOperator.equals, "x")).resolveFilterCriteria().isEmpty());
  }

  /** property 前后空白被 trim：属性名是标识符，不该带空白 */
  @Test
  public void testPropertyTrimmed() {
    List<ObjectTypeBinding.FilterCriterion> criteria = configOf(
            filter(" city ", FilterOperator.equals, "北京"))
            .resolveFilterCriteria();

    Assert.assertEquals("city", criteria.get(0).property());
  }

  /** 坏行只跳过自己，不连坐其它行 */
  @Test
  public void testBadRowDoesNotBlockOthers() {
    List<ObjectTypeBinding.FilterCriterion> criteria = configOf(
            filter("", FilterOperator.equals, "x"),
            filter("city", FilterOperator.equals, "北京"),
            filter("amount", FilterOperator.greaterThan, ""),
            filter("level", FilterOperator.equals, "1"))
            .resolveFilterCriteria();

    Assert.assertEquals(2, criteria.size());
    Assert.assertEquals("city", criteria.get(0).property());
    Assert.assertEquals("level", criteria.get(1).property());
  }

  /** 列表里的 null 元素（手工构造/异常数据）直接忽略 */
  @Test
  public void testNullElementIgnored() {
    ObjectSetDefinitionConfig config = new ObjectSetDefinitionConfig();
    config.filters = java.util.Arrays.asList(null, filter("city", FilterOperator.equals, "北京"));
    Assert.assertEquals(1, config.resolveFilterCriteria().size());
  }
}
