package com.qlangtech.tis.plugin.ontology.impl.objtype;

import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding.DataSourceQueryCriteria;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link ObjectSetRowQuery} 的「一次性取行」约定。
 *
 * <p>本类是 2026-11 「对象集取数收归 widget」迁移后<b>唯一</b>构造明细 criteria 的地方
 * （原先是 tis-console 的 {@code WorkshopOntologyService#queryObjectSet}）。
 * 下面的断言分成三组，每组都对应一个「错了也不报错、只是数据不对」的面：
 *
 * <ol>
 *   <li><b>行数夹取</b>：{@code null} / 0 / 负数走默认值而不是抛出或查出 0 行；
 *       超硬顶饱和而不是让用户因为多填一个 0 就报错。</li>
 *   <li><b>targetCols 的构成</b>：主键必须被自动补进去 —— {@code setOrderBy} 与
 *       {@code addFilter} 的白名单判据都是「列 ∈ targetCols」，漏了是 500 或静默丢过滤。</li>
 *   <li><b>分页槽位</b>：{@code BasicCriteria} 的 page / pageSize 不能是 null（拼 SQL 时直接用）。</li>
 * </ol>
 *
 * <p>纯逻辑断言，不连库、不触达 TIS 插件扫描，故无 surefire 的 {@code --add-opens} 前提。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/11
 */
public class TestObjectSetRowQuery {

    // ===================================================================
    //  1. 行数夹取
    // ===================================================================

    /** 没传 / 传了非法值一律回落默认值：行数上限是展示策略，不是请求合法性 */
    @Test
    public void testNullZeroAndNegativeFallBackToDefault() {
        Assert.assertEquals("null 应走默认值",
                ObjectSetRowQuery.DEFAULT_MAX_ROWS, ObjectSetRowQuery.clampMaxRows(null));
        Assert.assertEquals("0 应走默认值",
                ObjectSetRowQuery.DEFAULT_MAX_ROWS, ObjectSetRowQuery.clampMaxRows(0));
        Assert.assertEquals("负数应走默认值",
                ObjectSetRowQuery.DEFAULT_MAX_ROWS, ObjectSetRowQuery.clampMaxRows(-1));
        Assert.assertEquals("Integer.MIN_VALUE 应走默认值",
                ObjectSetRowQuery.DEFAULT_MAX_ROWS, ObjectSetRowQuery.clampMaxRows(Integer.MIN_VALUE));
    }

    /** 合法区间原样通过 —— 不能顺手「优化」成默认值，否则用户配的条数会被悄悄放大 */
    @Test
    public void testLegalValuePassesThrough() {
        Assert.assertEquals(1, ObjectSetRowQuery.clampMaxRows(1));
        Assert.assertEquals(50, ObjectSetRowQuery.clampMaxRows(50));
        Assert.assertEquals(ObjectSetRowQuery.MAX_ROWS_LIMIT,
                ObjectSetRowQuery.clampMaxRows(ObjectSetRowQuery.MAX_ROWS_LIMIT));
    }

    /**
     * 超硬顶饱和而不是抛，也不是原样放行 ——
     * 原样放行等于把「一次性取回」变成「把整表搬进浏览器」，那正是这条链路被废弃的原因。
     */
    @Test
    public void testOverLimitSaturates() {
        Assert.assertEquals(ObjectSetRowQuery.MAX_ROWS_LIMIT,
                ObjectSetRowQuery.clampMaxRows(ObjectSetRowQuery.MAX_ROWS_LIMIT + 1));
        Assert.assertEquals(ObjectSetRowQuery.MAX_ROWS_LIMIT,
                ObjectSetRowQuery.clampMaxRows(Integer.MAX_VALUE));
    }

    // ===================================================================
    //  2. criteria 的构成
    // ===================================================================

    @Test
    public void testCreateCriteriaAddsTargetColsAndIdentity() {
        DataSourceQueryCriteria criteria = ObjectSetRowQuery.createCriteria(
                Arrays.asList("name", "amount"), "id", 50);

        Assert.assertEquals("两列 + 自动补进来的主键",
                java.util.Set.of("name", "amount", "id"), criteria.getTargetCols());
        Assert.assertEquals(Integer.valueOf(1), criteria.getPage());
        Assert.assertEquals("pageSize 走夹取", Integer.valueOf(50), criteria.getPageSize());
        Assert.assertNull("不排序：SQL 里不该出现 ORDER BY", criteria.getOrderByCol());
        Assert.assertTrue("不施加任何过滤（静态条件由 ObjectSetVariable 内部施加）",
                criteria.getFilters().isEmpty());
    }

    /** 主键已经是展示列时重复添加无副作用（targetCols 是 Set） */
    @Test
    public void testIdentityAlreadyInTargetColsIsHarmless() {
        DataSourceQueryCriteria criteria = ObjectSetRowQuery.createCriteria(
                Arrays.asList("id", "name"), "id", 10);
        Assert.assertEquals(java.util.Set.of("id", "name"), criteria.getTargetCols());
    }

    /** 关闭主键的对象类型：没有 identityProp 就不补 */
    @Test
    public void testNoIdentityProp() {
        DataSourceQueryCriteria criteria = ObjectSetRowQuery.createCriteria(Arrays.asList("name"), null, 10);
        Assert.assertEquals(java.util.Set.of("name"), criteria.getTargetCols());
        Assert.assertEquals(Integer.valueOf(10), criteria.getPageSize());
    }

    /** 空白列名跳过，否则会拼出 `select ,name from t` */
    @Test
    public void testBlankColsSkipped() {
        DataSourceQueryCriteria criteria = ObjectSetRowQuery.createCriteria(
                Arrays.asList("name", "  ", null, "amount"), "id", 10);
        Assert.assertEquals(java.util.Set.of("name", "amount", "id"), criteria.getTargetCols());
    }

    /** pageSize 在 createCriteria 里就夹取过，调用方不必自己再 clamp */
    @Test
    public void testCreateCriteriaClampsPageSize() {
        Assert.assertEquals(Integer.valueOf(ObjectSetRowQuery.DEFAULT_MAX_ROWS),
                ObjectSetRowQuery.createCriteria(Arrays.asList("name"), null, 0).getPageSize());
        Assert.assertEquals(Integer.valueOf(ObjectSetRowQuery.MAX_ROWS_LIMIT),
                ObjectSetRowQuery.createCriteria(Arrays.asList("name"), null, 999_999).getPageSize());
    }

    /** 空列表会拼出非法的 `select  from t` —— 必须早抛，不能当成「全属性」 */
    @Test
    public void testEmptyTargetColsRejected() {
        try {
            ObjectSetRowQuery.createCriteria(new ArrayList<>(), "id", 10);
            Assert.fail("空 targetCols 应被拒");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("targetCols"));
        }
        try {
            ObjectSetRowQuery.createCriteria(null, "id", 10);
            Assert.fail("null targetCols 应被拒");
        } catch (NullPointerException e) {
            Assert.assertTrue(e.getMessage().contains("targetCols"));
        }
    }

    // ===================================================================
    //  3. __rid 与全列捷径
    // ===================================================================

    /**
     * 每行都要带 {@code __rid}：交互侧（Button Group → OntologyAction、多选写回）只认这个键，
     * 前端不猜主键列名。而主键列本身<b>仍然</b>在行上（只是多一个键，不是搬家）。
     */
    @Test
    public void testDecorateIdentityCopiesPkWithoutRemovingIt() {
        RowMap row = new RowMap();
        row.put("id", 42);
        row.put("name", "甲");

        ObjectSetRowQuery.decorateIdentity(List.of(row), "id");

        Assert.assertEquals(42, row.get(ObjectTypeBinding.KEY_ROW_IDENTITY));
        Assert.assertEquals("主键列本身不能丢", 42, row.get("id"));
        Assert.assertEquals("甲", row.get("name"));
    }

    /** 关闭主键时原样返回：补一个恒 null 的键只会让前端多一个空字段 */
    @Test
    public void testDecorateIdentitySkippedWhenNoIdentity() {
        RowMap row = new RowMap();
        row.put("name", "甲");

        ObjectSetRowQuery.decorateIdentity(List.of(row), null);
        Assert.assertFalse(row.containsKey(ObjectTypeBinding.KEY_ROW_IDENTITY));

        ObjectSetRowQuery.decorateIdentity(List.of(row), "");
        Assert.assertFalse(row.containsKey(ObjectTypeBinding.KEY_ROW_IDENTITY));
    }

    @Test
    public void testAllColNamesPreservesOrder() {
        List<String> names = ObjectSetRowQuery.allColNames(List.of(
                property("id"), property("name"), property("amount")));
        Assert.assertEquals(List.of("id", "name", "amount"), names);
    }

    private static OntologyProperty property(String name) {
        OntologyProperty p = new DefaultOntologyProperty();
        p.name = name;
        return p;
    }
}
