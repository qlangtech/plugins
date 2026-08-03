package com.qlangtech.tis.plugin.datax.transformer.impl.joiner;

import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.alibaba.datax.common.element.ICol2Index;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.table.join.TableJoinFilterCondition;
import com.qlangtech.tis.plugin.table.join.TableJoinFilterConditionCreatorFactory;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchCondition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 百岁 (baisui@qlangtech.com)
 */
public class JoinCacheKeyBuilderTest {

    @Test
    public void testIntegerLongShortNormalizeToEqualKey() {
        TableJoinMatchCondition mc = createMatchCondition("order_id", "order_id", JDBCTypes.INTEGER);
        List<TableJoinMatchCondition> matchCondition = Collections.singletonList(mc);
        JoinCacheKeyBuilder builder = new JoinCacheKeyBuilder(matchCondition, Collections.emptyList());

        TargetRowsCache.JoinCacheKey keyInt = builder.buildFromRecord(new TestRecord().put("order_id", Integer.valueOf(5)), matchCondition);
        TargetRowsCache.JoinCacheKey keyLong = builder.buildFromRecord(new TestRecord().put("order_id", Long.valueOf(5)), matchCondition);
        TargetRowsCache.JoinCacheKey keyShort = builder.buildFromRecord(new TestRecord().put("order_id", Short.valueOf((short) 5)), matchCondition);

        Assert.assertEquals(keyInt, keyLong);
        Assert.assertEquals(keyInt, keyShort);
        Assert.assertEquals(keyInt.hashCode(), keyLong.hashCode());
    }

    @Test
    public void testStringNormalize() {
        TableJoinMatchCondition mc = createMatchCondition("user_id", "user_id", JDBCTypes.VARCHAR);
        List<TableJoinMatchCondition> matchCondition = Collections.singletonList(mc);
        JoinCacheKeyBuilder builder = new JoinCacheKeyBuilder(matchCondition, Collections.emptyList());

        TargetRowsCache.JoinCacheKey key1 = builder.buildFromRecord(new TestRecord().put("user_id", "abc"), matchCondition);
        TargetRowsCache.JoinCacheKey key2 = builder.buildFromRecord(new TestRecord().put("user_id", new StringBuilder("abc")), matchCondition);

        Assert.assertEquals(key1, key2);
    }

    @Test
    public void testDimFilterParamsAppended() {
        TableJoinMatchCondition mc = createMatchCondition("order_id", "order_id", JDBCTypes.BIGINT);
        List<TableJoinMatchCondition> matchCondition = Collections.singletonList(mc);

        TableJoinFilterCondition dimFilter = new TableJoinFilterCondition();
        dimFilter.setTableType(TableJoinFilterConditionCreatorFactory.TableType.Dimension);
        dimFilter.setColumnName("is_valid");
        dimFilter.setOperator(TableJoinFilterConditionCreatorFactory.Operator.EQUAL);
        dimFilter.setValueType(TableJoinFilterConditionCreatorFactory.ValueType.NUMBER);
        dimFilter.setValue("1");

        JoinCacheKeyBuilder builder = new JoinCacheKeyBuilder(matchCondition, Collections.singletonList(dimFilter));
        TargetRowsCache.JoinCacheKey key = builder.buildFromRecord(new TestRecord().put("order_id", 100L), matchCondition);

        Assert.assertEquals(1, key.getPrimaryValsLength());
        Assert.assertFalse(key.hasNullPrimaryVal());
    }

    @Test
    public void testHasNullPrimaryVal() {
        TableJoinMatchCondition mc = createMatchCondition("order_id", "order_id", JDBCTypes.INTEGER);
        List<TableJoinMatchCondition> matchCondition = Collections.singletonList(mc);
        JoinCacheKeyBuilder builder = new JoinCacheKeyBuilder(matchCondition, Collections.emptyList());

        TargetRowsCache.JoinCacheKey key = builder.buildFromRecord(new TestRecord().put("order_id", null), matchCondition);
        Assert.assertTrue(key.hasNullPrimaryVal());
    }

    private static TableJoinMatchCondition createMatchCondition(String primaryCol, String dimCol, JDBCTypes type) {
        TableJoinMatchCondition mc = new TableJoinMatchCondition();
        mc.setPrimaryTableMatchColName(primaryCol);
        mc.setDimensionMatchColName(dimCol);
        mc.setDimensionMatchColType(new DataType(type));
        return mc;
    }

    private static class TestRecord implements ColumnAwareRecord<Object> {
        private final Map<String, Object> cols = new HashMap<>();

        public TestRecord put(String key, Object val) {
            cols.put(key, val);
            return this;
        }

        @Override
        public Object getColumn(String field) {
            return cols.get(field);
        }

        @Override
        public void setColumn(String field, Object colVal) {
            cols.put(field, colVal);
        }

        @Override
        public void setString(String field, String val) {
            cols.put(field, val);
        }

        @Override
        public String getString(String field) {
            return String.valueOf(cols.get(field));
        }

        @Override
        public String getString(String field, boolean origin) {
            return getString(field);
        }

        @Override
        public void setCol2Index(ICol2Index mapper) {
        }

        @Override
        public ICol2Index getCol2Index() {
            return null;
        }
    }
}
