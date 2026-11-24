package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.qlangtech.tis.plugin.ontology.OntologyType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * {@code CurrentTimeValueSource#resolveValue} 按列类型直出的防退化钉子。
 *
 * <p>签名返回 {@code Object}，编译器护不住——改回毫秒时间戳 Long 不会有编译错误，
 * 但裸数字经 {@code setObject} 绑 MySQL DATE 列会直接报
 * {@code Incorrect date value: '1795535636017'}（2026/11/25 实际踩中）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/25
 */
public class TestCurrentTimeValueSource {

    @Test
    public void timestampPropertyResolvesToSqlTimestamp() {
        Object val = new CurrentTimeValueSource().resolveValue(Map.of(), OntologyType.TIMESTAMP);
        Assert.assertTrue("TIMESTAMP 属性必须直出 java.sql.Timestamp（java.sql.Date 会丢时分秒），实际："
                + val.getClass(), val instanceof java.sql.Timestamp);
    }

    @Test
    public void datePropertyResolvesToSqlDate() {
        Object val = new CurrentTimeValueSource().resolveValue(Map.of(), OntologyType.DATE);
        Assert.assertTrue("DATE 属性必须返回 java.sql.Date（JDBC DATE 字面量兼容），实际：" + val.getClass(),
                val instanceof java.sql.Date);
    }

    @Test
    public void unknownColTypeFallsBackToSqlDate() {
        Object val = new CurrentTimeValueSource().resolveValue(Map.of(), null);
        Assert.assertTrue("类型未知（保守默认）也必须是 java.sql.Date——DATE 列是本来源的主战场，实际："
                + val.getClass(), val instanceof java.sql.Date);
    }
}
