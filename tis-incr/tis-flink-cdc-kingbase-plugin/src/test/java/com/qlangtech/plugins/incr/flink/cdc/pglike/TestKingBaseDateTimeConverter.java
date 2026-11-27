/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.plugins.incr.flink.cdc.pglike;

import com.qlangtech.plugins.incr.flink.cdc.valconvert.DateTimeConverter;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Time;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Properties;

/**
 * 离线回归：{@link KingBaseDateTimeConverter#convertTime} 此前对所有非空输入直接抛
 * UnsupportedOperationException，导致 TIME 列事件输出失败。现钉住三种实际到达形态
 * （LocalTime / java.sql.Time / pgproto 午夜起 Duration）的格式化行为，以及
 * Duration 的钟点范围守卫（负数或 >=24h 明确报错，不取模回绕到错误时刻）。
 * 对应社区交接包 ScopedTemporalCompatibilityTest 的 converter 层用例。
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2026-11-27
 **/
public class TestKingBaseDateTimeConverter {

    /** 按生产装配方式构建 converter：setDatetimeConverters 写入的 "datetime." 前缀属性回填 configure。 */
    private static KingBaseDateTimeConverter createConverter() {
        Properties defaults = new Properties();
        DateTimeConverter.setDatetimeConverters(KingBaseDateTimeConverter.class.getName(), defaults);
        Properties local = new Properties();
        defaults.forEach((k, v) -> {
            if (k.toString().startsWith(DateTimeConverter.KEY_DATETIME + ".")) {
                local.put(k.toString().substring(DateTimeConverter.KEY_DATETIME.length() + 1), v);
            }
        });
        KingBaseDateTimeConverter converter = new KingBaseDateTimeConverter();
        converter.configure(local);
        return converter;
    }

    @Test
    public void testLocalTimeFormattedClockRange() {
        KingBaseDateTimeConverter converter = createConverter();
        for (String text : new String[]{"00:00:00", "12:34:56", "23:59:59"}) {
            Assert.assertEquals(text, converter.convertTime(LocalTime.parse(text)));
        }
    }

    @Test
    public void testSqlTimeConverted() {
        KingBaseDateTimeConverter converter = createConverter();
        for (String text : new String[]{"00:00:00", "12:34:56", "23:59:59"}) {
            Assert.assertEquals(text, converter.convertTime(Time.valueOf(text)));
        }
    }

    @Test
    public void testPgprotoDurationSinceMidnight() {
        KingBaseDateTimeConverter converter = createConverter();
        for (String text : new String[]{"00:00:00", "12:34:56", "23:59:59"}) {
            Duration sinceMidnight = Duration.ofSeconds(LocalTime.parse(text).toSecondOfDay());
            Assert.assertEquals(text, converter.convertTime(sinceMidnight));
        }
    }

    @Test
    public void testSubSecondIntentionallyTruncatedToSeconds() {
        // 按现场最终要求，TIME/DATETIME 一律保留到秒
        KingBaseDateTimeConverter converter = createConverter();
        Assert.assertEquals("23:59:59"
                , converter.convertTime(LocalTime.of(23, 59, 59, 999_999_000)));
        Assert.assertEquals("23:59:59", converter.convertTime(Duration.ofNanos(86_399_123_400_000L)));
    }

    @Test
    public void testNullPassesThrough() {
        KingBaseDateTimeConverter converter = createConverter();
        Assert.assertNull(converter.convertTime(null));
    }

    @Test
    public void testUnsupportedValueTypeStillRejected() {
        KingBaseDateTimeConverter converter = createConverter();
        try {
            converter.convertTime("12:34:56");
            Assert.fail("未知类型必须明确报错");
        } catch (UnsupportedOperationException expect) {
            // 保留对未知形态的明确异常
        }
    }

    @Test
    public void testNegativeDurationRejectedWithoutWrapping() {
        KingBaseDateTimeConverter converter = createConverter();
        try {
            converter.convertTime(Duration.ofNanos(-1));
            Assert.fail("负 Duration 不得取模回绕");
        } catch (IllegalArgumentException expect) {
            // 越界明确报错
        }
    }

    @Test
    public void testTwentyFourHoursRejected() {
        KingBaseDateTimeConverter converter = createConverter();
        try {
            converter.convertTime(Duration.ofDays(1));
            Assert.fail("24h 不是合法钟点值");
        } catch (IllegalArgumentException expect) {
            // 越界明确报错
        }
    }

    @Test
    public void testMySqlRangeSentinelRejectedWithoutWrapping() {
        // MySQL "838:59:59" 哨兵值（Kingbase 兼容模式可能出现）不得回绕成合法时刻
        KingBaseDateTimeConverter converter = createConverter();
        try {
            converter.convertTime(Duration.ofHours(838));
            Assert.fail("838h 越界必须报错而非回绕");
        } catch (IllegalArgumentException expect) {
            // 越界明确报错
        }
    }

    @Test
    public void testSiblingConvertersNullSemanticsUnchanged() {
        KingBaseDateTimeConverter converter = createConverter();
        Assert.assertNull(converter.convertDate(null));
        Assert.assertNull(converter.convertDateTime(null));
        Assert.assertNull(converter.convertTimestamp(null));
    }
}
