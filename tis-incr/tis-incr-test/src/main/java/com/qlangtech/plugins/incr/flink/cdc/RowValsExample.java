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

package com.qlangtech.plugins.incr.flink.cdc;

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.trigger.util.JsonUtil;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-29 13:55
 **/
public class RowValsExample extends RowVals<RowValsExample.RowVal> {
    public RowValsExample(Map<String, RowVal> vals) {
        super(vals);
    }

    public static class RowVal implements Callable<Object> {
        final Object val;

        public static RowVal $(Object val) {
            return new RowVal(val) {
                @Override
                public String getAssertActual(Object val) {
                    return String.valueOf(val);
                }
            };
        }


        public static RowVal date(java.sql.Date date) {
            return new RowValsExample.RowVal(date) {
                @Override
                public String getExpect() {
                    return String.valueOf(date.getTime());
                }

                @Override
                public String getAssertActual(Object val) {
                    return String.valueOf(localDateTimeToDate((LocalDate) val).getTime());
                }
            };
        }

        public static RowVal timestamp(Timestamp time) {

            return new RowValsExample.RowVal(time) {
                @Override
                public String getExpect() {
                    return String.valueOf(time.getTime());
                }

                @Override
                public String getAssertActual(Object val) {
                    return String.valueOf(localDateTimeToDate((LocalDateTime) val).getTime());
                }
            };
        }

        public static RowVal json(String text) {
            return new RowVal(text) {
                @Override
                public String getExpect() {
                    return JsonUtil.toString(JSON.parseObject(text));
                }

                @Override
                public String getAssertActual(Object val) {
                    return JsonUtil.toString(JSON.parseObject(String.valueOf(val)));
                }
            };
        }

        public static RowVal stream(String text) {
            return new RowVal(new ByteArrayInputStream(text.getBytes(TisUTF8.get()))) {
                @Override
                public String getExpect() {
                    return text;
                    // return super.getExpect();
                }

                @Override
                public String getAssertActual(Object val) {
                    return new String((byte[]) val);
                }
            };

        }

        public static RowVal decimal(long unscaledVal, int scale) {
            BigDecimal val = BigDecimal.valueOf(unscaledVal, scale);
            NumberFormat format = NumberFormat.getNumberInstance();
            format.setMaximumFractionDigits(scale);
            return new RowVal(val) {
                @Override
                public String getExpect() {
                    return format.format(val);
                }

                @Override
                public String getAssertActual(Object val) {
                    return format.format(val);
                }
            };
        }

        private RowVal(Object val) {
            this.val = val;
        }

        @Override
        public Object call() throws Exception {
            return val;
        }

        public <T> T getVal() {
            return (T) val;
        }

        public String getExpect() {
            return String.valueOf(val);
        }

        public String getAssertActual(Object val) {
            //return String.valueOf(val);
            throw new UnsupportedOperationException(String.valueOf(this.getClass()));
        }
    }

    protected static ZoneId timestampZoneId = ZoneId.systemDefault();

    private static Date localDateTimeToDate(final LocalDate localDateTime) {
        if (null == localDateTime) {
            return null;
        }
        ZonedDateTime zdt = localDateTime.atStartOfDay(timestampZoneId);
        final Date date = Date.from(zdt.toInstant());
        return date;
    }

    private static Date localDateTimeToDate(final LocalDateTime localDateTime) {
        if (null == localDateTime) {
            return null;
        }
        //final ZoneId zoneId = ZoneId.systemDefault();
        final ZonedDateTime zdt = localDateTime.atZone(timestampZoneId);
        final Date date = Date.from(zdt.toInstant());
        return date;
    }
}
