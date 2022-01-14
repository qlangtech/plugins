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

package com.qlangtech.plugins.incr.flink.cdc.mysql;

import junit.framework.TestCase;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-13 14:52
 **/
public class TestIt extends TestCase {
    public void test() {

        ZoneId of = ZoneId.of("Z");
        System.out.println(of);

        System.out.println(of.getId());

        Date d = new Date(1529476623000l);

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");


        LocalDateTime localDateTime = LocalDateTime.ofInstant(d.toInstant(), of);
        System.out.println(localDateTime.format(dateTimeFormatter));
        //System.out.println(ZonedTimestamp.toIsoString(d, of, TemporalAdjusters.ofDateAdjuster((ld) -> ld)));

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(f.format(d));
    }
}
