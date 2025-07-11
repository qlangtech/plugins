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

package com.qlangtech.tis.plugins.incr.flink.cdc.mysql;

import com.qlangtech.plugins.incr.flink.cdc.RowValsExample;
import org.apache.flink.table.utils.DateTimeUtils;
import org.junit.Test;

import java.net.URL;
import java.sql.Time;
import java.time.LocalTime;
import java.util.Enumeration;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-09-27 10:53
 **/
public class TestTime {

    @Test
    public void testURLs() throws Exception {
        Enumeration<URL> resources = TestTime.class.getClassLoader().getResources(
                "org/apache/kafka/connect/errors/ConnectException.class");
        while (resources.hasMoreElements()) {
            System.out.println(resources.nextElement());
        }
    }

    @Test
    public void test() {
        RowValsExample.RowVal time = RowValsExample.RowVal.time("18:00:22");
        Time t = time.getVal();
        // LocalTime time = LocalTime.parse("18:00:22");
        //  System.out.println(  time.getNano());
        System.out.println(t.getTime());

        System.out.println(DateTimeUtils.toInternal(LocalTime.parse("18:00:22")));

        System.out.println(DateTimeUtils.toLocalTime(DateTimeUtils.toInternal(LocalTime.parse("18:00:22"))));

        System.out.println(Time.valueOf(LocalTime.ofNanoOfDay(t.getTime() * 1_000_000L)));
    }
}
