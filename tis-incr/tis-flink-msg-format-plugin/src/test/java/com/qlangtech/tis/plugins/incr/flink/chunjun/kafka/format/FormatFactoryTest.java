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

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format;

import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.json.SourceJsonFormatFactory;
import org.apache.flink.formats.json.JsonFormatOptionsUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-05 15:02
 **/
public class FormatFactoryTest {

    @Test
    public void isDateFormat() {
        FormatFactory formatFactory = new SourceJsonFormatFactory();
        boolean dateFormat = formatFactory.isDateFormat("2020-07-17 18:00:22");
        Assert.assertFalse(dateFormat);

        Assert.assertTrue(formatFactory.isDateFormat("2020-07-17"));
    }

    @Test
    public void isTimeStampFormat() {
        SourceJsonFormatFactory formatFactory = new SourceJsonFormatFactory();
        formatFactory.timestampFormat = JsonFormatOptionsUtil.SQL;
        Assert.assertTrue(formatFactory.isTimeStampFormat("2020-07-17 18:00:22"));

        Assert.assertFalse(formatFactory.isTimeStampFormat("2020-07-17"));
    }
}