/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugins.incr.flink.cdc.mysql.startup;

import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.plugin.common.PluginDesc;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-29 12:11
 **/
public class TestTimestampStartupOptions {

    @Test
    public void testDescGenerate() {
        TimeFormat.timestampForTest = 1706503617788l;
        PluginDesc.testDescGenerate(
                TimestampStartupOptions.class, "TimestampStartupOptions_desc.json");
    }
}
