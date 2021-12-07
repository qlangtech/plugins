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

package com.qlangtech.tis.plugins.incr.flink.connector.elasticsearch7;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-29 16:55
 **/
public class TestElasticSearchSinkFactory extends TestCase {
    public void testLoadDescriptorLoad() {
        List<Descriptor<TISSinkFactory>> descriptors = TISSinkFactory.sinkFactory.descriptors();
        assertEquals(1, descriptors.size());

        assertEquals(ElasticSearchSinkFactory.DISPLAY_NAME_FLINK_CDC_SINK, descriptors.get(0).getDisplayName());
    }
}
